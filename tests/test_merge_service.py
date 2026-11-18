"""Verification suite for the file merge service.

Covers the required scenarios:
  * plan generation from size / partition / hot-cold / query statistics
  * long queries keep reading the old snapshot while a merge commits
  * parallel writes during a merge are never lost
  * repeated execution is idempotent
  * failures clean temp files but keep recoverable manifests; recovery works
  * partition locks serialize merges; GC respects snapshot pins

Run: python3 -m unittest discover -s tests -v
"""

import json
import os
import shutil
import tempfile
import threading
import unittest

from merge_service import MergeConfig, MergeService


def small_config(**overrides):
    cfg = dict(
        target_size_bytes=250,       # tiny so a handful of JSON rows triggers grouping
        small_file_ratio=0.25,
        min_segments_per_task=3,
        hot_window_sec=60.0,
        hot_read_threshold=5,
        cold_read_threshold=0,
        hot_max_segments=50,
        gc_grace_sec=0.0,            # tests want immediate GC once unpinned
        lock_stale_after_sec=30.0,
    )
    cfg.update(overrides)
    return MergeConfig(**cfg)


class ServiceTestCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="merge-svc-")
        self.addCleanup(shutil.rmtree, self.dir, True)
        self.svc = MergeService(self.dir, small_config())

    # -- helpers ---------------------------------------------------------------
    def append_rows(self, partition, n_segments, rows_per_seg=3, start=0):
        for i in range(n_segments):
            self.svc.append(partition, [{"k": start + i * rows_per_seg + j} for j in range(rows_per_seg)])

    def segment_files(self, partition):
        d = os.path.join(self.dir, "segments", partition)
        return sorted(os.listdir(d)) if os.path.isdir(d) else []

    def tmp_files(self):
        d = os.path.join(self.dir, "tmp")
        return sorted(os.listdir(d)) if os.path.isdir(d) else []

    def manifests(self):
        d = os.path.join(self.dir, "manifests")
        out = {}
        for f in sorted(os.listdir(d)):
            with open(os.path.join(d, f)) as fh:
                out[f[:-5]] = json.load(fh)
        return out


class TestPlanGeneration(ServiceTestCase):
    def test_plan_uses_size_partition_hotcold_and_query_stats(self):
        # cold partition: 6 small segments, never queried
        self.append_rows("cold-p", 6)
        # hot partition: 6 small segments, heavily queried
        self.append_rows("hot-p", 6)
        for _ in range(8):
            self.svc.query("hot-p")
        # warm-ish partition: only 2 segments -> below min_segments_per_task
        self.append_rows("tiny-p", 2)

        plan = self.svc.create_plan()
        tasks = plan["tasks"]

        # size-based grouping: 6 small files of ~equal size, target 250B ->
        # grouped into tasks whose inputs never cross partitions
        self.assertTrue(tasks, "expected merge tasks")
        for task in tasks:
            self.assertEqual(len({self.svc.catalog.segment(task["partition"], i)["partition"]
                                  for i in task["inputs"]}), 1)
            self.assertGreaterEqual(len(task["inputs"]), 3)

        by_partition = {}
        for t in tasks:
            by_partition.setdefault(t["partition"], []).append(t)

        # cold partition planned, with cold bonus reflected in reason/score
        self.assertIn("cold-p", by_partition)
        self.assertTrue(all(t["temperature"] == "cold" for t in by_partition["cold-p"]))
        self.assertTrue(all("cold partition" in t["reason"] for t in by_partition["cold-p"]))
        # 6 files of ~57B each, target 250B -> groups of ~5/1 or 3+ -> at least one task
        total_inputs = sum(len(t["inputs"]) for t in by_partition["cold-p"])
        self.assertEqual(total_inputs, 6, "all small cold files should be planned")

        # hot partition deferred (query statistics drive hot/cold)
        self.assertNotIn("hot-p", by_partition)
        hot_skip = [s for s in plan["skipped"] if s["partition"] == "hot-p"]
        self.assertTrue(hot_skip and "hot partition" in hot_skip[0]["reason"])

        # too-small partition skipped
        self.assertNotIn("tiny-p", by_partition)

        # hot partition still merges when segment count is pathological
        self.append_rows("hot-p", self.svc.config.hot_max_segments + 3, start=1000)
        plan2 = self.svc.create_plan()
        self.assertIn("hot-p", {t["partition"] for t in plan2["tasks"]})
        hot_task = [t for t in plan2["tasks"] if t["partition"] == "hot-p"][0]
        self.assertEqual(hot_task["temperature"], "hot")

    def test_task_ids_are_deterministic(self):
        self.append_rows("p", 6)
        plan1 = self.svc.create_plan()
        plan2 = self.svc.create_plan()
        ids1 = sorted(t["task_id"] for t in plan1["tasks"])
        ids2 = sorted(t["task_id"] for t in plan2["tasks"])
        self.assertEqual(ids1, ids2, "same inputs must yield the same task ids (idempotency key)")


class TestLongQuerySnapshotIsolation(ServiceTestCase):
    def test_long_query_reads_old_snapshot_while_merge_commits(self):
        self.append_rows("p", 6)  # 18 rows total
        started = threading.Event()
        merge_done = threading.Event()
        result = {}

        def reader():
            def hook(n):
                if n == 1:
                    started.set()
                    self.assertTrue(merge_done.wait(10), "merge did not finish in time")
            result["rows"] = self.svc.query("p", row_hook=hook)

        t = threading.Thread(target=reader)
        t.start()
        self.assertTrue(started.wait(10), "reader did not start")
        # the reader's snapshot is pinned and visible
        self.assertEqual(len(self.svc.snapshots.list()), 1)

        plan = self.svc.create_plan()
        self.svc.run_plan(plan["id"])
        self.assertTrue(all(t_["state"] == "COMMITTED" for t_ in plan["tasks"]))
        # old files must still be on disk: the reader pins them
        self.assertGreater(len(self.segment_files("p")), 1)

        merge_done.set()
        t.join(10)
        self.assertEqual(len(result["rows"]), 18, "long query must read the full old snapshot")

        # after the reader releases, GC removes the retired files
        self.svc.gc()
        self.assertEqual(len(self.segment_files("p")), 1)
        rows = self.svc.query("p")
        self.assertEqual(len(rows), 18)
        self.assertEqual(sorted(r["k"] for r in rows), list(range(18)))


class TestParallelWritesDuringMerge(ServiceTestCase):
    def test_appends_during_merge_are_not_lost(self):
        self.append_rows("p", 6)  # 18 rows
        appended = threading.Event()

        svc = self.svc

        def on_merge_row(task, n):
            if n == 1 and not appended.is_set():
                # concurrent appends land while the merge is copying rows
                svc.append("p", [{"k": 1000}])
                svc.append("p", [{"k": 1001}, {"k": 1002}])
                appended.set()

        self.svc.merger.hooks["on_merge_row"] = on_merge_row
        plan = self.svc.create_plan()
        self.svc.run_plan(plan["id"])

        self.assertTrue(appended.is_set(), "hook did not run")
        self.assertTrue(all(t["state"] == "COMMITTED" for t in plan["tasks"]))

        rows = self.svc.query("p")
        keys = sorted(r["k"] for r in rows)
        self.assertEqual(keys, list(range(18)) + [1000, 1001, 1002],
                         "concurrent appends must survive the merge exactly once")
        # appended segments are still live in the catalog
        live_ids = {m["id"] for m in self.svc.catalog.segments("p")}
        self.assertEqual(len(live_ids), 3)  # merged output + 2 appended segments

    def test_concurrent_query_and_append_and_merge(self):
        self.append_rows("p", 6)
        errors = []

        def writer():
            try:
                for i in range(5):
                    self.svc.append("p", [{"k": 2000 + i}])
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        def reader():
            try:
                for _ in range(5):
                    self.svc.query("p")
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=writer) for _ in range(3)] + \
                  [threading.Thread(target=reader) for _ in range(3)]
        for t in threads:
            t.start()
        plan = self.svc.create_plan()
        self.svc.run_plan(plan["id"])
        for t in threads:
            t.join(10)

        self.assertFalse(errors)
        rows = self.svc.query("p")
        keys = sorted(r["k"] for r in rows)
        expected_appends = sorted([2000 + i for i in range(5)] * 3)
        self.assertEqual(keys, list(range(18)) + expected_appends)


class TestRepeatedExecution(ServiceTestCase):
    def test_repeated_execution_is_idempotent(self):
        self.append_rows("p", 6)
        before = sorted(r["k"] for r in self.svc.query("p"))

        plan = self.svc.create_plan()
        self.svc.run_plan(plan["id"])
        self.assertTrue(all(t["state"] == "COMMITTED" for t in plan["tasks"]))
        mid = sorted(r["k"] for r in self.svc.query("p"))
        self.assertEqual(before, mid)

        # re-run the SAME plan: every task must be a no-op
        self.svc.run_plan(plan["id"])
        self.assertTrue(all(t["state"] == "ALREADY_DONE" for t in plan["tasks"]),
                        f"expected ALREADY_DONE, got {[t['state'] for t in plan['tasks']]}")

        # a freshly generated plan finds nothing to do (merged file is not small,
        # and there are too few segments)
        plan2 = self.svc.create_plan()
        self.assertEqual(plan2["tasks"], [])

        # recovery with nothing to recover is a no-op
        self.assertEqual(self.svc.recover()["count"], 0)

        after = sorted(r["k"] for r in self.svc.query("p"))
        self.assertEqual(before, after, "repeated execution must not duplicate or lose rows")
        self.assertEqual(len(self.segment_files("p")), 1)


class TestFailureAndRecovery(ServiceTestCase):
    def test_failure_cleans_tmp_but_keeps_recoverable_manifest(self):
        self.append_rows("p", 6)
        calls = {"n": 0}

        def boom(task, n):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("injected disk failure")

        self.svc.merger.hooks["on_merge_row"] = boom
        plan = self.svc.create_plan()
        self.svc.run_plan(plan["id"])

        failed = [t for t in plan["tasks"] if t["state"] == "FAILED"]
        self.assertTrue(failed, "expected at least one FAILED task")
        self.assertIn("injected disk failure", failed[0]["error"])

        # temp files cleaned...
        self.assertEqual(self.tmp_files(), [], "failure must clean temp files")
        # ...but the recoverable manifest is kept, with inputs and error
        manifests = self.manifests()
        m = manifests[failed[0]["task_id"]]
        self.assertEqual(m["state"], "FAILED")
        self.assertEqual(m["inputs"], failed[0]["inputs"])
        self.assertIn("injected disk failure", m["error"])

        # catalog untouched: all original data still readable
        rows = self.svc.query("p")
        self.assertEqual(sorted(r["k"] for r in rows), list(range(18)))

        # remove the fault and re-run: the plan regenerates with the SAME task ids
        del self.svc.merger.hooks["on_merge_row"]
        plan2 = self.svc.create_plan()
        self.assertEqual(
            sorted(t["task_id"] for t in plan2["tasks"]),
            sorted(t["task_id"] for t in plan["tasks"] if t["state"] == "FAILED"),
            "previously failed tasks must regenerate with identical ids",
        )
        self.svc.run_plan(plan2["id"])
        self.assertTrue(all(t["state"] == "COMMITTED" for t in plan2["tasks"]))
        rows = self.svc.query("p")
        self.assertEqual(sorted(r["k"] for r in rows), list(range(18)))

    def test_recovery_after_crash_cleans_tmp_and_marks_manifest(self):
        self.append_rows("p", 6)
        plan = self.svc.create_plan()
        task = plan["tasks"][0]

        # simulate a crashed merge: leftover tmp file + PREPARED manifest
        tmp_rel = f"tmp/{task['task_id']}.tmp"
        tmp_abs = os.path.join(self.dir, tmp_rel)
        os.makedirs(os.path.dirname(tmp_abs), exist_ok=True)
        with open(tmp_abs, "w") as fh:
            fh.write('{"partial": true}\n')
        self.svc.merger.write_manifest({
            "task_id": task["task_id"], "plan_id": plan["id"], "partition": "p",
            "inputs": task["inputs"], "input_rows": task["input_rows"],
            "input_bytes": task["input_bytes"], "output_id": "seg-m-" + task["task_id"][5:],
            "tmp_path": tmp_rel, "output_path": f"segments/p/seg-m-{task['task_id'][5:]}.jsonl",
            "state": "MERGING", "error": None,
            "created_at": 1.0, "updated_at": 1.0,
        })

        report = self.svc.recover()
        self.assertEqual(report["count"], 1)
        self.assertEqual(report["recovered"][0]["task_id"], task["task_id"])
        self.assertTrue(report["recovered"][0]["tmp_cleaned"])
        self.assertEqual(self.tmp_files(), [], "recovery must remove leftover temp files")

        # manifest kept, marked RECOVERED with its previous state
        m = self.manifests()[task["task_id"]]
        self.assertEqual(m["state"], "RECOVERED")
        self.assertEqual(m["previous_state"], "MERGING")

        # service is fully usable afterwards
        plan2 = self.svc.create_plan()
        self.svc.run_plan(plan2["id"])
        rows = self.svc.query("p")
        self.assertEqual(sorted(r["k"] for r in rows), list(range(18)))


class TestLocksAndGc(ServiceTestCase):
    def test_partition_lock_skips_concurrent_merge(self):
        self.append_rows("p", 6)
        self.assertTrue(self.svc.locks.acquire("p", "test-holder"))
        try:
            plan = self.svc.create_plan()
            self.svc.run_plan(plan["id"])
            self.assertTrue(all(t["state"] == "SKIPPED_LOCKED" for t in plan["tasks"]))
            # nothing merged: all 6 segments still live
            self.assertEqual(len(self.svc.catalog.segments("p")), 6)
        finally:
            self.svc.locks.release("p", "test-holder")
        # after release the merge proceeds
        plan2 = self.svc.create_plan()
        self.svc.run_plan(plan2["id"])
        self.assertTrue(all(t["state"] == "COMMITTED" for t in plan2["tasks"]))

    def test_gc_respects_snapshot_pins(self):
        self.append_rows("p", 6)
        snap = self.svc.open_snapshot()
        pinned_before = self.svc.snapshots.pinned_segment_ids()
        self.assertEqual(len(pinned_before), 6)

        plan = self.svc.create_plan()
        self.svc.run_plan(plan["id"])  # commits; retired inputs are pinned by snap
        self.assertGreater(len(self.segment_files("p")), 1, "pinned files must survive GC")

        run = self.svc.gc()
        self.assertEqual(run["deleted"], [])
        self.assertEqual(len(run["pending"]), 6)
        self.assertTrue(all(e["reason"] == "pinned by snapshot" for e in run["pending"]))

        self.svc.release_snapshot(snap["id"])
        run2 = self.svc.gc()
        self.assertEqual(len(run2["deleted"]), 6)
        self.assertEqual(len(self.segment_files("p")), 1)
        # cleanup log records reclaimed bytes for the dashboard
        status = self.svc.status()
        self.assertGreater(status["cleanup"]["total_bytes_reclaimed"], 0)


class TestStatusAggregation(ServiceTestCase):
    def test_status_exposes_plans_locks_snapshots_cleanup(self):
        self.append_rows("p", 6)
        snap = self.svc.open_snapshot()
        self.svc.locks.acquire("p", "status-test")
        try:
            plan = self.svc.create_plan()
            status = self.svc.status()
            self.assertTrue(status["plans"])
            self.assertEqual(status["locks"][0]["holder"], "status-test")
            self.assertEqual(len(status["snapshots"]), 1)
            self.assertIn("cleanup", status)
            self.assertEqual(status["partitions"][0]["name"], "p")
            self.assertEqual(status["partitions"][0]["temperature"], "cold")
        finally:
            self.svc.locks.release("p", "status-test")
            self.svc.release_snapshot(snap["id"])
        self.assertEqual(self.svc.status()["locks"], [])
        self.assertEqual(self.svc.status()["snapshots"], [])


if __name__ == "__main__":
    unittest.main()
