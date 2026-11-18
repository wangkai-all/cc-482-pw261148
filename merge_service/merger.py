"""Merge execution, failure handling, recovery and garbage collection.

Invariants enforced here:

  * **Snapshot isolation for reads** -- the merger never deletes a file that any
    live snapshot pins. Retired inputs are removed from the catalog at commit and
    physically deleted later by ``gc()`` once unpinned.
  * **No lost appends** -- the commit only removes the task's own input ids from
    the catalog and adds the merged segment. Segments appended concurrently have
    different ids and are untouched. The merged output is written to ``tmp/`` and
    atomically renamed into place under the catalog lock.
  * **Recoverable failures** -- every state transition is persisted to
    ``manifests/<task-id>.json``. On failure the temp file is deleted but the
    manifest (inputs, output id, error) is kept, so the task can be re-run or
    cleaned up by ``recover()``.
  * **Idempotency** -- the output id is derived from the deterministic task id.
    Re-executing a committed task is a no-op (ALREADY_DONE); re-executing a task
    whose inputs are gone is a no-op (STALE_ABORTED).
"""

from __future__ import annotations

import json
import os
import time

from .storage import atomic_write_json

TASK_PREFIX = "task-"
OUTPUT_PREFIX = "seg-m-"

# terminal states that appear on the dashboard
STATE_PENDING = "PENDING"
STATE_PREPARED = "PREPARED"
STATE_MERGING = "MERGING"
STATE_COMMITTED = "COMMITTED"
STATE_FAILED = "FAILED"
STATE_RECOVERED = "RECOVERED"
STATE_ALREADY_DONE = "ALREADY_DONE"
STATE_STALE_ABORTED = "STALE_ABORTED"
STATE_SKIPPED_LOCKED = "SKIPPED_LOCKED"

# states from which a crash can leave temp files behind -> recoverable
RECOVERABLE_STATES = (STATE_PREPARED, STATE_MERGING)


class MergeError(Exception):
    pass


class Merger:
    def __init__(self, data_dir, catalog, snapshots, locks, config, hooks=None):
        self.data_dir = data_dir
        self.catalog = catalog
        self.snapshots = snapshots
        self.locks = locks
        self.config = config
        self.hooks = hooks or {}
        self.manifest_dir = os.path.join(data_dir, "manifests")
        self.tmp_dir = os.path.join(data_dir, "tmp")
        self.cleanup_log_path = os.path.join(data_dir, "cleanup_log.json")
        os.makedirs(self.manifest_dir, exist_ok=True)
        os.makedirs(self.tmp_dir, exist_ok=True)
        self._cleanup_log = {"runs": [], "total_bytes_reclaimed": 0}
        if os.path.exists(self.cleanup_log_path):
            try:
                with open(self.cleanup_log_path, encoding="utf-8") as fh:
                    self._cleanup_log = json.load(fh)
            except (OSError, json.JSONDecodeError):
                pass

    # ------------------------------------------------------------------ utils
    def _hook(self, name, *args):
        fn = self.hooks.get(name)
        if fn is not None:
            return fn(*args)
        return None

    def manifest_path(self, task_id: str) -> str:
        return os.path.join(self.manifest_dir, task_id + ".json")

    def write_manifest(self, manifest: dict) -> None:
        manifest["updated_at"] = time.time()
        atomic_write_json(self.manifest_path(manifest["task_id"]), manifest)

    def list_manifests(self) -> list:
        out = []
        for fname in sorted(os.listdir(self.manifest_dir)):
            if not fname.endswith(".json"):
                continue
            try:
                with open(os.path.join(self.manifest_dir, fname), encoding="utf-8") as fh:
                    out.append(json.load(fh))
            except (OSError, json.JSONDecodeError):
                continue
        return out

    def _delete_tmp(self, manifest: dict) -> bool:
        tmp_abs = os.path.join(self.data_dir, manifest["tmp_path"])
        try:
            os.unlink(tmp_abs)
            return True
        except OSError:
            return False

    # --------------------------------------------------------------- execution
    def execute_plan(self, plan: dict, on_task_done=None) -> dict:
        for i, task in enumerate(plan["tasks"]):
            plan["tasks"][i] = self.execute_task(plan["id"], task)
            if on_task_done is not None:
                on_task_done(plan, plan["tasks"][i])
        states = {t["state"] for t in plan["tasks"]} or {"EMPTY"}
        if states <= {STATE_COMMITTED, STATE_ALREADY_DONE}:
            plan["status"] = "COMMITTED"
        elif states == {"EMPTY"}:
            plan["status"] = "EMPTY"
        elif STATE_FAILED in states:
            plan["status"] = "FAILED"
        else:
            plan["status"] = "PARTIAL"
        return plan

    def execute_task(self, plan_id: str, task: dict) -> dict:
        task = dict(task)
        task_id = task["task_id"]
        partition = task["partition"]
        output_id = OUTPUT_PREFIX + task_id[len(TASK_PREFIX):]
        manifest = {
            "task_id": task_id,
            "plan_id": plan_id,
            "partition": partition,
            "inputs": list(task["inputs"]),
            "input_rows": task["input_rows"],
            "input_bytes": task["input_bytes"],
            "output_id": output_id,
            "tmp_path": f"tmp/{task_id}.tmp",
            "output_path": f"segments/{partition}/{output_id}.jsonl",
            "state": STATE_PREPARED,
            "error": None,
            "created_at": time.time(),
            "updated_at": time.time(),
        }
        holder = f"merge:{task_id}"
        if not self.locks.acquire(partition, holder):
            task["state"] = STATE_SKIPPED_LOCKED
            task["error"] = "partition is locked by another merge"
            manifest["state"] = STATE_SKIPPED_LOCKED
            manifest["error"] = task["error"]
            self.write_manifest(manifest)
            return task
        try:
            self.write_manifest(manifest)

            # Validate against the committed catalog before doing any work.
            state = None
            if self.catalog.segment(partition, output_id) is not None:
                state = STATE_ALREADY_DONE
            elif not self.catalog.has_segments(partition, manifest["inputs"]):
                state = STATE_STALE_ABORTED
                task["error"] = "input set no longer matches the catalog"
            if state is not None:
                task["state"] = state
                manifest["state"] = state
                manifest["error"] = task.get("error")
                self.write_manifest(manifest)
                return task

            # ---- merge phase: read the input files (the plan-time snapshot of
            # this partition) and write the merged output to tmp/.
            manifest["state"] = STATE_MERGING
            self.write_manifest(manifest)
            tmp_abs = os.path.join(self.data_dir, manifest["tmp_path"])
            os.makedirs(os.path.dirname(tmp_abs), exist_ok=True)
            rows_written = 0
            self._hook("before_merge_read", task)
            with open(tmp_abs, "w", encoding="utf-8") as out:
                for seg_id in manifest["inputs"]:
                    meta = self.catalog.segment(partition, seg_id)
                    if meta is None:
                        raise MergeError(f"input segment {seg_id} vanished during merge")
                    with open(os.path.join(self.data_dir, meta["path"]), encoding="utf-8") as fh:
                        for line in fh:
                            if not line.strip():
                                continue
                            out.write(line if line.endswith("\n") else line + "\n")
                            rows_written += 1
                            self._hook("on_merge_row", task, rows_written)
                out.flush()
                os.fsync(out.fileno())
            if rows_written != manifest["input_rows"]:
                raise MergeError(
                    f"row count mismatch: wrote {rows_written}, expected {manifest['input_rows']}"
                )

            # ---- commit: atomic rename + catalog swap. Concurrent appends live
            # in other segment ids and are not touched here.
            self._hook("before_commit", task)
            self.locks.refresh(partition, holder)
            with self.catalog.lock:
                if self.catalog.segment(partition, output_id) is not None:
                    task["state"] = STATE_ALREADY_DONE
                elif not self.catalog.has_segments(partition, manifest["inputs"]):
                    task["state"] = STATE_STALE_ABORTED
                    task["error"] = "input set changed while merging"
                else:
                    final_abs = os.path.join(self.data_dir, manifest["output_path"])
                    os.makedirs(os.path.dirname(final_abs), exist_ok=True)
                    os.replace(tmp_abs, final_abs)
                    self.catalog.remove_segments(partition, manifest["inputs"])
                    self.catalog.add_segment(
                        partition,
                        {
                            "id": output_id,
                            "partition": partition,
                            "path": manifest["output_path"],
                            "rows": rows_written,
                            "bytes": os.path.getsize(final_abs),
                            "created_at": time.time(),
                            "merged_from": list(manifest["inputs"]),
                        },
                    )
                    task["state"] = STATE_COMMITTED
                    task["output_id"] = output_id
            if task["state"] != STATE_COMMITTED:
                self._delete_tmp(manifest)
            manifest["state"] = task["state"]
            manifest["error"] = task.get("error")
            self.write_manifest(manifest)
            return task
        except Exception as exc:  # failure: clean temp file, keep recoverable manifest
            self._delete_tmp(manifest)
            task["state"] = STATE_FAILED
            task["error"] = f"{type(exc).__name__}: {exc}"
            manifest["state"] = STATE_FAILED
            manifest["error"] = task["error"]
            self.write_manifest(manifest)
            return task
        finally:
            self.locks.release(partition, holder)

    # ---------------------------------------------------------------- recovery
    def recover(self) -> dict:
        """Clean up after crashed merges.

        Manifests left in PREPARED/MERGING mean the holder died mid-task: delete
        the leftover temp file and mark the manifest RECOVERED (kept on disk as
        the recoverable receipt; the task is safe to re-run). Manifests whose
        partition is currently locked are left alone -- the merge is live.
        """
        recovered = []
        active_locks = {l["partition"] for l in self.locks.active() if not l["stale"]}
        for manifest in self.list_manifests():
            if manifest.get("state") not in RECOVERABLE_STATES:
                continue
            if manifest.get("partition") in active_locks:
                continue
            tmp_cleaned = self._delete_tmp(manifest)
            manifest["previous_state"] = manifest["state"]
            manifest["state"] = STATE_RECOVERED
            manifest["recovered_at"] = time.time()
            self.write_manifest(manifest)
            recovered.append(
                {
                    "task_id": manifest["task_id"],
                    "partition": manifest["partition"],
                    "previous_state": manifest["previous_state"],
                    "tmp_cleaned": tmp_cleaned,
                }
            )
        return {"recovered": recovered, "count": len(recovered)}

    # --------------------------------------------------------------------- gc
    def gc(self, now: float = None, grace_sec: float = None) -> dict:
        """Delete unreferenced files that no live snapshot pins.

        Candidates: segment files on disk that are not in the catalog (retired
        merge inputs, orphaned appends) and orphan tmp files. A candidate is
        deleted only when it is older than the grace period AND not pinned by
        any snapshot; otherwise it is reported as pending.
        """
        now = time.time() if now is None else now
        grace_sec = self.config.gc_grace_sec if grace_sec is None else grace_sec
        pinned = self.snapshots.pinned_segment_ids()
        deleted, pending = [], []

        live_ids = set()
        for partition in self.catalog.partitions():
            live_ids.update(m["id"] for m in self.catalog.segments(partition))
            seg_dir = self.catalog.segment_dir(partition)
            if not os.path.isdir(seg_dir):
                continue
            for fname in sorted(os.listdir(seg_dir)):
                if not fname.endswith(".jsonl"):
                    continue
                seg_id = fname[: -len(".jsonl")]
                if seg_id in live_ids:
                    continue
                path = os.path.join(seg_dir, fname)
                try:
                    st = os.stat(path)
                except OSError:
                    continue
                entry = {"partition": partition, "segment": seg_id, "bytes": st.st_size}
                if seg_id in pinned:
                    entry["reason"] = "pinned by snapshot"
                    pending.append(entry)
                elif now - st.st_mtime < grace_sec:
                    entry["reason"] = "within grace period"
                    pending.append(entry)
                else:
                    try:
                        os.unlink(path)
                        deleted.append(entry)
                    except OSError as exc:
                        entry["reason"] = f"delete failed: {exc}"
                        pending.append(entry)

        # orphan tmp files (crash leftovers not covered by a live manifest)
        busy_tmp = {
            m["tmp_path"] for m in self.list_manifests() if m.get("state") in RECOVERABLE_STATES
        }
        if os.path.isdir(self.tmp_dir):
            for fname in sorted(os.listdir(self.tmp_dir)):
                if not fname.endswith(".tmp"):
                    continue
                rel = f"tmp/{fname}"
                path = os.path.join(self.tmp_dir, fname)
                try:
                    st = os.stat(path)
                except OSError:
                    continue
                if rel in busy_tmp or now - st.st_mtime < grace_sec:
                    continue
                try:
                    os.unlink(path)
                    deleted.append({"partition": None, "segment": rel, "bytes": st.st_size})
                except OSError:
                    pass

        run = {
            "ts": now,
            "deleted": deleted,
            "deleted_bytes": sum(e["bytes"] for e in deleted),
            "pending": pending,
        }
        self._cleanup_log["runs"].append(run)
        del self._cleanup_log["runs"][:-50]
        self._cleanup_log["total_bytes_reclaimed"] = self._cleanup_log.get(
            "total_bytes_reclaimed", 0
        ) + run["deleted_bytes"]
        atomic_write_json(self.cleanup_log_path, self._cleanup_log)
        return run

    def cleanup_status(self) -> dict:
        runs = self._cleanup_log.get("runs", [])
        return {
            "total_bytes_reclaimed": self._cleanup_log.get("total_bytes_reclaimed", 0),
            "runs": runs[-10:],
            "last_run": runs[-1] if runs else None,
        }
