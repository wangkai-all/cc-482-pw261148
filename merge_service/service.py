"""MergeService: the public facade wiring catalog, snapshots, locks, planner and merger."""

from __future__ import annotations

import json
import os
import threading
import time

from .locks import LockManager
from .merger import Merger
from .planner import MergeConfig, classify_temperature, generate_plan
from .snapshots import SnapshotManager
from .storage import Catalog, QueryStats, atomic_write_json, validate_partition


class MergeService:
    def __init__(self, data_dir: str, config: MergeConfig = None, hooks: dict = None):
        self.data_dir = data_dir
        self.config = config or MergeConfig()
        os.makedirs(data_dir, exist_ok=True)
        self.catalog = Catalog(data_dir)
        self.stats = QueryStats(data_dir)
        self.snapshots = SnapshotManager(data_dir)
        self.locks = LockManager(data_dir, stale_after_sec=self.config.lock_stale_after_sec)
        self.merger = Merger(data_dir, self.catalog, self.snapshots, self.locks, self.config, hooks)
        self.plans_dir = os.path.join(data_dir, "plans")
        os.makedirs(self.plans_dir, exist_ok=True)
        # Serializes snapshot acquisition and append commits against GC: a file
        # must never be deleted between "a reader pinned it" and "the catalog
        # started pointing at it". Lock order (everywhere): _pin_lock -> catalog.lock.
        self._pin_lock = threading.Lock()
        self._plans_lock = threading.Lock()
        self._plans = {}
        for fname in sorted(os.listdir(self.plans_dir)):
            if not fname.endswith(".json"):
                continue
            try:
                with open(os.path.join(self.plans_dir, fname), encoding="utf-8") as fh:
                    plan = json.load(fh)
                self._plans[plan["id"]] = plan
            except (OSError, json.JSONDecodeError, KeyError):
                continue

    # ------------------------------------------------------------------ writes
    def append(self, partition: str, rows: list) -> dict:
        """Append rows as a NEW immutable segment. Safe to call during a merge:
        the new segment has a fresh id and is never part of an in-flight task."""
        validate_partition(partition)
        if not rows:
            raise ValueError("rows must be non-empty")
        with self._pin_lock:
            seg_id = self.catalog.allocate_segment_id()
            rel_path = os.path.join("segments", partition, seg_id + ".jsonl")
            abs_path = os.path.join(self.data_dir, rel_path)
            os.makedirs(os.path.dirname(abs_path), exist_ok=True)
            with open(abs_path, "w", encoding="utf-8") as fh:
                for row in rows:
                    fh.write(json.dumps(row, separators=(",", ":")) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            meta = {
                "id": seg_id,
                "partition": partition,
                "path": rel_path,
                "rows": len(rows),
                "bytes": os.path.getsize(abs_path),
                "created_at": time.time(),
                "merged_from": [],
            }
            self.catalog.add_segment(partition, meta)
        return meta

    # ------------------------------------------------------------------ reads
    def query(self, partition: str, row_hook=None, limit: int = None) -> list:
        """Read all rows of a partition through a pinned snapshot.

        The snapshot is held for the whole read: a merge committing mid-query
        swaps the catalog, but the files this query reads stay on disk until
        the snapshot is released here.
        """
        validate_partition(partition)
        with self._pin_lock:
            snap = self.snapshots.acquire(self.catalog.view())
        try:
            rows = []
            for entry in snap["view"].get(partition, []):
                path = os.path.join(self.data_dir, entry["path"])
                with open(path, encoding="utf-8") as fh:
                    for line in fh:
                        if not line.strip():
                            continue
                        rows.append(json.loads(line))
                        if row_hook is not None:
                            row_hook(len(rows))
                        if limit is not None and len(rows) >= limit:
                            break
            self.stats.record_read(partition, len(rows))
            return rows
        finally:
            self.snapshots.release(snap["id"])

    # --------------------------------------------------------------- snapshots
    def open_snapshot(self) -> dict:
        with self._pin_lock:
            return self.snapshots.acquire(self.catalog.view())

    def release_snapshot(self, snap_id: str) -> bool:
        return self.snapshots.release(snap_id)

    # ------------------------------------------------------------------ plans
    def _save_plan(self, plan: dict) -> None:
        with self._plans_lock:
            self._plans[plan["id"]] = plan
            atomic_write_json(os.path.join(self.plans_dir, plan["id"] + ".json"), plan)

    def create_plan(self) -> dict:
        plan = generate_plan(
            self.catalog.view(),
            self.stats.reads_in_window,
            self.config,
        )
        self._save_plan(plan)
        return plan

    def get_plan(self, plan_id: str):
        with self._plans_lock:
            return self._plans.get(plan_id)

    def run_plan(self, plan_id: str = None) -> dict:
        """Execute a plan (generating one when ``plan_id`` is None), then GC."""
        if plan_id is not None:
            plan = self.get_plan(plan_id)
            if plan is None:
                raise KeyError(f"unknown plan: {plan_id}")
        else:
            plan = self.create_plan()
        plan["status"] = "RUNNING"
        self._save_plan(plan)
        self.merger.execute_plan(plan, on_task_done=lambda p, _t: self._save_plan(p))
        plan["finished_at"] = time.time()
        self._save_plan(plan)
        self.gc()
        return plan

    # ------------------------------------------------------------- operations
    def gc(self, grace_sec: float = None) -> dict:
        with self._pin_lock:
            return self.merger.gc(grace_sec=grace_sec)

    def recover(self) -> dict:
        return self.merger.recover()

    # ----------------------------------------------------------------- status
    def status(self) -> dict:
        now = time.time()
        stats = self.stats.snapshot(self.config.hot_window_sec, now)
        cleanup = self.merger.cleanup_status()
        pinned = self.snapshots.pinned_segment_ids()

        partitions = []
        for name in self.catalog.partitions():
            segs = self.catalog.segments(name)
            seg_dir = self.catalog.segment_dir(name)
            pending_bytes = 0
            if os.path.isdir(seg_dir):
                live = {m["id"] for m in segs}
                for fname in os.listdir(seg_dir):
                    if fname.endswith(".jsonl") and fname[: -len(".jsonl")] not in live:
                        try:
                            pending_bytes += os.path.getsize(os.path.join(seg_dir, fname))
                        except OSError:
                            pass
            pstats = stats.get(
                name, {"total_reads": 0, "total_rows": 0, "last_read_at": None, "reads_in_window": 0}
            )
            partitions.append(
                {
                    "name": name,
                    "segments": len(segs),
                    "rows": sum(m["rows"] for m in segs),
                    "live_bytes": sum(m["bytes"] for m in segs),
                    "pending_delete_bytes": pending_bytes,
                    "temperature": classify_temperature(pstats["reads_in_window"], self.config),
                    **pstats,
                }
            )

        with self._plans_lock:
            plans = sorted(self._plans.values(), key=lambda p: p["created_at"], reverse=True)[:20]

        return {
            "now": now,
            "config": self.config.public_dict(),
            "partitions": partitions,
            "plans": plans,
            "locks": self.locks.active(now),
            "snapshots": self.snapshots.list(now),
            "pinned_segments": len(pinned),
            "cleanup": cleanup,
        }
