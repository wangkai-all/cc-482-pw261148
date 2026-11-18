"""Reader snapshots: pin the exact file set a query reads.

A query acquires a snapshot of the catalog view and reads *those* files. A merge
may commit while the query runs: the catalog swaps to the merged file, but the
old segment files must stay on disk until every snapshot referencing them is
released. Snapshots are persisted so they survive a service restart (a restarted
service must not delete files a crashed reader might still have open); operators
can expire stale ones.
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid

from .storage import atomic_write_json


class SnapshotManager:
    def __init__(self, data_dir: str):
        self.dir = os.path.join(data_dir, "snapshots")
        os.makedirs(self.dir, exist_ok=True)
        self.lock = threading.Lock()
        self._live = {}  # id -> snapshot dict
        for fname in sorted(os.listdir(self.dir)):
            if not fname.endswith(".json"):
                continue
            try:
                with open(os.path.join(self.dir, fname), encoding="utf-8") as fh:
                    snap = json.load(fh)
                self._live[snap["id"]] = snap
            except (OSError, json.JSONDecodeError, KeyError):
                continue  # corrupt snapshot files are ignored, never fatal

    def acquire(self, view: dict) -> dict:
        """Pin ``view`` ({partition: [segment-meta]}) and return the snapshot."""
        snap_id = f"snap-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"
        snap = {"id": snap_id, "created_at": time.time(), "view": view}
        with self.lock:
            self._live[snap_id] = snap
            atomic_write_json(os.path.join(self.dir, snap_id + ".json"), snap)
        return snap

    def release(self, snap_id: str) -> bool:
        with self.lock:
            if self._live.pop(snap_id, None) is None:
                return False
            try:
                os.unlink(os.path.join(self.dir, snap_id + ".json"))
            except OSError:
                pass
            return True

    def pinned_segment_ids(self) -> set:
        with self.lock:
            return {
                seg["id"]
                for snap in self._live.values()
                for segs in snap["view"].values()
                for seg in segs
            }

    def list(self, now: float = None) -> list:
        now = time.time() if now is None else now
        with self.lock:
            return [
                {
                    "id": s["id"],
                    "created_at": s["created_at"],
                    "age_sec": round(now - s["created_at"], 3),
                    "segments": sum(len(v) for v in s["view"].values()),
                    "partitions": sorted(s["view"].keys()),
                }
                for s in sorted(self._live.values(), key=lambda s: s["created_at"])
            ]

    def expire(self, max_age_sec: float, now: float = None) -> list:
        """Force-release snapshots older than ``max_age_sec`` (dead readers)."""
        now = time.time() if now is None else now
        expired = [
            s["id"] for s in self.list(now) if now - s["created_at"] > max_age_sec
        ]
        for snap_id in expired:
            self.release(snap_id)
        return expired
