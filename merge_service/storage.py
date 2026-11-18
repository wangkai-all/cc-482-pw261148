"""Storage primitives: atomic state files, the segment catalog and query statistics.

Data-directory layout:

    catalog.json                      committed, authoritative list of live segments
    stats.json                        query statistics (drives hot/cold classification)
    segments/<partition>/<id>.jsonl   immutable committed segment files
    tmp/<task-id>.tmp                 merge scratch space (never read by queries)
    manifests/<task-id>.json          recoverable per-task merge manifests
    snapshots/<snapshot-id>.json      persisted reader snapshots
    locks/<partition>.lock            merge lock files
    plans/<plan-id>.json              generated plans and per-task outcomes
    cleanup_log.json                  GC run history

Every state file is written write-temp-then-os.replace so a crash never leaves a
torn file. Segment files are immutable once committed; new data always lands in a
NEW segment, which is what makes concurrent appends during a merge safe.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import threading
import time

_PARTITION_RE = re.compile(r"^[A-Za-z0-9_.=-]+$")


def validate_partition(name: str) -> str:
    if not isinstance(name, str) or not _PARTITION_RE.match(name) or name in (".", ".."):
        raise ValueError(f"invalid partition name: {name!r}")
    return name


def atomic_write_json(path: str, payload) -> None:
    """Write JSON atomically: temp file in the same directory, fsync, os.replace."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path) or ".", prefix=".tmp-", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


class Catalog:
    """The committed set of live segments per partition. Source of truth for readers.

    All mutations are serialized on ``lock`` and persisted atomically. Readers never
    take this lock for the duration of a query; they pin a *snapshot* of the view
    instead (see snapshots.py).
    """

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.path = os.path.join(data_dir, "catalog.json")
        self.lock = threading.RLock()
        self._state = {"seq": 0, "partitions": {}}
        if os.path.exists(self.path):
            with open(self.path, encoding="utf-8") as fh:
                self._state = json.load(fh)

    def _save(self) -> None:
        atomic_write_json(self.path, self._state)

    # -- ids ---------------------------------------------------------------
    def allocate_segment_id(self, prefix: str = "seg") -> str:
        with self.lock:
            self._state["seq"] += 1
            return f"{prefix}-{self._state['seq']:06d}-{int(time.time() * 1000) % 1_000_000:06d}"

    # -- mutations -----------------------------------------------------------
    def add_segment(self, partition: str, meta: dict) -> None:
        validate_partition(partition)
        with self.lock:
            parts = self._state["partitions"].setdefault(partition, {"segments": {}})
            parts["segments"][meta["id"]] = meta
            self._save()

    def remove_segments(self, partition: str, seg_ids) -> list:
        with self.lock:
            parts = self._state["partitions"].get(partition, {"segments": {}})
            removed = []
            for seg_id in seg_ids:
                meta = parts["segments"].pop(seg_id, None)
                if meta is not None:
                    removed.append(meta)
            self._save()
            return removed

    # -- reads ----------------------------------------------------------------
    def partitions(self) -> list:
        with self.lock:
            return sorted(self._state["partitions"].keys())

    def segment(self, partition: str, seg_id: str):
        with self.lock:
            return self._state["partitions"].get(partition, {}).get("segments", {}).get(seg_id)

    def segments(self, partition: str) -> list:
        with self.lock:
            segs = self._state["partitions"].get(partition, {}).get("segments", {})
            return sorted(segs.values(), key=lambda m: (m["created_at"], m["id"]))

    def has_segments(self, partition: str, seg_ids) -> bool:
        with self.lock:
            segs = self._state["partitions"].get(partition, {}).get("segments", {})
            return all(seg_id in segs for seg_id in seg_ids)

    def view(self) -> dict:
        """Consistent {partition: [segment-meta]} view used to build snapshots."""
        with self.lock:
            return {
                p: [
                    {
                        "id": m["id"],
                        "path": m["path"],
                        "rows": m["rows"],
                        "bytes": m["bytes"],
                        "created_at": m["created_at"],
                    }
                    for m in sorted(
                        parts["segments"].values(), key=lambda m: (m["created_at"], m["id"])
                    )
                ]
                for p, parts in self._state["partitions"].items()
            }

    # -- paths -----------------------------------------------------------------
    def segment_dir(self, partition: str) -> str:
        validate_partition(partition)
        return os.path.join(self.data_dir, "segments", partition)


class QueryStats:
    """Per-partition query statistics. The planner derives hot/cold from these."""

    def __init__(self, data_dir: str, max_events: int = 500):
        self.path = os.path.join(data_dir, "stats.json")
        self.max_events = max_events
        self.lock = threading.Lock()
        self._state = {"partitions": {}}
        if os.path.exists(self.path):
            with open(self.path, encoding="utf-8") as fh:
                self._state = json.load(fh)

    def record_read(self, partition: str, n_rows: int, now: float = None) -> None:
        now = time.time() if now is None else now
        with self.lock:
            entry = self._state["partitions"].setdefault(
                partition, {"total_reads": 0, "total_rows": 0, "last_read_at": None, "read_events": []}
            )
            entry["total_reads"] += 1
            entry["total_rows"] += n_rows
            entry["last_read_at"] = now
            entry["read_events"].append(now)
            del entry["read_events"][: max(0, len(entry["read_events"]) - self.max_events)]
            atomic_write_json(self.path, self._state)

    def reads_in_window(self, partition: str, window_sec: float, now: float = None) -> int:
        now = time.time() if now is None else now
        cutoff = now - window_sec
        with self.lock:
            entry = self._state["partitions"].get(partition)
            if not entry:
                return 0
            return sum(1 for ts in entry["read_events"] if ts >= cutoff)

    def snapshot(self, window_sec: float, now: float = None) -> dict:
        now = time.time() if now is None else now
        cutoff = now - window_sec
        with self.lock:
            return {
                p: {
                    "total_reads": e["total_reads"],
                    "total_rows": e["total_rows"],
                    "last_read_at": e["last_read_at"],
                    "reads_in_window": sum(1 for ts in e["read_events"] if ts >= cutoff),
                }
                for p, e in self._state["partitions"].items()
            }
