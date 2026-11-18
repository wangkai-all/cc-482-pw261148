"""Per-partition merge locks.

Two layers:
  * an in-process set (fast, thread-safe) and
  * a lock file per partition (visible on the dashboard, survives restarts).

A lock file older than ``stale_after_sec`` is considered abandoned (its holder
crashed) and may be broken. Long merges call ``refresh`` to prove liveness.
"""

from __future__ import annotations

import json
import os
import threading
import time

from .storage import validate_partition


class LockManager:
    def __init__(self, data_dir: str, stale_after_sec: float = 300.0):
        self.dir = os.path.join(data_dir, "locks")
        os.makedirs(self.dir, exist_ok=True)
        self.stale_after_sec = stale_after_sec
        self.lock = threading.Lock()
        self._held = {}  # partition -> holder

    def _path(self, partition: str) -> str:
        return os.path.join(self.dir, validate_partition(partition) + ".lock")

    def acquire(self, partition: str, holder: str, now: float = None) -> bool:
        now = time.time() if now is None else now
        with self.lock:
            if partition in self._held:
                return False
            path = self._path(partition)
            if os.path.exists(path):
                try:
                    if now - os.path.getmtime(path) <= self.stale_after_sec:
                        return False
                except OSError:
                    return False
                # stale lock: break it
                try:
                    os.unlink(path)
                except OSError:
                    return False
            try:
                fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                return False
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(
                    {"partition": partition, "holder": holder, "pid": os.getpid(), "acquired_at": now},
                    fh,
                )
            self._held[partition] = holder
            return True

    def refresh(self, partition: str, holder: str) -> None:
        with self.lock:
            if self._held.get(partition) != holder:
                return
            try:
                os.utime(self._path(partition), None)
            except OSError:
                pass

    def release(self, partition: str, holder: str) -> bool:
        with self.lock:
            if self._held.get(partition) != holder:
                return False
            del self._held[partition]
            try:
                os.unlink(self._path(partition))
            except OSError:
                pass
            return True

    def active(self, now: float = None) -> list:
        now = time.time() if now is None else now
        out = []
        with self.lock:
            held = dict(self._held)
        try:
            files = sorted(os.listdir(self.dir))
        except OSError:
            return out
        for fname in files:
            if not fname.endswith(".lock"):
                continue
            path = os.path.join(self.dir, fname)
            try:
                with open(path, encoding="utf-8") as fh:
                    info = json.load(fh)
                mtime = os.path.getmtime(path)
            except (OSError, json.JSONDecodeError):
                continue
            out.append(
                {
                    "partition": info.get("partition", fname[: -len(".lock")]),
                    "holder": info.get("holder", "?"),
                    "acquired_at": info.get("acquired_at"),
                    "age_sec": round(now - mtime, 3),
                    "stale": (now - mtime) > self.stale_after_sec,
                    "held_in_process": held.get(info.get("partition")) == info.get("holder"),
                }
            )
        return out
