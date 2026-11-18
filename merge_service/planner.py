"""Merge plan generation.

A plan is derived from four signals, per the service contract:

  * **size**        -- small files (below ``small_file_ratio`` of the target) are
                       greedily grouped into tasks of roughly ``target_size_bytes``;
  * **partition**   -- tasks never span partitions; each partition is planned
                       independently;
  * **hot/cold**    -- partitions are classified by reads inside ``hot_window_sec``;
                       cold partitions get a score bonus, hot partitions are
                       deferred entirely unless their segment count is pathological
                       (``hot_max_segments``);
  * **query stats** -- the read counts behind hot/cold come from QueryStats.

Task ids are a deterministic hash of (partition, sorted input ids). Re-generating
a plan over unchanged inputs therefore yields the *same* task ids, which is what
makes repeated execution idempotent: the merger can recognise already-committed
work by id.
"""

from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field, asdict


@dataclass
class MergeConfig:
    target_size_bytes: int = 64 * 1024 * 1024
    small_file_ratio: float = 0.25
    min_segments_per_task: int = 3
    max_inputs_per_task: int = 32
    hot_window_sec: float = 300.0
    hot_read_threshold: int = 10   # >= this many reads in window => hot
    cold_read_threshold: int = 0   # <= this many reads in window => cold
    hot_max_segments: int = 50     # hot partitions merge only above this count
    w_count: float = 1.0
    w_size: float = 1.0
    cold_bonus: float = 2.0
    hot_penalty: float = 5.0
    gc_grace_sec: float = 60.0     # unreferenced files younger than this are kept
    lock_stale_after_sec: float = 300.0

    def public_dict(self) -> dict:
        return asdict(self)


def task_id_for(partition: str, input_ids) -> str:
    h = hashlib.sha1()
    h.update(partition.encode("utf-8"))
    h.update(b"\x00")
    for seg_id in sorted(input_ids):
        h.update(seg_id.encode("utf-8"))
        h.update(b"\x00")
    return "task-" + h.hexdigest()[:16]


def classify_temperature(reads_in_window: int, config: MergeConfig) -> str:
    if reads_in_window >= config.hot_read_threshold:
        return "hot"
    if reads_in_window <= config.cold_read_threshold:
        return "cold"
    return "warm"


def generate_plan(partitions_view: dict, reads_in_window, config: MergeConfig, now: float = None) -> dict:
    """Build a plan.

    ``partitions_view``: {partition: [segment-meta]} from Catalog.view().
    ``reads_in_window``: callable(partition, window_sec, now) -> int (QueryStats).
    """
    now = time.time() if now is None else now
    small_threshold = config.target_size_bytes * config.small_file_ratio
    tasks = []
    skipped = []

    for partition in sorted(partitions_view.keys()):
        segs = partitions_view[partition]
        reads = reads_in_window(partition, config.hot_window_sec, now)
        temperature = classify_temperature(reads, config)

        if len(segs) < config.min_segments_per_task:
            skipped.append(
                {"partition": partition, "temperature": temperature,
                 "reason": f"only {len(segs)} segment(s), below min_segments_per_task={config.min_segments_per_task}"}
            )
            continue
        if temperature == "hot" and len(segs) <= config.hot_max_segments:
            skipped.append(
                {"partition": partition, "temperature": temperature,
                 "reason": f"hot partition ({reads} reads in {config.hot_window_sec:.0f}s window), merge deferred"}
            )
            continue

        small = [s for s in segs if s["bytes"] < small_threshold]
        if len(small) < config.min_segments_per_task:
            skipped.append(
                {"partition": partition, "temperature": temperature,
                 "reason": f"only {len(small)} small file(s) (< {small_threshold:.0f}B), nothing to compact"}
            )
            continue

        def emit(group):
            input_ids = [s["id"] for s in group]
            input_bytes = sum(s["bytes"] for s in group)
            score = (
                config.w_count * len(group)
                + config.w_size * (input_bytes / max(1, config.target_size_bytes))
                + (config.cold_bonus if temperature == "cold" else 0.0)
                - (config.hot_penalty if temperature == "hot" else 0.0)
            )
            tasks.append(
                {
                    "task_id": task_id_for(partition, input_ids),
                    "partition": partition,
                    "inputs": input_ids,
                    "input_rows": sum(s["rows"] for s in group),
                    "input_bytes": input_bytes,
                    "temperature": temperature,
                    "reads_in_window": reads,
                    "score": round(score, 3),
                    "reason": (
                        f"{len(group)} small files "
                        f"(each < {config.small_file_ratio:.0%} of {config.target_size_bytes}B target); "
                        f"{temperature} partition ({reads} reads in {config.hot_window_sec:.0f}s)"
                    ),
                    "state": "PENDING",
                    "error": None,
                }
            )

        group, group_bytes = [], 0
        for seg in small:  # view() is ordered by created_at: merge oldest-first
            group.append(seg)
            group_bytes += seg["bytes"]
            if group_bytes >= config.target_size_bytes or len(group) >= config.max_inputs_per_task:
                if len(group) >= config.min_segments_per_task:
                    emit(group)
                group, group_bytes = [], 0
        if group and len(group) >= config.min_segments_per_task:
            emit(group)

    tasks.sort(key=lambda t: t["score"], reverse=True)
    return {
        "id": f"plan-{int(now * 1000)}-{uuid.uuid4().hex[:6]}",
        "created_at": now,
        "status": "PENDING",
        "tasks": tasks,
        "skipped": skipped,
        "config": {
            "target_size_bytes": config.target_size_bytes,
            "small_file_ratio": config.small_file_ratio,
            "min_segments_per_task": config.min_segments_per_task,
            "hot_window_sec": config.hot_window_sec,
            "hot_read_threshold": config.hot_read_threshold,
            "cold_read_threshold": config.cold_read_threshold,
        },
    }
