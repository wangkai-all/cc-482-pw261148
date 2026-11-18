# File Merge Service

Snapshot-isolated file merging (compaction) for an append-only segment store,
with a web operations dashboard. Pure Python 3 stdlib — no dependencies.

```
python3 run_server.py --data-dir data --port 8000 --seed
# dashboard: http://127.0.0.1:8000/
python3 -m unittest discover -s tests -v
```

## What it does

Data lives in immutable JSONL **segment files**, one partition per directory.
Appends create new segments, so over time partitions accumulate many small
files. The merge service compacts them:

1. **Plan** — `planner.py` scores merge tasks from four signals:
   - **size**: files below `small_file_ratio` of `target_size_bytes` are greedily
     grouped (oldest-first) into tasks of roughly the target size;
   - **partition**: tasks never span partitions;
   - **hot/cold**: partitions are classified by reads inside `hot_window_sec`
     (≥ `hot_read_threshold` → hot, ≤ `cold_read_threshold` → cold). Cold
     partitions get a score bonus; hot partitions are deferred unless their
     segment count exceeds `hot_max_segments`;
   - **query statistics**: the read counts behind hot/cold come from
     `QueryStats` (`stats.json`).
2. **Merge** — `merger.py` executes each task under a per-partition **lock**:
   read the input segments, write the merged output to `tmp/<task>.tmp`, fsync,
   then atomically `os.replace` it into place and swap the catalog under the
   catalog lock.
3. **Clean up** — retired input files are deleted by `gc()` only once no live
   snapshot pins them.

## Invariants

- **Reads use the old snapshot during a merge.** A query pins the catalog view
  in a **snapshot** (`snapshots/`) and reads those files. A committing merge
  swaps the catalog, but retired files stay on disk until every snapshot that
  references them is released. Snapshots are persisted so a restarted service
  won't delete files a crashed reader had open.
- **Concurrent appends are never lost.** Appends always create a *new* segment
  id; a merge commit only removes its own input ids from the catalog and adds
  the merged segment. The merged output itself is written to `tmp/` and
  atomically renamed — a crash before commit leaves the catalog untouched.
- **Failures clean temp files but keep a recoverable manifest.** Every task
  transition is persisted to `manifests/<task-id>.json` (inputs, output id,
  error, state). On failure the tmp file is deleted, the manifest stays.
  `recover()` removes crash-leftover tmp files and marks interrupted manifests
  `RECOVERED`; the task can then be re-run.
- **Repeated execution is idempotent.** Task ids are a deterministic hash of
  `(partition, sorted input ids)` and the output id derives from the task id.
  Re-running a committed task is a no-op (`ALREADY_DONE`); re-running a task
  whose inputs are gone is a no-op (`STALE_ABORTED`); re-generating a plan over
  unchanged inputs yields the same task ids.

Concurrency control: a service-level `pin_lock` serializes snapshot acquisition
and append commits against GC (a file is never deleted between being pinned and
being cataloged); the catalog has its own `RLock`; merges serialize per
partition via lock files (`locks/`), with stale-lock breaking after
`lock_stale_after_sec`.

## Dashboard

`GET /` shows, auto-refreshing every 2 s: stat tiles, partitions (with
temperature, query stats and a live/pending-delete bytes bar), **merge plans
and task states**, **locks**, **snapshots**, and **cleanup results** (deleted
files, reclaimed bytes, pending deletions with reasons). Buttons trigger
append / query / long-query / plan / merge / GC / recover.

### API

| Endpoint | Description |
|---|---|
| `GET /api/status` | plans, locks, snapshots, cleanup, partitions, config |
| `POST /api/append` | `{partition, n, segments}` — append generated rows |
| `POST /api/query` | `{partition, slow}` — read rows; `slow` simulates a long query |
| `POST /api/plan` | generate a merge plan |
| `POST /api/merge` | `{plan_id?}` — run a plan (background) |
| `POST /api/gc` | delete unreferenced, unpinned files |
| `POST /api/recover` | clean up crashed merges |

## Data directory layout

```
catalog.json                 committed live segments (atomic replace)
stats.json                   query statistics
segments/<partition>/*.jsonl immutable segment files
tmp/<task-id>.tmp            merge scratch space
manifests/<task-id>.json     recoverable per-task manifests
snapshots/<id>.json          persisted reader snapshots
locks/<partition>.lock       merge locks
plans/<plan-id>.json         plan history with task outcomes
cleanup_log.json             GC run history
```

## Tests

`tests/test_merge_service.py` (stdlib `unittest`, 11 tests) verifies the
required scenarios with deterministic hooks instead of sleeps:

- plan generation respects size / partition / hot-cold / query statistics;
- a **long query** blocked mid-read still reads its full old snapshot while a
  merge commits, and retired files are deleted only after release;
- **parallel writes** during a merge survive exactly once (plus a
  multi-threaded query+append+merge stress);
- **repeated execution**: same plan → `ALREADY_DONE`, fresh plan → empty, rows
  unchanged;
- failure → tmp cleaned + manifest kept → re-run succeeds; crash recovery via
  `recover()`;
- partition locks skip concurrent merges; GC respects snapshot pins.
