#!/usr/bin/env python3
"""Run the file merge service.

    python3 run_server.py [--data-dir data] [--port 8000] [--seed]

--seed writes a few small segments into three demo partitions so the dashboard
has something to show immediately.
"""

import argparse

from merge_service import MergeConfig, MergeService
from merge_service.server import serve


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--seed", action="store_true", help="seed demo data")
    ap.add_argument("--target-size", type=int, default=4096, help="merge target size in bytes")
    args = ap.parse_args()

    config = MergeConfig(target_size_bytes=args.target_size, hot_window_sec=120.0, hot_read_threshold=5)
    service = MergeService(args.data_dir, config)

    if args.seed:
        for partition, n_segments in (("tenant-a", 7), ("tenant-b", 5), ("tenant-c", 2)):
            for i in range(n_segments):
                service.append(partition, [{"ts": i, "partition": partition, "seq": j} for j in range(20)])
        # make tenant-b look hot
        for _ in range(8):
            service.query("tenant-b")
        print("seeded demo data: tenant-a (cold, 7 segments), tenant-b (hot, 5), tenant-c (2)")

    httpd = serve(service, args.host, args.port)
    print(f"dashboard: http://{args.host}:{args.port}/  (data dir: {args.data_dir})")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
