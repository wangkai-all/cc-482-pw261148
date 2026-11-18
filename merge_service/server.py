"""HTTP server: JSON API + the operations dashboard (stdlib only).

Endpoints:
    GET  /                     dashboard
    GET  /api/status           full status snapshot (plans, locks, snapshots, cleanup)
    POST /api/append           {"partition": "p", "n": 20, "segments": 3}
    POST /api/query            {"partition": "p", "slow": false}
    POST /api/plan             generate a merge plan
    POST /api/merge            {"plan_id": optional}  run a plan in the background
    POST /api/gc               run garbage collection
    POST /api/recover          clean up crashed merges
"""

from __future__ import annotations

import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .service import MergeService

_STATIC = os.path.join(os.path.dirname(__file__), "static")


def _json_body(handler) -> dict:
    length = int(handler.headers.get("Content-Length") or 0)
    if not length:
        return {}
    try:
        return json.loads(handler.rfile.read(length).decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}


class Handler(BaseHTTPRequestHandler):
    service: MergeService = None  # injected by serve()
    server_version = "MergeService/0.1"

    # -- helpers -------------------------------------------------------------
    def _send_json(self, payload, status=200):
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path, content_type):
        with open(path, "rb") as fh:
            body = fh.read()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # keep the console quiet
        pass

    # -- routing ---------------------------------------------------------------
    def do_GET(self):
        if self.path in ("/", "/index.html"):
            self._send_file(os.path.join(_STATIC, "index.html"), "text/html; charset=utf-8")
        elif self.path == "/api/status":
            self._send_json(self.service.status())
        else:
            self._send_json({"error": "not found"}, 404)

    def do_POST(self):
        try:
            body = _json_body(self)
            if self.path == "/api/append":
                self._send_json(self._append(body))
            elif self.path == "/api/query":
                self._send_json(self._query(body))
            elif self.path == "/api/plan":
                plan = self.service.create_plan()
                self._send_json({"plan_id": plan["id"], "tasks": len(plan["tasks"])})
            elif self.path == "/api/merge":
                self._send_json(self._merge(body))
            elif self.path == "/api/gc":
                self._send_json(self.service.gc())
            elif self.path == "/api/recover":
                self._send_json(self.service.recover())
            else:
                self._send_json({"error": "not found"}, 404)
        except Exception as exc:  # surface errors to the dashboard caller
            self._send_json({"error": f"{type(exc).__name__}: {exc}"}, 500)

    # -- actions -----------------------------------------------------------------
    def _append(self, body):
        partition = (body.get("partition") or "tenant-a").strip()
        n = int(body.get("n") or 20)
        segments = max(1, int(body.get("segments") or 1))
        now = time.time()
        metas = []
        for s in range(segments):
            rows = [
                {"ts": now, "partition": partition, "seq": s * n + i, "payload": f"row-{s}-{i}"}
                for i in range(n)
            ]
            metas.append(self.service.append(partition, rows))
        return {"appended": sum(m["rows"] for m in metas), "segments": [m["id"] for m in metas]}

    def _query(self, body):
        partition = (body.get("partition") or "tenant-a").strip()
        if body.get("slow"):
            # background long query: visible in the snapshots table while it runs
            def slow_query():
                self.service.query(partition, row_hook=lambda _n: time.sleep(0.05))

            threading.Thread(target=slow_query, daemon=True).start()
            return {"started": True, "partition": partition}
        rows = self.service.query(partition)
        return {"partition": partition, "rows": len(rows)}

    def _merge(self, body):
        plan_id = body.get("plan_id")

        def run():
            self.service.run_plan(plan_id)

        threading.Thread(target=run, daemon=True).start()
        return {"started": True, "plan_id": plan_id}


def serve(service: MergeService, host: str, port: int) -> ThreadingHTTPServer:
    Handler.service = service
    httpd = ThreadingHTTPServer((host, port), Handler)
    return httpd
