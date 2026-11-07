"""Minimal local stand-in for the two workspace endpoints `bundle validate` calls.

Lets CI run `databricks bundle validate` with no real workspace and no secret.
Usage: python scripts/stub_workspace.py 8765
"""

from __future__ import annotations

import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

ME = {"id": "1", "userName": "ci-user@example.com", "active": True, "groups": []}


class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, body: object) -> None:
        payload = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:
        path = self.path.split("?")[0]
        if path.endswith("/scim/v2/Me"):
            self._send(200, ME)
        elif path.endswith("/workspace/get-status"):
            self._send(200, {"object_type": "DIRECTORY", "path": "/Workspace/stub"})
        else:
            sys.stderr.write(f"stub: unhandled GET {self.path}\n")
            self._send(404, {"error_code": "NOT_FOUND", "message": path})

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(length)
        if self.path.split("?")[0].endswith("/workspace/mkdirs"):
            self._send(200, {})
        else:
            sys.stderr.write(f"stub: unhandled POST {self.path}\n")
            self._send(404, {"error_code": "NOT_FOUND", "message": self.path})

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        sys.stderr.write(f"stub: {format % args}\n")


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
