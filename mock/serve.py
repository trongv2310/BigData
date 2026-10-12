#!/usr/bin/env python3
"""Mock server cho frontend làm trước backend thật.

Chạy:  python mock/serve.py            (cổng 8010)
Chỉ dùng thư viện chuẩn. Dữ liệu lấy từ các file JSON cạnh file này, ánh xạ trong routes.json.
Mock KHÔNG tính toán gì: /api/predict luôn trả cùng một kết quả, /api/sql luôn trả cùng một bảng.
WebSocket chưa có trong mock; xem docs/contract/events.md mục "Phát lại sự kiện khi dev".
"""
import json, re, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

HERE = Path(__file__).resolve().parent
ROUTES = json.loads((HERE / "routes.json").read_text(encoding="utf-8"))["routes"]


def compile_route(r):
    pattern = re.sub(r"\{(\w+)\}", r"(?P<\1>[^/]+)", r["path"])
    return re.compile("^" + pattern + "$")


COMPILED = [(r, compile_route(r)) for r in ROUTES]


class Handler(BaseHTTPRequestHandler):
    def _send(self, status, body):
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()
        self.wfile.write(data)

    def _error(self, status, code, message):
        self._send(status, {"error": {"code": code, "message": message}})

    def _handle(self, method):
        path = urlparse(self.path).path
        if method == "POST":
            length = int(self.headers.get("Content-Length") or 0)
            if length:
                self.rfile.read(length)
        path_known = False
        for route, rx in COMPILED:
            m = rx.match(path)
            if not m:
                continue
            path_known = True
            if route["method"] != method:
                continue
            fname = route["file"].format(**m.groupdict())
            f = HERE / fname
            if not f.is_file():
                return self._error(404, "NOT_FOUND", f"Mock chưa có dữ liệu cho {path}")
            return self._send(200, json.loads(f.read_text(encoding="utf-8")))
        if path_known:
            return self._error(405, "METHOD_NOT_ALLOWED", f"{method} không dùng cho {path}")
        return self._error(404, "NOT_FOUND", f"Không có route {path}")

    def do_GET(self):
        self._handle("GET")

    def do_POST(self):
        self._handle("POST")

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()

    def log_message(self, fmt, *args):
        sys.stderr.write("[mock] " + fmt % args + "\n")


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8010
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"Mock server: http://127.0.0.1:{port}  (Ctrl+C để dừng)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
