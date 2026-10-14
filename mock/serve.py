#!/usr/bin/env python3
"""Mock server cho frontend làm trước backend thật.

Chạy:  python mock/serve.py            (cổng 8010)
Chỉ dùng thư viện chuẩn. Dữ liệu lấy từ các file JSON cạnh file này, ánh xạ trong routes.json.
MỌI SỐ TRONG MOCK LÀ MINH HỌA, không phải kết quả thật; mọi phản hồi có header `X-Mock: true`.

Mock KHÔNG tính toán gì: /api/predict luôn trả cùng một kết quả, /api/sql luôn trả cùng một bảng. Hai ngoại lệ có chủ ý:
  - /api/aqi/latest chọn file theo ?source=... (xem "variants" trong routes.json).
  - /api/replay/control đổi trạng thái trong bộ nhớ (pause/resume/speed) để giao diện bấm thử được; /api/replay/status trả trạng thái đó.
Query bắt đầu bằng `mock` (ví dụ /api/model/metrics?mock=untrained, /api/replay/status?mock=stopped, /api/predict?mock=heuristic) là cách
riêng của mock để xem trạng thái khó gặp (lỗi, rỗng); chúng không có trong openapi.yaml.
WebSocket chưa có trong mock; xem docs/contract/events.md mục "Phát lại sự kiện khi dev".
"""
import json, re, sys
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

HERE = Path(__file__).resolve().parent
ROUTES = json.loads((HERE / "routes.json").read_text(encoding="utf-8"))["routes"]
VN = timezone(timedelta(hours=7))
SPEEDS = (1, 10, 60)


def compile_route(r):
    pattern = re.sub(r"\{(\w+)\}", r"(?P<\1>[^/]+)", r["path"])
    return re.compile("^" + pattern + "$")


COMPILED = [(r, compile_route(r)) for r in ROUTES]
REPLAY = json.loads((HERE / "replay-status.json").read_text(encoding="utf-8"))  # trạng thái phát lại trong bộ nhớ


def error_body(code, message):
    return {"error": {"code": code, "message": message}}


def pick_variant(route, query):
    for v in route.get("variants", []):
        if all(query.get(k, [None])[0] == val for k, val in v["query"].items()):
            return v
    return None


def replay_control(body):
    """Trả (status, json). Kiểm action/speed như backend thật sẽ làm (400 nếu sai)."""
    action, speed = body.get("action"), body.get("speed", REPLAY["speed"])
    if action not in ("pause", "resume") or speed not in SPEEDS or isinstance(speed, bool):
        return 400, error_body("BAD_REQUEST", "action phải là pause hoặc resume; speed (nếu có) phải là 1, 10 hoặc 60.")
    REPLAY["state"] = "paused" if action == "pause" else "running"
    REPLAY["speed"] = speed
    REPLAY["intervalSeconds"] = REPLAY["baseIntervalSeconds"] / speed
    REPLAY["updatedAt"] = datetime.now(VN).isoformat(timespec="seconds")
    return 200, REPLAY


class Handler(BaseHTTPRequestHandler):
    def _send(self, status, body):
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("X-Mock", "true")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Expose-Headers", "X-Mock")
        self.end_headers()
        self.wfile.write(data)

    def _error(self, status, code, message):
        self._send(status, error_body(code, message))

    def _handle(self, method):
        url = urlparse(self.path)
        path, query = url.path, parse_qs(url.query)
        raw = b""
        if method == "POST":
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b""
        path_known = False
        for route, rx in COMPILED:
            m = rx.match(path)
            if not m:
                continue
            path_known = True
            if route["method"] != method:
                continue
            variant = pick_variant(route, query)
            if variant is None and route.get("handler") == "replay-status":
                return self._send(200, REPLAY)
            if variant is None and route.get("handler") == "replay-control":
                try:
                    body = json.loads(raw or b"{}")
                    if not isinstance(body, dict):
                        raise ValueError
                except ValueError:
                    return self._error(400, "BAD_REQUEST", "Thân yêu cầu phải là JSON object.")
                return self._send(*replay_control(body))
            fname = (variant["file"] if variant else route["file"]).format(**m.groupdict())
            f = HERE / fname
            if not f.is_file():
                return self._error(404, "NOT_FOUND", f"Mock chưa có dữ liệu cho {path}")
            return self._send(variant["status"] if variant else 200, json.loads(f.read_text(encoding="utf-8")))
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
    if hasattr(sys.stdout, "reconfigure"):  # Windows mặc định cp1252, không in được tiếng Việt
        sys.stdout.reconfigure(encoding="utf-8")
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8010
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"Mock server: http://127.0.0.1:{port}  (Ctrl+C để dừng)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
