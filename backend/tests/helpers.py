"""Tiện ích test: kiểm theo schema trong docs/contract/openapi.yaml và một NameNode giả (HTTP cục bộ)."""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

import yaml
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]
SCHEMAS = yaml.safe_load((ROOT / "docs/contract/openapi.yaml").read_text(encoding="utf-8"))["components"]["schemas"]


def schema_errors(name: str, data) -> list[str]:
    v = Draft202012Validator({"$schema": "https://json-schema.org/draft/2020-12/schema",
                              "$ref": f"#/components/schemas/{name}", "components": {"schemas": SCHEMAS}})
    return [f"{'/'.join(map(str, e.path)) or '(gốc)'}: {e.message}" for e in v.iter_errors(data)]


class FakeNameNode:
    """Phục vụ /jmx?qry=... từ dict `jmx` (khóa = qry) và /webhdfs/v1/<path>?op=... từ `files`.

    files: {"/stream_input": [FileStatus...]} cho LISTSTATUS; {"/stream_input/a.json": "nội dung"} cho OPEN;
    {"/x": [LocatedBlock...]} cho GET_BLOCK_LOCATIONS khi đặt trong `blocks`.
    """

    def __init__(self, jmx=None, files=None, blocks=None, defaults=None):
        self.jmx, self.files, self.blocks, self.defaults = jmx or {}, files or {}, blocks or {}, defaults
        outer = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, code, obj):
                data = obj.encode() if isinstance(obj, str) else json.dumps(obj).encode()
                self.send_response(code)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                u = urlparse(self.path)
                q = parse_qs(u.query)
                if u.path == "/jmx":
                    bean = outer.jmx.get(q["qry"][0])
                    return self._send(200, {"beans": [bean] if bean else []})
                path = unquote(u.path[len("/webhdfs/v1"):]) or "/"
                op = q.get("op", [""])[0]
                if op == "GETSERVERDEFAULTS" and outer.defaults:
                    return self._send(200, {"FsServerDefaults": outer.defaults})
                if op == "LISTSTATUS" and isinstance(outer.files.get(path), list):
                    return self._send(200, {"FileStatuses": {"FileStatus": outer.files[path]}})
                if op == "OPEN" and isinstance(outer.files.get(path), str):
                    return self._send(200, outer.files[path])
                if op in ("GET_BLOCK_LOCATIONS", "GETFILEBLOCKLOCATIONS") and path in outer.blocks:
                    return self._send(200, {"LocatedBlocks": {"locatedBlocks": outer.blocks[path]}})
                if op == "GETFILESTATUS" and isinstance(outer.files.get(path + "#status"), dict):
                    return self._send(200, {"FileStatus": outer.files[path + "#status"]})
                self._send(404, {"RemoteException": {"exception": "FileNotFoundException", "message": f"File {path} does not exist."}})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
