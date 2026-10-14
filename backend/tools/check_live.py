#!/usr/bin/env python3
"""Gọi backend đang chạy, kiểm từng route với openapi.yaml và so cấu trúc với file mock cùng route.

  python3 backend/tools/check_live.py [http://127.0.0.1:8000]

Chỉ đọc (GET, và POST của /api/predict, /api/sql khi chúng đã có). Thoát mã 1 nếu có route sai schema.
"""
import json, sys
from pathlib import Path

import httpx
import yaml
from jsonschema import Draft202012Validator

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parents[2]
SPEC = yaml.safe_load((ROOT / "docs/contract/openapi.yaml").read_text(encoding="utf-8"))
SCHEMAS = SPEC["components"]["schemas"]
MOCK = ROOT / "mock"
ROUTES = json.loads((MOCK / "routes.json").read_text(encoding="utf-8"))["routes"]
BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"

POST_BODIES = {
    "/api/predict": {"hour": 18, "month": 11, "temperatureC": 30, "windSpeedKmh": 8},
    "/api/sql": {"query": "SELECT 1 AS x", "limit": 5},
}


def validator(name):
    return Draft202012Validator({"$schema": "https://json-schema.org/draft/2020-12/schema",
                                 "$ref": f"#/components/schemas/{name}", "components": {"schemas": SCHEMAS}})


def shape_diff(real, mock, path="$"):
    """Khác biệt về cấu trúc (khóa, kiểu); giá trị khác nhau là bình thường."""
    out = []
    if isinstance(real, dict) and isinstance(mock, dict):
        for k in sorted(set(real) | set(mock)):
            if k not in real:
                out.append(f"{path}.{k}: mock có, thật không có")
            elif k not in mock:
                out.append(f"{path}.{k}: thật có, mock không có")
            else:
                out += shape_diff(real[k], mock[k], f"{path}.{k}")
    elif isinstance(real, list) and isinstance(mock, list):
        if real and mock:
            out += shape_diff(real[0], mock[0], path + "[0]")
    elif type(real) is not type(mock) and not (isinstance(real, (int, float)) and isinstance(mock, (int, float))) \
            and real is not None and mock is not None:
        out.append(f"{path}: kiểu {type(real).__name__} (thật) và {type(mock).__name__} (mock)")
    return out


def cases(http):
    for r in ROUTES:
        if "{id}" in r["path"]:
            ids = [f["id"] for f in http.get(BASE + "/api/flows").json().get("flows", []) if f["status"] == "available"]
            for i in ids:
                yield r, r["path"].replace("{id}", i), {}, r["file"].replace("{id}", i)
        elif r["path"] == "/api/hdfs/blocks":
            yield r, r["path"], {"path": "/raw/Dataset.csv"}, r["file"]
        else:
            yield r, r["path"], {}, r["file"]


def main():
    bad = 0
    with httpx.Client(timeout=30) as http:
        for r, path, params, mock_file in cases(http):
            if r["method"] == "POST" and path not in POST_BODIES:
                print(f"--  POST {path}: bỏ qua (công cụ này chỉ đọc; route đổi trạng thái, ví dụ /api/replay/control, tự thử bằng tay)")
                continue
            if r["method"] == "POST":
                resp = http.post(BASE + path, json=POST_BODIES[path])
            else:
                resp = http.get(BASE + path, params=params)
            label = f"{r['method']} {path}" + (f" {params}" if params else "")
            if resp.status_code != 200:
                print(f"--  {label}: HTTP {resp.status_code} {resp.text[:120]}")
                continue
            errs = [f"{'/'.join(map(str, e.path)) or '(gốc)'}: {e.message}" for e in validator(r["schema"]).iter_errors(resp.json())]
            mock = json.loads((MOCK / mock_file).read_text(encoding="utf-8")) if (MOCK / mock_file).is_file() else None
            diffs = shape_diff(resp.json(), mock) if mock is not None else ["(không có file mock)"]
            print(f"{'LỖI' if errs else 'OK '} {label}")
            for e in errs[:5]:
                print("      schema:", e)
            for d in diffs[:8]:
                print("      lệch mock:", d)
            bad += bool(errs)
    sys.exit(1 if bad else 0)


main()
