#!/usr/bin/env python3
"""Kiểm tra hợp đồng không bị lệch.

  python docs/contract/validate_contract.py

Cần: pip install pyyaml jsonschema
Kiểm tra:
  1. openapi.yaml là schema hợp lệ (mọi $ref trỏ được, mọi schema đúng Draft 2020-12).
  2. Mọi path + method trong openapi.yaml có dòng trong mock/routes.json, và ngược lại.
  3. Mỗi file mock khớp schema khai báo trong routes.json.
  4. Mỗi dòng mock/stream-events.ndjson khớp StreamEvent và seq tăng dần.
Thoát mã 1 nếu có lỗi.
"""
import json, sys
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]
SPEC = ROOT / "docs" / "contract" / "openapi.yaml"
MOCK = ROOT / "mock"

errors = []


def err(msg):
    errors.append(msg)
    print("LỖI:", msg)


spec = yaml.safe_load(SPEC.read_text(encoding="utf-8"))
schemas = spec["components"]["schemas"]


def validator_for(name):
    if name not in schemas:
        err(f"schema '{name}' không có trong components.schemas")
        return None
    wrapper = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$ref": f"#/components/schemas/{name}",
        "components": {"schemas": schemas},
    }
    return Draft202012Validator(wrapper)


# 1. mọi schema hợp lệ
for name, sch in schemas.items():
    try:
        Draft202012Validator.check_schema(sch)
    except Exception as ex:
        err(f"schema {name} không hợp lệ: {ex}")

# 2. đối chiếu route
routes = json.loads((MOCK / "routes.json").read_text(encoding="utf-8"))["routes"]
spec_ops = {(m.upper(), p) for p, item in spec["paths"].items() for m in item if m in ("get", "post", "put", "delete", "patch")}
route_ops = {(r["method"], r["path"]) for r in routes}
for op in sorted(spec_ops - route_ops):
    err(f"openapi có {op[0]} {op[1]} nhưng mock/routes.json chưa có")
for op in sorted(route_ops - spec_ops):
    err(f"mock/routes.json có {op[0]} {op[1]} nhưng openapi chưa khai báo")

# 3. mock khớp schema
for r in routes:
    files = [r["file"]]
    if "{id}" in r["file"]:
        files = sorted(p.name for p in MOCK.glob(r["file"].replace("{id}", "*")))
        if not files:
            err(f"không có file mock nào cho {r['path']}")
    v = validator_for(r["schema"])
    if v is None:
        continue
    for fn in files:
        data = json.loads((MOCK / fn).read_text(encoding="utf-8"))
        for e in sorted(v.iter_errors(data), key=lambda e: list(e.path)):
            err(f"{fn}: {'/'.join(map(str, e.path)) or '(gốc)'}: {e.message}")

# 4. luồng sự kiện
v = validator_for("StreamEvent")
last = -1
for i, line in enumerate((MOCK / "stream-events.ndjson").read_text(encoding="utf-8").splitlines(), 1):
    if not line.strip():
        continue
    ev = json.loads(line)
    if v:
        for e in v.iter_errors(ev):
            err(f"stream-events.ndjson dòng {i}: {e.message}")
    if ev.get("seq", -1) <= last:
        err(f"stream-events.ndjson dòng {i}: seq không tăng dần")
    last = ev.get("seq", last)

if errors:
    print(f"\n{len(errors)} lỗi")
    sys.exit(1)
print("OK: openapi.yaml hợp lệ, mock khớp schema, routes đủ, stream-events đúng.")
