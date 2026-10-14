#!/usr/bin/env python3
"""Kiểm tra hợp đồng không bị lệch.

  python docs/contract/validate_contract.py

Cần: pip install pyyaml 'jsonschema>=4.18'
Kiểm tra:
  1. openapi.yaml là schema hợp lệ (mọi $ref trỏ được, mọi schema đúng Draft 2020-12).
  2. Mọi path + method trong openapi.yaml có dòng trong mock/routes.json, và ngược lại.
  3. Mỗi file mock khớp schema khai báo trong routes.json, kể cả các 'variants' (status của variant phải được openapi khai báo; status >= 400 khớp Error).
  4. Mỗi dòng mock/stream-events.ndjson khớp StreamEvent và seq tăng dần.
  5. Nhất quán giữa các file mock (JSON Schema không diễn đạt được): replay-dataset có originalDatetime còn live-* thì null; 9 bước pipeline đúng thứ tự;
     đúng một mô hình isBest và phiên bản của nó trùng modelVersion/predict; cờ beats* khớp RMSE so với hai mốc.
Thoát mã 1 nếu có lỗi.
"""
import json, sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):  # Windows mặc định cp1252, không in được tiếng Việt
    sys.stdout.reconfigure(encoding="utf-8")

import yaml
try:
    from jsonschema import Draft202012Validator
except ImportError:
    sys.exit("Cần jsonschema >= 4.0: pip install -U 'jsonschema>=4.18' pyyaml")

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

# 3. mock khớp schema (route chính và các variants)
def check_file(fn, schema_name):
    v = validator_for(schema_name)
    if v is None:
        return
    data = json.loads((MOCK / fn).read_text(encoding="utf-8"))
    for e in sorted(v.iter_errors(data), key=lambda e: list(e.path)):
        err(f"{fn}: {'/'.join(map(str, e.path)) or '(gốc)'}: {e.message}")


for r in routes:
    files = [r["file"]]
    if "{id}" in r["file"]:
        files = sorted(p.name for p in MOCK.glob(r["file"].replace("{id}", "*")))
        if not files:
            err(f"không có file mock nào cho {r['path']}")
    for fn in files:
        check_file(fn, r["schema"])
    declared = {str(c) for c in spec["paths"][r["path"]][r["method"].lower()]["responses"]} if r["path"] in spec["paths"] else set()
    for var in r.get("variants", []):
        status = var["status"]
        if str(status) not in declared:
            err(f"{r['method']} {r['path']}: variant {var['query']} trả {status} nhưng openapi không khai báo response này")
        check_file(var["file"], r["schema"] if status < 400 else "Error")

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


# 5. nhất quán giữa các file mock
def load(fn):
    return json.loads((MOCK / fn).read_text(encoding="utf-8"))


def check_reading(where, d):
    if d["source"] == "replay-dataset" and d["originalDatetime"] is None:
        err(f"{where}: bản ghi replay-dataset phải có originalDatetime")
    if d["source"].startswith("live-") and d["originalDatetime"] is not None:
        err(f"{where}: bản ghi {d['source']} phải có originalDatetime = null")
    if (d["coPredicted"] is None) != (d["modelVersion"] is None):
        err(f"{where}: coPredicted và modelVersion phải cùng null hoặc cùng có giá trị")


steps = load("pipeline-steps.json")["steps"]
if [s_["order"] for s_ in steps] != list(range(1, 10)):
    err("pipeline-steps.json: order phải là 1 đến 9 theo thứ tự")
want_ids = schemas["PipelineStep"]["properties"]["id"]["enum"]
if [s_["id"] for s_ in steps] != want_ids:
    err(f"pipeline-steps.json: id theo thứ tự phải là {want_ids}")
for s_ in steps:
    if s_["state"] == "notStarted" and (s_["updatedAt"] is not None or s_["summary"]):
        err(f"pipeline-steps.json: bước {s_['order']} notStarted nhưng có updatedAt hoặc summary")

mm = load("model-metrics.json")
best = [m_ for m_ in mm["models"] if m_["isBest"]]
if len(best) != 1:
    err("model-metrics.json: phải có đúng một mô hình isBest")
base = {b["id"]: b["test"]["rmse"] for b in mm["baselines"]}
if sorted(base) != ["hourly-mean", "previous-hour"]:
    err("model-metrics.json: baselines phải là previous-hour và hourly-mean")
else:
    for m_ in mm["models"]:
        if m_["beatsPreviousHour"] != (m_["test"]["rmse"] < base["previous-hour"]):
            err(f"model-metrics.json: {m_['name']}.beatsPreviousHour không khớp RMSE")
        if m_["beatsHourlyMean"] != (m_["test"]["rmse"] < base["hourly-mean"]):
            err(f"model-metrics.json: {m_['name']}.beatsHourlyMean không khớp RMSE")
if mm["trainSet"]["to"] >= mm["testSet"]["from"]:
    err("model-metrics.json: tập huấn luyện phải kết thúc trước khi tập kiểm tra bắt đầu (chia theo thời gian)")
if len(best) == 1:
    pm = load("predict.json")["model"]
    if pm["kind"] != "ml" or pm["version"] != best[0]["version"]:
        err("predict.json: model phải là kind=ml và version trùng mô hình isBest của model-metrics.json")
    for fn in ("aqi-latest.json", "aqi-latest-live-openweather.json", "aqi-latest-replay-dataset.json"):
        d = load(fn)
        check_reading(fn, d)
        if d["modelVersion"] not in (None, best[0]["version"]):
            err(f"{fn}: modelVersion không trùng mô hình isBest")
for i, line in enumerate((MOCK / "stream-events.ndjson").read_text(encoding="utf-8").splitlines(), 1):
    if line.strip() and json.loads(line)["type"] == "reading":
        check_reading(f"stream-events.ndjson dòng {i}", json.loads(line)["data"])
for fn, src in (("aqi-latest-live-openweather.json", "live-openweather"), ("aqi-latest-replay-dataset.json", "replay-dataset")):
    if load(fn)["source"] != src:
        err(f"{fn}: source phải là {src}")

if errors:
    print(f"\n{len(errors)} lỗi")
    sys.exit(1)
print("OK: openapi.yaml hợp lệ, mock khớp schema (cả variants), routes đủ, stream-events đúng, các mock nhất quán với nhau.")
