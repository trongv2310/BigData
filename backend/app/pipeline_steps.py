"""GET /api/pipeline/steps: trạng thái 9 bước pipeline, đọc từ /co/output/step_summary.json (backend CHỈ ĐỌC; các script trong pipeline/ ghi file này).

Định dạng file: {"version": 1, "steps": {"<id>": {"state": "running|partial|done|failed", "updatedAt": ISO, "summary": [{"label", "value", "unit"}], "detail": str|null}}}.
Bước không có trong file là `notStarted`. Backend không tự bịa trạng thái hay số liệu: mục nào hỏng định dạng thì bỏ riêng mục đó và nói rõ ở `detail`.
"""
import json
import re

from . import timeutil

STEPS = [
    ("prepare-hdfs", "Chuẩn bị HDFS"),
    ("upload-raw", "Upload Dataset.csv"),
    ("read-spark", "Đọc bằng Spark"),
    ("clean", "Làm sạch"),
    ("spark-sql", "Spark SQL"),
    ("cluster-and-model", "Phân cụm ngày và mô hình dự đoán CO"),
    ("save-results", "Lưu kết quả"),
    ("kafka-ingest", "Kafka ingest và dự đoán trên luồng sống"),
    ("airflow-dag", "Airflow DAG"),
]
_DATETIME = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:\d{2}|Z)$")
_STATES = ("running", "partial", "done", "failed")


def _summary(items) -> tuple[list[dict], int]:
    """Các mục hợp lệ và số mục bị bỏ vì sai định dạng."""
    out, dropped = [], 0
    for it in items if isinstance(items, list) else []:
        ok = (isinstance(it, dict) and isinstance(it.get("label"), str) and not isinstance(it.get("value"), bool)
              and isinstance(it.get("value"), (str, int, float, type(None))) and isinstance(it.get("unit"), (str, type(None))))
        if ok:
            out.append({"label": it["label"], "value": it["value"], "unit": it["unit"]})
        else:
            dropped += 1
    if items is not None and not isinstance(items, list):
        dropped += 1
    return out, dropped


def build(doc) -> dict:
    """doc: nội dung step_summary.json đã parse, hoặc None nếu chưa có file."""
    entries = doc.get("steps") if isinstance(doc, dict) and isinstance(doc.get("steps"), dict) else {}
    steps = []
    for order, (sid, title) in enumerate(STEPS, 1):
        e = entries.get(sid)
        step = {"order": order, "id": sid, "title": title, "state": "notStarted", "updatedAt": None, "summary": [], "detail": None}
        if isinstance(e, dict):
            if e.get("state") in _STATES:
                step["state"] = e["state"]
                if isinstance(e.get("updatedAt"), str) and _DATETIME.match(e["updatedAt"]):
                    step["updatedAt"] = e["updatedAt"]
                step["summary"], dropped = _summary(e.get("summary"))
                detail = e.get("detail") if isinstance(e.get("detail"), str) else None
                if dropped:
                    detail = ((detail + " ") if detail else "") + f"(Backend bỏ {dropped} mục summary sai định dạng trong step_summary.json.)"
                step["detail"] = detail
            else:
                step["state"], step["detail"] = "failed", f"Mục của bước này trong step_summary.json có state không hợp lệ ({e.get('state')!r}); backend không đoán trạng thái."
        steps.append(step)
    return {"generatedAt": timeutil.now_iso(), "steps": steps}


def parse(text: str):
    return json.loads(text)
