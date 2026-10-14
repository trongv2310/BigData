"""/api/predict dùng mô hình học máy thật khi pipeline đã huấn luyện (kind = "ml"); chưa có mô hình thì trả None để route dùng heuristic như trước.

Đầu vào mô hình = các đặc trưng ở /co/output/model_inputs_reference.json (hour, month, temperatureC, windSpeedKmh, no, no2, o3, so2, pm25, pm10, nh3, coLag1h, coLag2h,
coLag3h, coLag24h). Form chỉ có giờ, tháng, nhiệt độ, gió; các đầu vào còn lại, nếu yêu cầu bỏ trống, được điền bằng TRUNG VỊ của tập huấn luyện và liệt kê ở `filledInputs`
(không giấu việc điền). temperatureC và windSpeedKmh là của giờ ngay trước giờ cần dự đoán, đúng như lúc huấn luyện.
"""
import json

from . import config, ml_engine, reading
from .clients import hdfs

FILL_ORIGIN = "trung vị của tập huấn luyện (giai đoạn sau chuyển tiếp), vì yêu cầu bỏ trống"


async def _read(path: str) -> dict | None:
    try:
        doc = json.loads(await hdfs.read_text(path))
    except (hdfs.HdfsNotFound, hdfs.HdfsUnreachable, hdfs.HdfsError, ValueError):
        return None
    return doc if isinstance(doc, dict) else None


async def try_predict(req, run=None) -> dict | None:
    """Phản hồi PredictResponse kind=ml, hoặc None nếu chưa có mô hình (hoặc không đọc được tệp mô tả mô hình). Ném ml_engine.MlUnavailable nếu CÓ mô hình nhưng không chạy được."""
    ref = await _read(config.MODEL_REFERENCE_PATH)
    metrics = await _read(config.MODEL_METRICS_PATH)
    if not ref or not metrics or not isinstance(ref.get("median"), dict) or not isinstance(ref.get("features"), list):
        return None
    best = next((m for m in metrics.get("models", []) if m.get("isBest")), None)
    if best is None or best.get("version") != ref.get("modelVersion"):
        return None  # hai tệp không cùng một lần huấn luyện: không dùng, không đoán
    given = req.model_dump()
    values, filled = {}, []
    for name in ref["features"]:
        v = given.get(name)
        if v is None:
            if name not in ref["median"]:
                return None
            v = ref["median"][name]
            filled.append({"name": name, "value": float(v), "origin": FILL_ORIGIN})
        values[name] = float(v)
    import asyncio
    co = await asyncio.to_thread(run or ml_engine.predict, config.MODEL_DIR_URI, best["version"], ref["features"], values)
    co = round(co, 1)
    return {
        "coPredicted": co, "unit": "µg/m³", "status": reading.status_for(co), "interval": None, "filledInputs": filled,
        "model": {"name": best["name"], "version": best["version"], "kind": "ml",
                  "trainedOn": (metrics["trainedOn"] + " Mô hình này không có khoảng dự đoán. Đầu vào bỏ trống được điền bằng trung vị (xem filledInputs); "
                                "kết quả chỉ đáng tin khi các đầu vào quan trọng (đặc biệt CO các giờ trước) được cung cấp. " + metrics["conclusion"])},
    }
