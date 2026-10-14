"""Chuyển một bản ghi của luồng /stream_input (schema của sensor_producer_*.py) sang AqiReading của hợp đồng."""
import json
from datetime import datetime

from . import config, timeutil

LIVE_STATION = "Tram_TPHCM_Live_API"      # Open-Meteo (infra/live_producer.py), không bật mặc định
OWM_STATION = "Tram_TPHCM_OWM"            # OpenWeather (infra/live_producer_owm.py)
# Nhãn trạm trong producer replay gán luân phiên theo số thứ tự dòng, không phải trạm đo thật.
STATION_LABELS = {
    "Tram_Quan_1": "Quận 1",
    "Tram_Binh_Thanh": "Bình Thạnh",
    "Tram_Thu_Duc": "Thủ Đức",
    "Tram_Quan_5": "Quận 5",
    LIVE_STATION: "TP.HCM (Open-Meteo)",
    OWM_STATION: "TP.HCM (OpenWeather)",
}


def source_of(rec: dict) -> str:
    """Nguồn của một bản ghi. Tin mới có `provider`; tin cũ (file /stream_input, Open-Meteo trước 14/10/2026) suy ra từ nhãn trạm."""
    provider = rec.get("provider")
    if provider == "replay-dataset":
        return "replay-dataset"
    if provider == "openweather" or rec.get("station") == OWM_STATION:
        return "live-openweather"
    if provider == "open-meteo" or rec.get("station") == LIVE_STATION:
        return "live-open-meteo"
    return "replay-dataset"  # producer replay cũ ghi file vào HDFS, không có provider


def status_for(co: float) -> str:
    """Quyết định D3: ngưỡng theo thang CO của OpenWeather (config.CO_WARNING_UG, CO_DANGER_UG); mốc thuộc về mức cao hơn như thang gốc."""
    if co >= config.CO_DANGER_UG:
        return "DANGER"
    if co >= config.CO_WARNING_UG:
        return "WARNING"
    return "SAFE"


def from_record(rec: dict) -> dict:
    """rec: {"datetime": "YYYY-MM-DD HH:MM:SS" (giờ hệ thống của máy chạy producer, WSL = +07), station, co, wind_speed_kmh, temperature_C,
    tùy chọn: provider, originalDatetime (chỉ bản phát lại), ...}."""
    co = float(rec["co"])
    wind = float(rec["wind_speed_kmh"])
    station = rec.get("station") or ""
    source = source_of(rec)
    predicted, version = rec.get("co_predicted"), rec.get("model_version")
    predicted = float(predicted) if isinstance(predicted, (int, float)) and not isinstance(predicted, bool) and isinstance(version, str) and version else None
    flag = rec.get("co_is_forecast", rec.get("coIsForecast"))  # bản ghi làm giàu của Spark dùng tên snake_case, bản ghi thô của producer dùng camelCase
    flag = flag if isinstance(flag, bool) else None  # chỉ nhận boolean thật; thiếu hoặc kiểu khác là null (không suy ra)
    original = None
    if source == "replay-dataset" and rec.get("originalDatetime"):  # thời điểm gốc của dòng lịch sử; nguồn thật luôn null
        original = timeutil.iso(datetime.fromisoformat(rec["originalDatetime"]))
    return {
        "datetime": timeutil.iso(datetime.fromisoformat(rec["datetime"])),
        # Producer phát lại cũ (HDFS /stream_input) không gửi thời điểm gốc nên vẫn null; producer Kafka (Bước A1) có gửi.
        "originalDatetime": original,
        "station": STATION_LABELS.get(station, station),
        # Quyết định D2 (TẠM): luôn true, kể cả luồng live (một điểm tọa độ chung, không phải trạm đo).
        "isSimulatedStation": True,
        "source": source,
        "co": co,
        "windSpeedKmh": wind,
        "temperatureC": float(rec["temperature_C"]),
        "coAccumulationIndex": round(co / (wind + 1.0), 2),
        "status": status_for(co),
        # do Spark (infra/spark_kafka_consumer.py) thêm vào bản ghi đã làm giàu; bản ghi thô, hoặc thiếu đầu vào cho mô hình: null, không điền số thay thế
        "coPredicted": predicted,
        "modelVersion": version if predicted is not None else None,
        "coIsForecast": flag,
    }


def parse_file_text(text: str) -> dict | None:
    """File trong /stream_input có một dòng JSON mỗi file. Lấy dòng cuối hợp lệ; hỏng thì None."""
    for line in reversed(text.splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            return from_record(json.loads(line))
        except (ValueError, KeyError, TypeError):
            return None
    return None
