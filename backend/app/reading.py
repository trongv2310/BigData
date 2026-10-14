"""Chuyển một bản ghi của luồng /stream_input (schema của sensor_producer_*.py) sang AqiReading của hợp đồng."""
import json
from datetime import datetime

from . import config, timeutil

LIVE_STATION = "Tram_TPHCM_Live_API"
# Nhãn trạm trong producer replay gán luân phiên theo số thứ tự dòng, không phải trạm đo thật.
STATION_LABELS = {
    "Tram_Quan_1": "Quận 1",
    "Tram_Binh_Thanh": "Bình Thạnh",
    "Tram_Thu_Duc": "Thủ Đức",
    "Tram_Quan_5": "Quận 5",
    LIVE_STATION: "TP.HCM (Open-Meteo)",
}


def status_for(co: float) -> str:
    """Quyết định D3: ngưỡng theo thang CO của OpenWeather (config.CO_WARNING_UG, CO_DANGER_UG); mốc thuộc về mức cao hơn như thang gốc."""
    if co >= config.CO_DANGER_UG:
        return "DANGER"
    if co >= config.CO_WARNING_UG:
        return "WARNING"
    return "SAFE"


def from_record(rec: dict) -> dict:
    """rec: {"datetime": "YYYY-MM-DD HH:MM:SS" (giờ hệ thống của máy chạy producer, WSL = +07), station, co, wind_speed_kmh, temperature_C}."""
    co = float(rec["co"])
    wind = float(rec["wind_speed_kmh"])
    station = rec.get("station") or ""
    return {
        "datetime": timeutil.iso(datetime.fromisoformat(rec["datetime"])),
        # Hợp đồng 0.2.0: trước Bước A1 producer chưa gửi thời điểm gốc nên chưa điền được cho bản phát lại; không đoán. coPredicted/modelVersion
        # chỉ có từ Bước A5 (chưa có mô hình), nên luôn null, không điền số thay thế.
        "originalDatetime": None,
        "station": STATION_LABELS.get(station, station),
        # Quyết định D2 (TẠM): luôn true, kể cả luồng live (một điểm tọa độ chung, không phải trạm đo).
        "isSimulatedStation": True,
        "source": "live-open-meteo" if station == LIVE_STATION else "replay-dataset",
        "co": co,
        "windSpeedKmh": wind,
        "temperatureC": float(rec["temperature_C"]),
        "coAccumulationIndex": round(co / (wind + 1.0), 2),
        "status": status_for(co),
        "coPredicted": None,
        "modelVersion": None,
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
