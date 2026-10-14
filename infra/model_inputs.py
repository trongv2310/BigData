"""Dựng `modelInputs` (đầu vào của mô hình dự đoán CO, bước A5) cho từng bản ghi trước khi gửi vào Kafka. Hàm thuần túy, không cần Kafka/Spark/mạng.

Mô hình (pipeline/modeling.py) dự đoán CO của giờ t từ: hour, month của t (giờ +07); temperatureC, windSpeedKmh, no, no2, o3, so2, pm25, pm10, nh3 của giờ t-1; CO của các giờ t-1, t-2, t-3, t-24.
Độ trễ KHÔNG tính được từ chính luồng (cần 24 giờ lịch sử và luồng sống chỉ có một quan sát mỗi giờ), nên mỗi producer tự lấy từ nơi nó có lịch sử:
  - phát lại: từ chính bộ dữ liệu, theo thời điểm gốc (originalDatetime);
  - dữ liệu thật OpenWeather: từ air_pollution/history (điểm theo giờ, đúng :00), tìm điểm lịch sử có ĐÚNG giá trị của quan sát hiện tại rồi lấy các giờ liền trước.
Không đủ lịch sử liên tục thì trả None (không đoán, không điền): bản ghi vẫn đi tiếp, chỉ không có dự đoán.
"""
from datetime import datetime, timedelta, timezone

VN = timezone(timedelta(hours=7))
FEATURES = ["hour", "month", "temperatureC", "windSpeedKmh", "no", "no2", "o3", "so2", "pm25", "pm10", "nh3", "coLag1h", "coLag2h", "coLag3h", "coLag24h"]
LAGS = (1, 2, 3, 24)
GASES = ("no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3")  # tên trong OpenWeather/bộ Kaggle; trong modelInputs pm2_5 gọi là pm25
HOUR = timedelta(hours=1)


def _inputs(target_time: datetime, prev_weather: tuple, prev_gases: dict, co_lags: dict) -> dict | None:
    """Ghép dict modelInputs; thiếu bất kỳ giá trị nào (None) thì trả None."""
    out = {"hour": target_time.hour, "month": target_time.month, "temperatureC": prev_weather[0], "windSpeedKmh": prev_weather[1],
           "no": prev_gases["no"], "no2": prev_gases["no2"], "o3": prev_gases["o3"], "so2": prev_gases["so2"], "pm25": prev_gases["pm2_5"],
           "pm10": prev_gases["pm10"], "nh3": prev_gases["nh3"], **{f"coLag{k}h": co_lags[k] for k in LAGS}}
    if any(v is None for v in out.values()):
        return None
    return {k: float(v) if k not in ("hour", "month") else int(v) for k, v in out.items()}


def from_dataset(rows: list[dict], i: int) -> dict | None:
    """rows: các dòng giờ theo thứ tự thời gian, mỗi dòng {orig (datetime +07), co, wind, temp, no, no2, o3, so2, pm2_5, pm10, nh3}. Đầu vào để dự đoán dòng i.
    Cần các dòng i-1, i-2, i-3, i-24 liên tục đúng k giờ (kiểm theo `orig`), nếu không thì None."""
    if i < max(LAGS):
        return None
    cur = rows[i]
    for k in LAGS:
        if cur["orig"] - rows[i - k]["orig"] != k * HOUR:
            return None
    prev = rows[i - 1]
    return _inputs(cur["orig"], (prev["temp"], prev["wind"]), prev, {k: rows[i - k]["co"] for k in LAGS})


def locate_current(history: list[dict], current_components: dict, tolerance: float = 1e-6) -> int | None:
    """Chỉ số của điểm lịch sử có ĐÚNG giá trị của quan sát hiện tại (CO và 7 chất khí); lấy điểm mới nhất nếu trùng nhiều điểm. Không khớp thì None.
    Lý do: `dt` của API hiện tại không phải thời điểm quan sát đáng tin (A1), còn giá trị hiện tại bằng điểm lịch sử của giờ hiện hành."""
    names = ("co", *GASES)
    for j in range(len(history) - 1, -1, -1):
        comp = history[j]["components"]
        if all(comp.get(n) is not None and current_components.get(n) is not None and abs(comp[n] - current_components[n]) <= tolerance for n in names):
            return j
    return None


def forecast_flag(history: list[dict], current_components: dict, observed_dt: int | None) -> bool | None:
    """True nếu điểm lịch sử khớp giá trị hiện tại nằm SAU `dt` của API (giá trị hiện tại thực ra là điểm DỰ BÁO của giờ kế tiếp), False nếu không (giờ hiện hành hoặc đã qua),
    None nếu không xác định được (không biết dt, hoặc không có điểm lịch sử khớp). Đây là giá trị của trường `coIsForecast`."""
    if observed_dt is None:
        return None
    pts = sorted(history, key=lambda h: h["dt"])
    j = locate_current(pts, current_components)
    return None if j is None else pts[j]["dt"] > observed_dt


def from_history(history: list[dict], current_components: dict, weather: dict, observed_dt: int | None = None) -> tuple[dict | None, str]:
    """history: điểm theo giờ của OpenWeather {dt (unix), components}, sắp tăng dần; weather = {temperatureC, windSpeedKmh} (hiện tại).
    Trả (modelInputs hoặc None, ghi chú cho người đọc). Nhiệt độ và gió là của quan sát hiện tại chứ không phải giờ trước (API lịch sử không có thời tiết): xấp xỉ, ghi ở ghi chú.
    observed_dt: `dt` của API hiện tại; nếu điểm lịch sử khớp nằm SAU nó thì giá trị "hiện tại" thực ra là điểm DỰ BÁO của giờ kế tiếp (API lịch sử trả cả điểm tương lai; đo ngày 14/10/2026
    lúc 16:30 trả điểm 17:00), nên ghi chú nói rõ: `co` của bản ghi khi đó là giá trị dự báo của OpenWeather chứ không phải quan sát."""
    pts = sorted(history, key=lambda h: h["dt"])
    j = locate_current(pts, current_components)
    if j is None:
        return None, "không có điểm lịch sử nào khớp giá trị hiện tại nên không dựng được độ trễ"
    if j < max(LAGS):
        return None, f"lịch sử chỉ có {j} giờ trước quan sát hiện tại, cần {max(LAGS)}"
    cur_dt = pts[j]["dt"]
    for k in LAGS:
        if cur_dt - pts[j - k]["dt"] != k * 3600:
            return None, f"lịch sử không liên tục theo giờ quanh quan sát hiện tại (thiếu giờ t-{k})"
    target_time = datetime.fromtimestamp(cur_dt, timezone.utc).astimezone(VN)
    prev = pts[j - 1]["components"]
    out = _inputs(target_time, (weather.get("temperatureC"), weather.get("windSpeedKmh")), prev,
                  {k: pts[j - k]["components"].get("co") for k in LAGS})
    if out is None:
        return None, "lịch sử hoặc thời tiết thiếu giá trị"
    note = ("CO và các chất khí các giờ trước lấy từ air_pollution/history của OpenWeather; nhiệt độ và gió lấy của quan sát hiện tại "
            "(không có lịch sử thời tiết), nên lệch nhẹ so với định nghĩa lúc huấn luyện (giờ t-1)")
    if observed_dt is not None and cur_dt > observed_dt:
        note += (f". LƯU Ý: giá trị hiện tại khớp điểm lịch sử của {target_time:%H:%M} (+07), sau thời điểm API báo ({datetime.fromtimestamp(observed_dt, timezone.utc).astimezone(VN):%H:%M:%S}), nhiều khả năng là giá trị DỰ BÁO của OpenWeather "
                 "cho giờ kế tiếp, không phải quan sát")
    return out, note
