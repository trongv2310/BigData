"""Producer DỮ LIỆU THẬT: CO và các chất khí hiện tại của TP.HCM từ OpenWeather Air Pollution API, gửi vào Kafka (topic hcmc_co_stream).

Cùng nhà cung cấp với bản raw `scripts/fetch_owm_raw.py` (thư mục cha) và cùng tọa độ (10.8231, 106.6297), nên đây là nguồn mô hình dự đoán áp dụng được.
  - Khóa API CHỈ đọc từ biến môi trường OWM_API_KEY; không in ra màn hình, log hay Kafka (mọi chuỗi lỗi đều được che khóa trước khi in).
  - Air Pollution API không có nhiệt độ và gió, mà hợp đồng (AqiReading) bắt buộc cả hai: lấy từ Current Weather API của CHÍNH OpenWeather
    (cùng khóa, cùng gói miễn phí). Thiếu một trong hai phản hồi thì bỏ qua lần đó, không điền số giả.
  - `dt` (Unix UTC) của Air Pollution được đổi sang giờ Việt Nam (+07:00) và là `datetime` của bản ghi. ĐO THỰC TẾ 14/10/2026: `dt` KHÔNG đơn điệu
    (cùng một số liệu đôi lúc kèm `dt` mới hơn rồi `dt` cũ hơn ở lần hỏi sau, giống như các máy chủ cache khác nhau) và có lúc đổi hàng chục phút
    mà số liệu khí y hệt. Nên chỉ GỬI khi `dt` LỚN HƠN `dt` đã gửi VÀ bộ số liệu khí (CO và 7 chất khác) khác bộ đã gửi; ngược lại bỏ qua và ghi lý do.
    Khi khởi động đọc lại các tin gần nhất trong topic để không gửi lại.
  - Lỗi mạng, 401, 429, 5xx: ghi log (đã che khóa), chờ lùi dần (tối đa 15 phút) rồi thử lại; không bịa số và không thoát.
  - Hạn mức gói miễn phí (openweathermap.org/price, tra 14/10/2026): 60 lời gọi/phút, 1.000.000 lời gọi/tháng. Mỗi lần hỏi tốn 2 lời gọi;
    mặc định hỏi mỗi 300 giây = 576 lời gọi/ngày (khoảng 17 300/tháng), chỉ bằng 1,7% hạn mức tháng.

Chạy (WSL, Kafka đang chạy):  export OWM_API_KEY=...   rồi   python3 infra/live_producer_owm.py [--interval 300] [--once]
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

try:  # kafka-python chỉ cần khi thật sự gửi/đọc Kafka; các hàm thuần túy bên dưới thử được không cần nó
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    from kafka.errors import KafkaError
except ImportError:  # pragma: no cover
    KafkaConsumer = KafkaProducer = TopicPartition = None

    class KafkaError(Exception):
        pass

LATITUDE, LONGITUDE = 10.8231, 106.6297  # đúng tọa độ của scripts/fetch_owm_raw.py
VN = timezone(timedelta(hours=7))
STATION = "Tram_TPHCM_OWM"   # backend (reading.py) nhận nhãn này và provider "openweather" là source=live-openweather
PROVIDER = "openweather"
AIR_URL = "https://api.openweathermap.org/data/2.5/air_pollution"
WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
POLLUTANTS = ["no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3"]
MAX_BACKOFF = 900.0
SCAN_BACK = 200  # khi khởi động: đọc lùi tối đa ngần này tin cuối mỗi partition để tìm `dt` OpenWeather đã gửi


def scrub(text: object, key: str | None) -> str:
    """Chuỗi lỗi có thể chứa URL kèm appid=<khóa>: che khóa trước khi in."""
    s = str(text)
    if key:
        s = s.replace(key, "***")
    return s


def vn_time(unix: int) -> datetime:
    return datetime.fromtimestamp(unix, timezone.utc).astimezone(VN)


def to_record(air: dict, weather: dict) -> dict:
    """Hai phản hồi API -> bản ghi gửi Kafka. Ném ValueError nếu thiếu trường (không điền số giả)."""
    try:
        item = air["list"][0]
        co = item["components"]["co"]
        air_dt = int(item["dt"])
        temp = weather["main"]["temp"]
        wind_ms = weather["wind"]["speed"]
        weather_dt = int(weather["dt"])
    except (KeyError, IndexError, TypeError) as e:
        raise ValueError(f"phản hồi thiếu trường {e!r}") from e
    if co is None or temp is None or wind_ms is None:
        raise ValueError("API trả giá trị rỗng cho CO, nhiệt độ hoặc gió")
    rec = {
        "datetime": vn_time(air_dt).strftime("%Y-%m-%d %H:%M:%S"),
        "station": STATION,
        "co": float(co),
        "wind_speed_kmh": round(float(wind_ms) * 3.6, 2),  # Current Weather trả m/s (units=metric)
        "temperature_C": float(temp),
        "coObservedAt": vn_time(air_dt).strftime("%Y-%m-%dT%H:%M:%S"),
        "weatherObservedAt": vn_time(weather_dt).strftime("%Y-%m-%dT%H:%M:%S"),
        "provider": PROVIDER,
        "owmDt": air_dt,
        "aqi": item.get("main", {}).get("aqi"),
    }
    for name in POLLUTANTS:
        rec[name] = item["components"].get(name)
    return rec


def fetch_record(key: str, session: requests.Session | None = None) -> dict:
    s = session or requests
    params = {"lat": LATITUDE, "lon": LONGITUDE, "appid": key}
    air = s.get(AIR_URL, params=params, timeout=15)
    air.raise_for_status()
    weather = s.get(WEATHER_URL, params={**params, "units": "metric"}, timeout=15)
    weather.raise_for_status()
    return to_record(air.json(), weather.json())


def values_of(rec: dict) -> tuple:
    """Bộ số liệu khí của một bản ghi, dùng để nhận ra "quan sát mới" (xem đầu tệp: `dt` một mình không đáng tin)."""
    return tuple(rec.get(k) for k in ("co", *POLLUTANTS))


def skip_reason(rec: dict, last: dict | None) -> str | None:
    """None nếu `rec` là quan sát mới cần gửi; ngược lại là lý do bỏ qua. `last` = {"dt": int, "values": tuple} đã gửi gần nhất."""
    if last is None:
        return None
    if rec["owmDt"] <= last["dt"]:
        return f"dt={rec['owmDt']} không mới hơn dt đã gửi ({last['dt']})"
    if values_of(rec) == last["values"]:
        return f"dt đổi ({last['dt']} -> {rec['owmDt']}) nhưng số liệu khí y hệt bản đã gửi"
    return None


def last_published(bootstrap: str, topic: str, consumer_cls=None) -> dict | None:
    """{"dt", "values"} của tin OpenWeather có `owmDt` lớn nhất trong SCAN_BACK tin cuối của topic (bỏ qua tin của nguồn khác), hoặc None.

    Chỉ đọc, không dùng consumer group. Topic dùng chung với bản phát lại nên tin cuối cùng có thể không phải của OpenWeather.
    """
    consumer_cls = consumer_cls or KafkaConsumer
    try:
        c = consumer_cls(bootstrap_servers=bootstrap, group_id=None, enable_auto_commit=False, consumer_timeout_ms=5000)
    except KafkaError as e:
        print(f"[live_producer_owm] không đọc được tin cũ để tránh gửi trùng ({type(e).__name__}: {e})", file=sys.stderr, flush=True)
        return None
    try:
        parts = [TopicPartition(topic, p) for p in sorted(c.partitions_for_topic(topic) or [])]
        if not parts:
            return None
        c.assign(parts)
        best = None
        for tp, end in c.end_offsets(parts).items():
            if end <= 0:
                continue
            c.seek(tp, max(0, end - SCAN_BACK))
            while True:
                batch = c.poll(timeout_ms=3000, max_records=SCAN_BACK).get(tp, [])
                for rec in batch:
                    try:
                        v = json.loads(rec.value.decode("utf-8"))
                    except (ValueError, AttributeError):
                        continue
                    if isinstance(v, dict) and v.get("provider") == PROVIDER and isinstance(v.get("owmDt"), int):
                        if best is None or v["owmDt"] > best["dt"]:
                            best = {"dt": v["owmDt"], "values": values_of(v)}
                if not batch or c.position(tp) >= end:
                    break
        return best
    except (KafkaError, ValueError) as e:
        print(f"[live_producer_owm] không đọc được tin cũ để tránh gửi trùng ({type(e).__name__}: {e})", file=sys.stderr, flush=True)
        return None
    finally:
        c.close()


def describe_http_error(e: requests.HTTPError, key: str | None) -> tuple[str, float]:
    """(thông điệp đã che khóa, số giây nên chờ tối thiểu) cho lỗi HTTP."""
    code = e.response.status_code if e.response is not None else 0
    if code == 401:
        return "HTTP 401: khóa API sai hoặc chưa kích hoạt (khóa mới có thể cần vài giờ)", 600.0
    if code == 429:
        return "HTTP 429: hết hạn mức gọi (gói miễn phí 60 lời gọi/phút, 1.000.000/tháng)", 120.0
    return f"HTTP {code}: {scrub(e, key)[:160]}", 0.0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--interval", type=float, default=300.0, help="giây giữa hai lần hỏi API (mặc định 300)")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="hcmc_co_stream")
    p.add_argument("--once", action="store_true", help="hỏi một lần, gửi nếu là quan sát mới, rồi thoát")
    a = p.parse_args()

    key = os.environ.get("OWM_API_KEY", "").strip()
    if not key:
        print("[live_producer_owm] thiếu biến môi trường OWM_API_KEY (chỉ đọc khóa từ biến môi trường). Ví dụ: export OWM_API_KEY=... rồi chạy lại.", file=sys.stderr)
        return 2

    producer = KafkaProducer(bootstrap_servers=a.bootstrap, acks="all", retries=5,
                             key_serializer=lambda k: k.encode("utf-8"),
                             value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"))
    print(f"[live_producer_owm] DỮ LIỆU THẬT OpenWeather ({LATITUDE}, {LONGITUDE}) -> Kafka {a.bootstrap} topic {a.topic}, hỏi API mỗi {a.interval:.0f}s", flush=True)
    last = last_published(a.bootstrap, a.topic)
    if last:
        print(f"[live_producer_owm] quan sát OpenWeather mới nhất trong topic: dt={last['dt']} ({vn_time(last['dt']):%Y-%m-%d %H:%M:%S} +07), không gửi lại", flush=True)
    session = requests.Session()
    failures = 0
    while True:
        wait = a.interval
        try:
            rec = fetch_record(key, session)
            failures = 0
            reason = skip_reason(rec, last)
            if reason is None:
                meta = producer.send(a.topic, key=STATION, value=rec).get(timeout=15)
                last = {"dt": rec["owmDt"], "values": values_of(rec)}
                print(f"[{datetime.now(VN):%H:%M:%S}] GỬI offset={meta.offset} quan sát {rec['coObservedAt']} (dt={rec['owmDt']}) | "
                      f"CO={rec['co']} µg/m³ gió={rec['wind_speed_kmh']} km/h nhiệt độ={rec['temperature_C']}°C", flush=True)
            else:
                print(f"[{datetime.now(VN):%H:%M:%S}] chưa có quan sát mới: {reason} (CO={rec['co']})", flush=True)
        except requests.HTTPError as e:
            msg, min_wait = describe_http_error(e, key)
            failures += 1
            wait = min(MAX_BACKOFF, max(min_wait, a.interval * 2 ** min(failures, 5)))
            print(f"[{datetime.now(VN):%H:%M:%S}] bỏ qua lần này: {msg}; thử lại sau {wait:.0f}s", file=sys.stderr, flush=True)
        except (requests.RequestException, ValueError) as e:
            failures += 1
            wait = min(MAX_BACKOFF, a.interval * 2 ** min(failures, 5))
            print(f"[{datetime.now(VN):%H:%M:%S}] bỏ qua lần này, lỗi lấy dữ liệu: {type(e).__name__}: {scrub(e, key)[:200]}; thử lại sau {wait:.0f}s",
                  file=sys.stderr, flush=True)
        except KafkaError as e:
            print(f"[{datetime.now(VN):%H:%M:%S}] lỗi Kafka: {type(e).__name__}: {scrub(e, key)}", file=sys.stderr, flush=True)
            if a.once:
                return 1
        if a.once:
            return 0 if (last or failures == 0) else 1
        time.sleep(wait)


if __name__ == "__main__":
    sys.exit(main())
