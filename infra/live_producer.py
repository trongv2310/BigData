"""Producer DỮ LIỆU THẬT: lấy CO và thời tiết hiện tại của TP.HCM từ Open-Meteo rồi gửi vào Kafka (topic hcmc_co_stream).

Khác sensor_producer_live_api.py ở thư mục cha (script cũ không bị sửa):
  - gửi vào Kafka, không ghi file vào HDFS;
  - gọi API mỗi --interval giây nhưng chỉ GỬI khi có quan sát mới (thời điểm `time` do API trả đổi), vì Open-Meteo cập nhật
    CO mỗi 3600 giây và thời tiết mỗi 900 giây (trường `interval` của API); gọi 3 giây một lần chỉ nhận lại cùng một số;
  - không tự điền số khi API thiếu dữ liệu (script cũ điền 450.0): thiếu thì bỏ qua lần đó và ghi log;
  - datetime của bản ghi là thời điểm quan sát của API (giờ Việt Nam), không phải giờ máy;
  - khi khởi động đọc tin mới nhất trong topic để không gửi lại quan sát đã gửi (khởi động lại không tạo bản trùng).

Lưu ý nguồn: CO của Open-Meteo là số của mô hình chất lượng không khí (CAMS), không phải cảm biến đặt tại một trạm.

Chạy (WSL, Kafka đang chạy): python3 infra/live_producer.py            # chạy liên tục
                            python3 infra/live_producer.py --once     # lấy và gửi một lần rồi thoát
"""
import argparse
import json
import sys
import time
from datetime import datetime

import requests
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import KafkaError

LATITUDE, LONGITUDE = 10.8231, 106.6297
STATION = "Tram_TPHCM_Live_API"  # backend (reading.py) nhận nhãn này là source=live-open-meteo
TZ = "Asia%2FHo_Chi_Minh"
AIR_URL = (f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude={LATITUDE}&longitude={LONGITUDE}"
           f"&current=carbon_monoxide&timezone={TZ}")
WEATHER_URL = (f"https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}"
               f"&current=temperature_2m,wind_speed_10m&timezone={TZ}")


def fetch_record() -> dict:
    """Một bản ghi từ hai lời gọi API. Ném ValueError nếu thiếu trường nào (không điền số giả)."""
    air = requests.get(AIR_URL, timeout=10)
    air.raise_for_status()
    weather = requests.get(WEATHER_URL, timeout=10)
    weather.raise_for_status()
    a, w = air.json().get("current") or {}, weather.json().get("current") or {}
    for name, cur, key in (("CO", a, "carbon_monoxide"), ("nhiệt độ", w, "temperature_2m"), ("gió", w, "wind_speed_10m")):
        if cur.get(key) is None:
            raise ValueError(f"API không trả {name} ({key})")
    co_time, weather_time = a["time"], w["time"]  # "2026-10-12T23:00" theo giờ Việt Nam
    newest = max(co_time, weather_time)
    return {
        "datetime": datetime.fromisoformat(newest).strftime("%Y-%m-%d %H:%M:%S"),
        "station": STATION,
        "co": float(a["carbon_monoxide"]),
        "wind_speed_kmh": float(w["wind_speed_10m"]),
        "temperature_C": float(w["temperature_2m"]),
        "coObservedAt": co_time,
        "weatherObservedAt": weather_time,
        "provider": "open-meteo",
    }


def last_published(bootstrap: str, topic: str, consumer_cls=KafkaConsumer):
    """(coObservedAt, weatherObservedAt) của tin mới nhất đang nằm trong topic, hoặc None nếu topic trống/không đọc được.

    Producer chỉ giữ trạng thái "đã gửi quan sát nào" trong bộ nhớ; không có hàm này thì mỗi lần khởi động lại (hay khởi động nguội
    sau khi tắt WSL) nó gửi lại quan sát hiện tại thành bản trùng (đã gặp: offset 44 và 45 giống hệt nhau). Đọc chỉ-đọc, không dùng
    consumer group nên không để lại offset commit.
    """
    try:
        c = consumer_cls(bootstrap_servers=bootstrap, group_id=None, enable_auto_commit=False,
                         consumer_timeout_ms=5000, value_deserializer=lambda v: json.loads(v.decode("utf-8")))
    except KafkaError as e:
        print(f"[live_producer] không đọc được tin cũ để tránh gửi trùng ({type(e).__name__}: {e})", file=sys.stderr, flush=True)
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
            c.seek(tp, end - 1)
            for rec in c.poll(timeout_ms=5000).get(tp, []):
                v = rec.value
                if isinstance(v, dict) and v.get("coObservedAt") and v.get("weatherObservedAt"):
                    key = (v["coObservedAt"], v["weatherObservedAt"])
                    best = key if best is None or key > best else best
        return best
    except (KafkaError, ValueError) as e:
        print(f"[live_producer] không đọc được tin cũ để tránh gửi trùng ({type(e).__name__}: {e})", file=sys.stderr, flush=True)
        return None
    finally:
        c.close()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--interval", type=float, default=60.0, help="giây giữa hai lần gọi API (mặc định 60)")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="hcmc_co_stream")
    p.add_argument("--once", action="store_true", help="gửi một lần rồi thoát")
    a = p.parse_args()

    producer = KafkaProducer(bootstrap_servers=a.bootstrap, acks="all", retries=5,
                             key_serializer=lambda k: k.encode("utf-8"),
                             value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"))
    print(f"[live_producer] DỮ LIỆU THẬT Open-Meteo -> Kafka {a.bootstrap} topic {a.topic}, hỏi API mỗi {a.interval:.0f}s", flush=True)
    last = last_published(a.bootstrap, a.topic)  # (coObservedAt, weatherObservedAt) của bản ghi đã gửi; lấy từ topic để không gửi trùng khi khởi động lại
    if last:
        print(f"[live_producer] tin mới nhất trong topic: CO {last[0]}, thời tiết {last[1]} (không gửi lại quan sát này)", flush=True)
    while True:
        try:
            rec = fetch_record()
            seen = (rec["coObservedAt"], rec["weatherObservedAt"])
            if seen != last:
                meta = producer.send(a.topic, key=STATION, value=rec).get(timeout=15)
                last = seen
                print(f"[{datetime.now():%H:%M:%S}] GỬI offset={meta.offset} quan sát CO {seen[0]}, thời tiết {seen[1]} | "
                      f"CO={rec['co']} µg/m³ gió={rec['wind_speed_kmh']} km/h nhiệt độ={rec['temperature_C']}°C", flush=True)
            else:
                print(f"[{datetime.now():%H:%M:%S}] chưa có quan sát mới (CO {seen[0]}, thời tiết {seen[1]})", flush=True)
        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"[{datetime.now():%H:%M:%S}] bỏ qua lần này, lỗi lấy dữ liệu: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
        except KafkaError as e:
            print(f"[{datetime.now():%H:%M:%S}] lỗi Kafka: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
            if a.once:
                return 1
        if a.once:
            return 0 if last else 1
        time.sleep(a.interval)


if __name__ == "__main__":
    sys.exit(main())
