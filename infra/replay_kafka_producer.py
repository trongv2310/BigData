"""Producer PHÁT LẠI (MÔ PHỎNG): phát từng dòng giờ của bộ lịch sử Kaggle vào Kafka (topic hcmc_co_stream), có thể tạm dừng và đổi tốc độ.

Đây là luồng "bên trái" của dashboard, không phải đo lúc này:
  - `datetime` = thời điểm PHÁT (giờ máy, +07); `originalDatetime` = thời điểm gốc của dòng lịch sử (cột datetime của Kaggle là UTC, đã cộng 7 giờ).
    Không giả dấu thời gian: dữ liệu lịch sử không được trình bày như thể đo lúc này.
  - `provider` = "replay-dataset"; nhãn trạm gán luân phiên (isSimulatedStation luôn true, giữ quy ước D2).
  - Mỗi dòng cách nhau --base-interval giây ở tốc độ 1; tốc độ 10 và 60 chia khoảng này (60 với 10 giây/dòng = 6 dòng/giây, dưới giới hạn
    20 tin/giây của /ws/stream). Hết dữ liệu thì quay lại dòng đầu (vòng lặp); mặc định bắt đầu từ 01/07/2025 00:00 (+07), tức giai đoạn sau điểm gãy.
  - Điều khiển bằng tệp cục bộ trong --state-dir (mặc định /tmp/hcmc_replay): `control.json` ({"paused": bool, "speed": 1|10|60}) do backend ghi,
    `status.json` do producer ghi mỗi 2 giây (nhịp tim) để backend đọc. Không có tệp nhịp tim mới trong 10 giây thì backend coi là "stopped".
    Luồng DỮ LIỆU THẬT (live_producer_owm.py) không đọc các tệp này nên không bao giờ bị ảnh hưởng.

Chạy (WSL, Kafka đang chạy):  python3 infra/replay_kafka_producer.py [--base-interval 10] [--start 2025-07-01T00:00:00+07:00]
"""
import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from kafka import KafkaProducer
except ImportError:  # pragma: no cover
    KafkaProducer = None

DEFAULT_CSV = "/mnt/f/HK1_Nam04/BigData/Data/hcmc_aqi_dataset_after_preprocess_final.csv"
VN = timezone(timedelta(hours=7))
STATIONS = ["Tram_Quan_1", "Tram_Binh_Thanh", "Tram_Thu_Duc", "Tram_Quan_5"]
SPEEDS = (1, 10, 60)
POLLUTANTS = ["no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3"]
HEARTBEAT_SECONDS = 2.0
TICK_SECONDS = 0.1


def parse_original(text: str) -> datetime:
    """Cột datetime của Kaggle (UTC), ví dụ '01-04-22 0:00' -> giờ Việt Nam."""
    return datetime.strptime(text.strip(), "%d-%m-%y %H:%M").replace(tzinfo=timezone.utc).astimezone(VN)


def load_rows(path: str, start: datetime) -> list[dict]:
    """Các dòng từ `start` trở đi (theo giờ +07). Không có dòng nào thì ném ValueError."""
    rows = []
    with open(path, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            t = parse_original(r["datetime"])
            if t >= start:
                rows.append({"orig": t, "co": float(r["co"]), "wind": float(r["wind_speed_kmh"]), "temp": float(r["temperature_C"]),
                             **{p: float(r[p]) if r.get(p) not in (None, "") else None for p in POLLUTANTS}})
    if not rows:
        raise ValueError(f"bộ dữ liệu không có dòng nào từ {start.isoformat()}")
    return rows


def make_message(row: dict, n: int, now: datetime) -> dict:
    msg = {
        "datetime": now.strftime("%Y-%m-%d %H:%M:%S"),
        "originalDatetime": row["orig"].strftime("%Y-%m-%d %H:%M:%S"),
        "station": STATIONS[n % len(STATIONS)],
        "co": row["co"],
        "wind_speed_kmh": row["wind"],
        "temperature_C": row["temp"],
        "provider": "replay-dataset",
    }
    for p in POLLUTANTS:
        msg[p] = row[p]
    return msg


def read_control(state_dir: Path, current: dict) -> dict:
    """Đọc control.json; hỏng hoặc sai giá trị thì giữ trạng thái hiện tại (không nhận lệnh nửa vời)."""
    try:
        c = json.loads((state_dir / "control.json").read_text(encoding="utf-8"))
        paused, speed = c["paused"], c["speed"]
        if isinstance(paused, bool) and speed in SPEEDS and not isinstance(speed, bool):
            return {"paused": paused, "speed": speed}
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return current


def write_status(state_dir: Path, state: dict, base_interval: float, emitted: int, last_orig: datetime | None) -> None:
    speed = state["speed"]
    status = {
        "state": "paused" if state["paused"] else "running",
        "speed": speed,
        "baseIntervalSeconds": base_interval,
        "intervalSeconds": base_interval / speed,
        "emittedCount": emitted,
        "lastOriginalDatetime": last_orig.isoformat(timespec="seconds") if last_orig else None,
        "updatedAt": datetime.now(VN).isoformat(timespec="seconds"),
        "pid": os.getpid(),
    }
    tmp = state_dir / "status.json.tmp"
    tmp.write_text(json.dumps(status, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, state_dir / "status.json")  # ghi nguyên tử để backend không đọc phải nửa tệp


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--csv", default=DEFAULT_CSV)
    p.add_argument("--start", default="2025-07-01T00:00:00+07:00", help="phát từ dòng có thời điểm gốc (+07) không sớm hơn mốc này")
    p.add_argument("--base-interval", type=float, default=10.0, help="giây giữa hai bản ghi ở tốc độ 1 (mặc định 10)")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="hcmc_co_stream")
    p.add_argument("--state-dir", default=os.environ.get("REPLAY_STATE_DIR", "/tmp/hcmc_replay"))
    p.add_argument("--count", type=int, default=0, help="dừng sau ngần này bản ghi (0 = chạy mãi)")
    a = p.parse_args()

    state_dir = Path(a.state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    rows = load_rows(a.csv, datetime.fromisoformat(a.start))
    producer = KafkaProducer(bootstrap_servers=a.bootstrap, acks="all", retries=5,
                             key_serializer=lambda k: k.encode("utf-8"),
                             value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"))
    state = read_control(state_dir, {"paused": False, "speed": 1})
    print(f"[replay_kafka] MÔ PHỎNG: phát lại {len(rows)} dòng lịch sử từ {rows[0]['orig']:%Y-%m-%d %H:%M} (+07), mỗi {a.base_interval:g}s ở tốc độ 1 -> "
          f"Kafka {a.bootstrap} topic {a.topic}; điều khiển qua {state_dir}", flush=True)

    emitted, last_orig = 0, None
    next_send = time.monotonic()
    next_beat = 0.0
    while a.count == 0 or emitted < a.count:
        new = read_control(state_dir, state)
        if new != state:
            print(f"[replay_kafka] điều khiển: {'tạm dừng' if new['paused'] else 'chạy'}, tốc độ {new['speed']}x", flush=True)
            if state["paused"] and not new["paused"]:
                next_send = time.monotonic()  # vừa tiếp tục: phát ngay, không bù các lượt đã nhỡ
            state = new
            write_status(state_dir, state, a.base_interval, emitted, last_orig)  # xác nhận ngay để backend thấy lệnh đã nhận
        now_m = time.monotonic()
        if not state["paused"] and now_m >= next_send:
            row = rows[emitted % len(rows)]
            msg = make_message(row, emitted + 1, datetime.now(VN))
            meta = producer.send(a.topic, key="replay", value=msg).get(timeout=15)
            emitted, last_orig = emitted + 1, row["orig"]
            next_send = now_m + a.base_interval / state["speed"]
            if emitted % 20 == 1:
                print(f"[replay_kafka] {emitted} bản ghi; offset={meta.offset} gốc {row['orig']:%Y-%m-%d %H:%M} CO={row['co']}", flush=True)
            write_status(state_dir, state, a.base_interval, emitted, last_orig)
            next_beat = now_m + HEARTBEAT_SECONDS
        elif now_m >= next_beat:
            write_status(state_dir, state, a.base_interval, emitted, last_orig)
            next_beat = now_m + HEARTBEAT_SECONDS
        time.sleep(TICK_SECONDS if state["paused"] else max(0.0, min(TICK_SECONDS, next_send - time.monotonic())))  # tốc độ 60 cần nhịp dưới 0,1 s
    producer.flush()
    print("[replay_kafka] xong", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
