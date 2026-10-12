"""Phát lại có giới hạn các dòng cuối của bộ Kaggle vào HDFS /stream_input (một file JSON mỗi bản ghi).

Đây là MÔ PHỎNG (source = replay-dataset, nhãn trạm luân phiên, datetime = giờ máy lúc phát), không phải đo thật.
Cùng định dạng với sensor_producer_replay_dataset.py ở thư mục cha, nhưng:
  - có giới hạn số bản ghi (--count) để /stream_input không phình mãi,
  - tên file có tiền tố riêng (replay_<epoch>_<n>.json), không đè file của producer cũ,
  - dùng đúng đường dẫn bộ dữ liệu trong Data/.

Chạy (trong WSL, HDFS đang chạy):
  python3 infra/replay_producer.py --count 300 --interval 5
Dọn lại: hdfs dfs -rm '/stream_input/replay_*.json'
"""
import argparse
import csv
import json
import os
import subprocess
import sys
import time
from datetime import datetime

DEFAULT_CSV = "/mnt/f/HK1_Nam04/BigData/Data/hcmc_aqi_dataset_after_preprocess_final.csv"
HDFS = os.environ.get("HDFS_BIN", "/home/trong/hadoop/bin/hdfs")
STATIONS = ["Tram_Quan_1", "Tram_Binh_Thanh", "Tram_Thu_Duc", "Tram_Quan_5"]


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--count", type=int, default=300, help="số bản ghi cần phát (mặc định 300)")
    p.add_argument("--interval", type=float, default=5.0, help="giây giữa hai bản ghi (mặc định 5)")
    p.add_argument("--tail", type=int, default=48, help="lấy bấy nhiêu dòng cuối bộ dữ liệu rồi lặp vòng (mặc định 48)")
    p.add_argument("--csv", default=DEFAULT_CSV, help="đường dẫn bộ dữ liệu Kaggle")
    p.add_argument("--dest", default="/stream_input", help="thư mục HDFS đích")
    a = p.parse_args()

    with open(a.csv, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))[-a.tail:]
    if not rows:
        print("Bộ dữ liệu rỗng", file=sys.stderr)
        return 1

    run = int(time.time())
    print(f"[replay_producer] MÔ PHỎNG: {a.count} bản ghi, mỗi {a.interval}s, vòng lặp {len(rows)} dòng cuối bộ Kaggle -> {a.dest}", flush=True)
    for n in range(1, a.count + 1):
        row = rows[(n - 1) % len(rows)]
        rec = {
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "station": STATIONS[n % len(STATIONS)],
            "co": float(row["co"]),
            "wind_speed_kmh": float(row["wind_speed_kmh"]),
            "temperature_C": float(row["temperature_C"]),
        }
        local = f"/tmp/replay_{run}_{n}.json"
        with open(local, "w") as g:
            g.write(json.dumps(rec) + "\n")
        r = subprocess.run([HDFS, "dfs", "-put", "-f", local, f"{a.dest}/replay_{run}_{n}.json"],
                           capture_output=True, text=True)
        os.remove(local)
        print(f"[{rec['datetime']}] {n}/{a.count} {rec['station']:16} CO={rec['co']:7.2f} rc={r.returncode} {r.stderr.strip()[:100]}", flush=True)
        if r.returncode != 0:
            print("[replay_producer] ghi HDFS lỗi, dừng", file=sys.stderr)
            return r.returncode
        time.sleep(a.interval)
    print("[replay_producer] xong", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
