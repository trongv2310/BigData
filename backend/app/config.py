"""Cấu hình backend, đọc từ biến môi trường (xem .env.example)."""
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _flag(name: str) -> bool:
    return _env(name, "").strip().lower() in ("1", "true", "yes")


# Địa chỉ dịch vụ (backend chạy trong WSL cùng Hadoop/Kafka/Spark)
NAMENODE_HTTP = _env("HDFS_NAMENODE_HTTP", "http://localhost:9870")
DATANODE_HTTP_PORTS = [int(p) for p in _env("DATANODE_HTTP_PORTS", "9864,9874,9884,9894").split(",")]
YARN_HTTP = _env("YARN_RM", "http://localhost:8088")
SPARK_UI_PORTS = [int(p) for p in _env("SPARK_UI_PORTS", "4040,4041,4042,4043").split(",")]
AIRFLOW_HTTP = _env("AIRFLOW_UI", "http://localhost:8080")
KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = _env("KAFKA_TOPIC", "hcmc_co_stream")
# Thư mục checkpoint (trên HDFS) của các truy vấn Spark đọc Kafka: Spark không dùng consumer group nên độ trễ lấy từ offset đã commit ở đây
SPARK_KAFKA_CHECKPOINTS = [p for p in _env("SPARK_KAFKA_CHECKPOINTS", "/live/_checkpoints/co_stream").split(",") if p.strip()]
HDFS_STREAM_DIR = _env("HDFS_STREAM_DIR", "/stream_input")
HDFS_DATASET_PATH = _env("HDFS_RAW_PATH", "/raw/Dataset.csv")

# Bộ Kaggle (D4). Ưu tiên file cục bộ; không có thì đọc /raw/Dataset.csv trên HDFS qua WebHDFS (cùng 3 179 004 byte).
AQI_CSV_PATH = _env("AQI_CSV_PATH", "/mnt/f/HK1_Nam04/BigData/Data/hcmc_aqi_dataset_after_preprocess_final.csv")

# Thời gian chờ khi kiểm tra từng dịch vụ (giây)
PROBE_TIMEOUT = float(_env("PROBE_TIMEOUT", "1.5"))

# Nguồn của luồng `reading` và /api/aqi/latest (quyết định D1): "hdfs" = file JSON trong HDFS /stream_input (mặc định, như trước);
# "kafka" = tin trong topic KAFKA_TOPIC (producer thật: infra/live_producer.py); "both" = cả hai.
STREAM_SOURCE = _env("STREAM_SOURCE", "hdfs").strip().lower()
if STREAM_SOURCE not in ("hdfs", "kafka", "both"):
    raise SystemExit(f"STREAM_SOURCE phải là hdfs, kafka hoặc both (nhận: {STREAM_SOURCE!r})")

# /ws/stream: "live" = chỉ phát cái thật (bản ghi mới trong /stream_input, đổi trạng thái dịch vụ, kafka);
#             "replay" = phát lại mock/stream-events.ndjson (GIẢ LẬP, chỉ để dev giao diện).
STREAM_MODE = _env("STREAM_MODE", "live")
STREAM_REPLAY_FILE = Path(_env("STREAM_REPLAY_FILE", str(REPO_ROOT / "mock" / "stream-events.ndjson")))
STREAM_MAX_EVENTS_PER_SECOND = 20  # events.md

# /api/sql: SparkSQL đọc bộ dữ liệu từ HDFS
SQL_DATASET_URI = _env("SQL_DATASET_URI", f"hdfs://localhost:9000{HDFS_DATASET_PATH}")
SQL_TIMEOUT_SECONDS = int(_env("SQL_TIMEOUT_SECONDS", "60"))
# Dữ liệu sống Spark ghi từ Kafka (infra/spark_kafka_consumer.py), thành view `live` của /api/sql
SQL_LIVE_URI = _env("SQL_LIVE_URI", "hdfs://localhost:9000/live/co_stream")

# Cửa sổ lệnh /ws/terminal (events.md mục 2): chạy lệnh thật, nên tắt cho đến khi BACKEND_ALLOW_WRITES=1
TERMINAL_ORIGINS = _env("ALLOWED_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173").split(",")
TERMINAL_IDLE_SECONDS = int(_env("TERMINAL_IDLE_SECONDS", "1800"))

# Log của NameNode (backend chỉ đọc) để phát hdfs.op / flow.step
HADOOP_LOG_DIR = _env("HADOOP_LOG_DIR", "/home/trong/hadoop/logs")

# Luồng kafka-produce và spark-streaming (định nghĩa ở /api/flows và sự kiện flow.step). Mặc định TẮT vì chỉ có ý nghĩa khi
# STREAM_SOURCE=kafka (frontend đã lọc flow.step theo flowId từ PR #11).
FLOWS_EXTRA = _flag("FLOWS_EXTRA")

# Ngưỡng CO (µg/m³) cho `status` (quyết định D3, chốt 13/10/2026): thang CO của OpenWeather Air Pollution API, cũng là nguồn của bộ
# dữ liệu lịch sử. Good < 4400, Fair 4400..9400, Moderate 9400..12400, Poor 12400..15400, Very Poor >= 15400. Hợp đồng chỉ có ba mức:
# SAFE = Good; WARNING = Fair và Moderate (từ 4400, cũng xấp xỉ hướng dẫn 24 giờ 4 mg/m³ của WHO 2021); DANGER = Poor và Very Poor.
CO_WARNING_UG = float(_env("CO_WARNING_UG", "4400"))
CO_DANGER_UG = float(_env("CO_DANGER_UG", "12400"))
if not 0 < CO_WARNING_UG < CO_DANGER_UG:
    raise SystemExit(f"CO_WARNING_UG phải dương và nhỏ hơn CO_DANGER_UG (nhận: {CO_WARNING_UG}, {CO_DANGER_UG})")

# Trạng thái 9 bước pipeline do các script trong pipeline/ ghi (backend chỉ đọc), xem app/pipeline_steps.py
PIPELINE_SUMMARY_PATH = _env("PIPELINE_SUMMARY_PATH", "/co/output/step_summary.json")

# Luồng PHÁT LẠI (infra/replay_kafka_producer.py): backend đọc trạng thái và ghi lệnh qua tệp cục bộ trong thư mục này (không qua HDFS/Kafka/Spark).
# REPLAY_CONTROL=0 để backend không điều khiển được (chỉ xem trạng thái). Luồng dữ liệu thật không bao giờ bị điều khiển.
REPLAY_STATE_DIR = _env("REPLAY_STATE_DIR", "/tmp/hcmc_replay")
REPLAY_CONTROL = _env("REPLAY_CONTROL", "1").strip().lower() in ("1", "true", "yes")
REPLAY_BASE_INTERVAL = float(_env("REPLAY_BASE_INTERVAL", "10"))  # giây giữa hai bản ghi ở tốc độ 1; dùng khi producer chưa chạy

# Ghi lên HDFS/Kafka/Spark: mặc định TẮT (xem app/writeguard.py)
ALLOW_WRITES = _flag("BACKEND_ALLOW_WRITES")

# CORS: chỉ frontend dev
CORS_ORIGINS = _env("ALLOWED_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173").split(",")
