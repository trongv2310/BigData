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
HDFS_STREAM_DIR = _env("HDFS_STREAM_DIR", "/stream_input")
HDFS_DATASET_PATH = _env("HDFS_RAW_PATH", "/raw/Dataset.csv")

# Bộ Kaggle (D4). Ưu tiên file cục bộ; không có thì đọc /raw/Dataset.csv trên HDFS qua WebHDFS (cùng 3 179 004 byte).
AQI_CSV_PATH = _env("AQI_CSV_PATH", "/mnt/f/HK1_Nam04/BigData/Data/hcmc_aqi_dataset_after_preprocess_final.csv")

# Thời gian chờ khi kiểm tra từng dịch vụ (giây)
PROBE_TIMEOUT = float(_env("PROBE_TIMEOUT", "1.5"))

# /ws/stream: "live" = chỉ phát cái thật (bản ghi mới trong /stream_input, đổi trạng thái dịch vụ, kafka);
#             "replay" = phát lại mock/stream-events.ndjson (GIẢ LẬP, chỉ để dev giao diện).
STREAM_MODE = _env("STREAM_MODE", "live")
STREAM_REPLAY_FILE = Path(_env("STREAM_REPLAY_FILE", str(REPO_ROOT / "mock" / "stream-events.ndjson")))
STREAM_MAX_EVENTS_PER_SECOND = 20  # events.md

# /api/sql: SparkSQL đọc bộ dữ liệu từ HDFS
SQL_DATASET_URI = _env("SQL_DATASET_URI", f"hdfs://localhost:9000{HDFS_DATASET_PATH}")
SQL_TIMEOUT_SECONDS = int(_env("SQL_TIMEOUT_SECONDS", "60"))

# Cửa sổ lệnh /ws/terminal (events.md mục 2): chạy lệnh thật, nên tắt cho đến khi BACKEND_ALLOW_WRITES=1
TERMINAL_ORIGINS = _env("ALLOWED_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173").split(",")
TERMINAL_IDLE_SECONDS = int(_env("TERMINAL_IDLE_SECONDS", "1800"))

# Log của NameNode (backend chỉ đọc) để phát hdfs.op / flow.step
HADOOP_LOG_DIR = _env("HADOOP_LOG_DIR", "/home/trong/hadoop/logs")

# Ghi lên HDFS/Kafka/Spark: mặc định TẮT (xem app/writeguard.py)
ALLOW_WRITES = _flag("BACKEND_ALLOW_WRITES")

# CORS: chỉ frontend dev
CORS_ORIGINS = _env("ALLOWED_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173").split(",")
