"""Phần dùng chung của pipeline CO (bám 9 bước của giảng viên, đổi cho CO). Chạy trong WSL cùng Hadoop và Spark `local[*]`.

- Đường dẫn HDFS của dự án: /co/raw (dữ liệu gốc), /co/processed (đã làm sạch), /co/output (kết quả và step_summary.json).
- `step_summary.json` ở /co/output: mỗi bước pipeline ghi trạng thái và số liệu thật của chính nó; backend chỉ ĐỌC file này để trả
  `GET /api/pipeline/steps`. Không bước nào được ghi số liệu mà nó chưa tính.
- Không có thư viện ngoài (chỉ chuẩn Python + PySpark); đọc/ghi HDFS bằng lệnh `hdfs dfs` để không phụ thuộc phiên Spark.
"""
import json
import os
import subprocess
import sys
import traceback
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

HDFS_URI = os.environ.get("HDFS_URI", "hdfs://localhost:9000")
HDFS_BIN = os.environ.get("HDFS_BIN", os.path.expanduser("~/hadoop/bin/hdfs"))
RAW_DIR, PROCESSED_DIR, OUTPUT_DIR = "/co/raw", "/co/processed", "/co/output"
RAW_FILE = f"{RAW_DIR}/Dataset.csv"
CLEAN_DIR = f"{PROCESSED_DIR}/aqi_clean"
SUMMARY_PATH = f"{OUTPUT_DIR}/step_summary.json"
VN = timezone(timedelta(hours=7))

# Khớp enum `PipelineStep.id` trong docs/contract/openapi.yaml
STEP_ORDER = ["prepare-hdfs", "upload-raw", "read-spark", "clean", "spark-sql", "cluster-and-model", "save-results", "kafka-ingest", "airflow-dag"]

# Cột của bộ Kaggle theo thứ tự trong file (data_dictionary.csv). Cột `humidity_%` đổi thành humidity_pct khi làm sạch.
RAW_COLUMNS = ["datetime", "aqi_label", "co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3", "temperature_C", "humidity_%", "rain_mm",
               "wind_speed_kmh", "is_event_anomaly", "hour", "month", "dayofweek", "o3_was_missing"]
GASES = ["co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3"]
# Miền hợp lệ theo data_dictionary.csv: (thấp nhất, cao nhất); None = không giới hạn phía đó
DOMAIN = {"aqi_label": (1, 5), **{g: (0, None) for g in GASES}, "humidity_%": (0, 100), "rain_mm": (0, None), "wind_speed_kmh": (0, None),
          "is_event_anomaly": (0, 1), "o3_was_missing": (0, 1), "hour": (0, 23), "month": (1, 12), "dayofweek": (0, 6)}
DATETIME_FORMAT = "dd-MM-yy H:mm"  # file thật, KHÔNG phải M/D/Y như data_dictionary.csv ghi; giờ gốc là UTC
BREAK_VN = "2025-07-01 00:00:00"   # điểm gãy dữ liệu (CO giảm mạnh), giờ Việt Nam


def now_iso() -> str:
    return datetime.now(VN).isoformat(timespec="seconds")


def hdfs(*args: str, input_text: str | None = None, check: bool = True) -> subprocess.CompletedProcess:
    r = subprocess.run([HDFS_BIN, "dfs", *args], input=input_text, capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"hdfs dfs {' '.join(args)} lỗi (mã {r.returncode}): {r.stderr.strip()[-300:]}")
    return r


def hdfs_exists(path: str) -> bool:
    return hdfs("-test", "-e", path, check=False).returncode == 0


def hdfs_write_text(path: str, text: str) -> None:
    hdfs("-put", "-f", "-", path, input_text=text)


def hdfs_read_text(path: str) -> str:
    return hdfs("-cat", path).stdout


def read_summary() -> dict:
    if not hdfs_exists(SUMMARY_PATH):
        return {"version": 1, "steps": {}}
    return json.loads(hdfs_read_text(SUMMARY_PATH))


def update_step(step_id: str, state: str, summary: list[dict] | None = None, detail: str | None = None) -> None:
    """Ghi trạng thái một bước vào step_summary.json (đọc, sửa đúng bước đó, ghi lại). summary: [{"label", "value", "unit"}]."""
    assert step_id in STEP_ORDER and state in ("running", "done", "failed"), (step_id, state)
    doc = read_summary()
    doc["steps"][step_id] = {"state": state, "updatedAt": now_iso(), "summary": summary or [], "detail": detail}
    hdfs_write_text(SUMMARY_PATH, json.dumps(doc, ensure_ascii=False, indent=2) + "\n")


def item(label: str, value, unit: str | None = None) -> dict:
    return {"label": label, "value": value, "unit": unit}


@contextmanager
def step(step_id: str):
    """Đánh dấu running, rồi done khi `yield` trả về (summary, detail) hoặc failed (kèm thông báo lỗi) khi có ngoại lệ. Dùng:

        with step("clean") as result:
            ...
            result.summary, result.detail = [...], "..."
    """
    class Result:
        summary: list[dict] = []
        detail: str | None = None
    res = Result()
    update_step(step_id, "running")
    try:
        yield res
    except BaseException as e:  # kể cả SystemExit/KeyboardInterrupt: không để bước kẹt ở "running"
        update_step(step_id, "failed", detail=f"{type(e).__name__}: {str(e)[:400]}")
        if not isinstance(e, Exception):
            raise
        traceback.print_exc()
        sys.exit(1)
    update_step(step_id, "done", res.summary, res.detail)


def spark_session(app_name: str):
    """SparkSession local[*], múi giờ phiên UTC (để datetime_vn = datetime_utc + 7 giờ hiển thị đúng đồng hồ VN, như view `aqi` của /api/sql)."""
    os.environ.setdefault("HADOOP_CONF_DIR", os.path.expanduser("~/hadoop/etc/hadoop"))
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName(app_name).master("local[*]")
             .config("spark.sql.shuffle.partitions", "4").config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
