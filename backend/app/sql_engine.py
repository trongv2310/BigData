"""SparkSQL chỉ đọc cho /api/sql. SparkSession (local[2]) được tạo ở lần gọi đầu tiên và nằm trong tiến trình backend.

Bảng duy nhất là view `aqi`, dựng từ HDFS /raw/Dataset.csv (cùng nội dung bộ Kaggle):
  datetime_utc   timestamp  giờ gốc của file (UTC)
  datetime_vn    timestamp  datetime_utc + 7 giờ; hiển thị đúng đồng hồ Việt Nam (Spark phiên làm việc đặt UTC nên không đổi thêm)
  hour_vn, month_vn, year_vn   tách từ datetime_vn
  aqi_label, co, no, no2, o3, so2, pm2_5, pm10, nh3, temperature_C, humidity_pct, rain_mm, wind_speed_kmh, is_event_anomaly, o3_was_missing
Cố ý bỏ các cột hour, month, dayofweek của file vì chúng tính theo UTC, dễ bị nhầm với giờ địa phương.
"""
import math
import threading
import time
from datetime import date, datetime
from decimal import Decimal

from . import config, sql_guard

_lock = threading.Lock()
_spark = None


class SqlUnavailable(RuntimeError):
    """Không dựng được Spark hoặc không đọc được dữ liệu (HDFS tắt...)."""


class SqlError(ValueError):
    """Câu lệnh sai (cú pháp, tên cột...) hoặc chạy lỗi."""


class SqlTimeout(RuntimeError):
    pass


def _first_line(e: Exception) -> str:
    msg = getattr(e, "desc", None) or str(e)
    return msg.strip().splitlines()[0][:400] if msg.strip() else type(e).__name__


def _session():
    global _spark
    if _spark is not None:
        return _spark
    try:
        from pyspark.sql import SparkSession, functions as F

        spark = (SparkSession.builder.appName("hcmc_co_backend_sql").master("local[2]")
                 .config("spark.ui.enabled", "false")  # không chiếm cổng 4040, không lẫn với ứng dụng streaming khi /api/services dò Spark
                 .config("spark.sql.runSQLOnFiles", "false")  # chặn csv.`/etc/...`
                 .config("spark.sql.session.timeZone", "UTC")
                 .config("spark.sql.shuffle.partitions", "4")
                 .config("spark.driver.memory", "768m")
                 .getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        raw = spark.read.option("header", True).option("inferSchema", True).csv(config.SQL_DATASET_URI)
        utc = F.to_timestamp("datetime", "dd-MM-yy H:mm")
        vn = utc + F.expr("INTERVAL 7 HOURS")
        keep = ["aqi_label", "co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3", "temperature_C"]
        df = raw.select(utc.alias("datetime_utc"), vn.alias("datetime_vn"), F.hour(vn).alias("hour_vn"), F.month(vn).alias("month_vn"),
                        F.year(vn).alias("year_vn"), *keep, F.col("`humidity_%`").alias("humidity_pct"),
                        "rain_mm", "wind_speed_kmh", "is_event_anomaly", "o3_was_missing").cache()
        df.count()  # đọc thật một lần: lỗi đường dẫn/HDFS lộ ra ngay ở đây
        df.createOrReplaceTempView("aqi")
    except Exception as e:  # JVM, HDFS, thiếu pyspark...
        raise SqlUnavailable(f"Không dựng được SparkSQL: {_first_line(e)}") from e
    _spark = spark
    return spark


def _json_value(v):
    if v is None or isinstance(v, (bool, str, int)):
        return v
    if isinstance(v, float):
        return v if math.isfinite(v) else None  # NaN/Infinity không hợp lệ trong JSON
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return str(v)  # mảng, map, struct, bytes: dạng chữ


def run(query: str, limit: int) -> dict:
    q = sql_guard.check(query)  # ném SqlNotAllowed
    with _lock:  # một truy vấn tại một thời điểm
        from pyspark.sql import functions as F
        from pyspark.sql.types import TimestampType

        t0 = time.monotonic()
        spark = _session()
        gid = f"api-sql-{int(t0 * 1000)}"
        sc = spark.sparkContext
        sc.setJobGroup(gid, "api/sql", interruptOnCancel=True)
        timed_out = threading.Event()

        def cancel():
            timed_out.set()
            sc.cancelJobGroup(gid)

        timer = threading.Timer(config.SQL_TIMEOUT_SECONDS, cancel)
        timer.start()
        try:
            df = spark.sql(q)
            fields = df.schema.fields
            cols = [{"name": f.name, "type": f.dataType.simpleString()} for f in fields]
            safe = df.toDF(*[f"_c{i}" for i in range(len(fields))])  # tránh trùng tên cột
            safe = safe.select(*[F.date_format(F.col(f"_c{i}"), "yyyy-MM-dd HH:mm:ss") if isinstance(f.dataType, TimestampType) else F.col(f"_c{i}")
                                 for i, f in enumerate(fields)])  # chuỗi giờ UTC của phiên, không qua múi giờ của Python
            rows = safe.limit(limit + 1).collect()
        except Exception as e:
            if timed_out.is_set():
                raise SqlTimeout(f"Truy vấn chạy quá {config.SQL_TIMEOUT_SECONDS} giây nên bị hủy") from e
            raise SqlError(_first_line(e)) from e
        finally:
            timer.cancel()
        truncated = len(rows) > limit
        out = [[_json_value(v) for v in r] for r in rows[:limit]]
        return {"columns": cols, "rows": out, "rowCount": len(out), "truncated": truncated,
                "elapsedMs": int((time.monotonic() - t0) * 1000)}


def stop() -> None:
    global _spark
    with _lock:
        if _spark is not None:
            _spark.stop()
            _spark = None
