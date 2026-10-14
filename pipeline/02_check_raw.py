"""Bước 3 (đọc bằng Spark): kiểm dữ liệu gốc /co/raw/Dataset.csv, KHÔNG sửa gì.

  spark-submit --driver-memory 1g pipeline/02_check_raw.py

Đọc toàn bộ cột dưới dạng chuỗi (không tự suy kiểu để không che giá trị lạ), rồi đếm:
  - schema: số dòng, số cột, tên cột có đúng như data_dictionary.csv;
  - thiếu: ô null hoặc rỗng theo từng cột;
  - giá trị lạ: không phải số, hoặc ngoài miền của data_dictionary.csv (ví dụ CO âm, độ ẩm > 100, nhãn AQI ngoài 1..5);
  - thời gian: datetime không đọc được, trùng, và các giờ bị thiếu giữa hai đầu (dữ liệu phải liên tục theo giờ);
  - các cột hour/month/dayofweek của file tính theo giờ nào (UTC hay +07) bằng cách so với datetime.
Báo cáo đầy đủ ghi vào /co/output/check_raw.json; số liệu chính ghi vào step_summary.json (bước read-spark). Mọi số đều do Spark tính ở lần chạy này.
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as c  # noqa: E402


def main() -> int:
    from pyspark.sql import functions as F

    with c.step("read-spark") as r:
        spark = c.spark_session("HCMC_CO_02_check_raw")
        df = spark.read.option("header", True).option("inferSchema", False).csv(f"{c.HDFS_URI}{c.RAW_FILE}")
        cols = df.columns
        if cols != c.RAW_COLUMNS:
            raise RuntimeError(f"cột khác kỳ vọng: {cols}")
        df = df.cache()
        rows = df.count()

        blank = lambda col: F.col(f"`{col}`").isNull() | (F.trim(F.col(f"`{col}`")) == "")  # noqa: E731
        num = lambda col: F.col(f"`{col}`").cast("double")  # noqa: E731
        aggs = []
        for col in cols:
            aggs.append(F.count(F.when(blank(col), 1)).alias(f"{col}|blank"))
            if col != "datetime":
                aggs.append(F.count(F.when(~blank(col) & num(col).isNull(), 1)).alias(f"{col}|nonnumeric"))
                lo, hi = c.DOMAIN.get(col, (None, None))
                cond = F.lit(False)
                if lo is not None:
                    cond = cond | (num(col) < lo)
                if hi is not None:
                    cond = cond | (num(col) > hi)
                aggs.append(F.count(F.when(num(col).isNotNull() & cond, 1)).alias(f"{col}|outofdomain"))
        agg = df.agg(*aggs).first().asDict()

        ts = F.to_timestamp(F.col("datetime"), c.DATETIME_FORMAT)
        t = df.select(ts.alias("t"), F.col("hour").cast("int").alias("h"), F.col("month").cast("int").alias("m"), F.col("dayofweek").cast("int").alias("d"))
        tstats = t.agg(F.count(F.when(F.col("t").isNull(), 1)).alias("unparsable"), F.countDistinct("t").alias("distinct"),
                       F.date_format(F.min("t"), "yyyy-MM-dd HH:mm").alias("min"), F.date_format(F.max("t"), "yyyy-MM-dd HH:mm").alias("max"),
                       F.min(F.unix_timestamp("t")).alias("min_s"), F.max(F.unix_timestamp("t")).alias("max_s"),
                       F.count(F.when(F.col("h") != F.hour("t"), 1)).alias("hour_not_utc"),
                       F.count(F.when(F.col("h") != F.hour(F.expr("t + interval 7 hours")), 1)).alias("hour_not_vn"),
                       F.count(F.when(F.col("m") != F.month("t"), 1)).alias("month_not_utc"),
                       F.count(F.when(F.col("m") != F.month(F.expr("t + interval 7 hours")), 1)).alias("month_not_vn"),
                       F.count(F.when(F.col("d") != (F.dayofweek("t") + 5) % 7, 1)).alias("dow_not_utc"),
                       F.count(F.when(F.col("d") != (F.dayofweek(F.expr("t + interval 7 hours")) + 5) % 7, 1)).alias("dow_not_vn")).first().asDict()
        # Chuỗi/giây do Spark tính trong múi giờ phiên UTC (thu timestamp về Python sẽ bị đổi sang giờ máy +07)
        t_min, t_max = tstats["min"], tstats["max"]
        expected_hours = (tstats["max_s"] - tstats["min_s"]) // 3600 + 1
        duplicates = rows - tstats["unparsable"] - tstats["distinct"]
        missing_hours = expected_hours - tstats["distinct"]

        per_col = {col: {"blank": agg[f"{col}|blank"], "nonNumeric": agg.get(f"{col}|nonnumeric", 0), "outOfDomain": agg.get(f"{col}|outofdomain", 0)} for col in cols}
        total = lambda k: sum(v[k] for v in per_col.values())  # noqa: E731
        anomalies = df.agg(F.count(F.when(F.col("is_event_anomaly") == "1", 1)).alias("event"), F.count(F.when(F.col("o3_was_missing") == "1", 1)).alias("o3_filled")).first().asDict()
        report = {"generatedAt": c.now_iso(), "file": c.RAW_FILE, "rows": rows, "columns": len(cols), "perColumn": per_col,
                  "datetime": {"format": c.DATETIME_FORMAT, "unparsable": tstats["unparsable"], "duplicates": duplicates, "minUtc": str(t_min), "maxUtc": str(t_max),
                               "expectedHourlyRows": expected_hours, "missingHours": missing_hours},
                  "derivedColumnsInFile": {"hourNotMatchingUtc": tstats["hour_not_utc"], "hourNotMatchingVn": tstats["hour_not_vn"],
                                           "monthNotMatchingUtc": tstats["month_not_utc"], "monthNotMatchingVn": tstats["month_not_vn"],
                                           "dayofweekNotMatchingUtc": tstats["dow_not_utc"], "dayofweekNotMatchingVn": tstats["dow_not_vn"]},
                  "flags": {"isEventAnomalyRows": anomalies["event"], "o3WasMissingRows": anomalies["o3_filled"]}}
        c.hdfs_write_text(f"{c.OUTPUT_DIR}/check_raw.json", json.dumps(report, ensure_ascii=False, indent=2) + "\n")

        bad = {k: v for k, v in per_col.items() if v["blank"] or v["nonNumeric"] or v["outOfDomain"]}
        r.summary = [c.item("Số dòng", rows, "dòng"), c.item("Số cột", len(cols), "cột"),
                     c.item("Ô thiếu (null hoặc rỗng)", total("blank"), "ô"), c.item("Ô không phải số", total("nonNumeric"), "ô"),
                     c.item("Ô ngoài miền hợp lệ", total("outOfDomain"), "ô"), c.item("datetime không đọc được", tstats["unparsable"], "dòng"),
                     c.item("datetime trùng", duplicates, "dòng"), c.item("Giờ bị thiếu giữa hai đầu", missing_hours, "giờ"),
                     c.item("Từ (UTC)", str(t_min), None), c.item("Đến (UTC)", str(t_max), None)]
        r.detail = (f"Spark đọc {c.RAW_FILE}, toàn bộ cột là chuỗi, chỉ đếm, không sửa. "
                    + (f"Cột có vấn đề: {json.dumps(bad, ensure_ascii=False)}. " if bad else "Không cột nào có ô thiếu, không phải số hay ngoài miền của data_dictionary.csv. ")
                    + f"Cột hour/month/dayofweek trong file lệch với giờ UTC ở {tstats['hour_not_utc']}/{tstats['month_not_utc']}/{tstats['dow_not_utc']} dòng "
                      f"và lệch với giờ +07 ở {tstats['hour_not_vn']}/{tstats['month_not_vn']}/{tstats['dow_not_vn']} dòng. "
                    + f"Cờ is_event_anomaly=1: {anomalies['event']} dòng; o3_was_missing=1: {anomalies['o3_filled']} dòng (đã điền từ trước khi vào bước này). "
                    + f"Báo cáo đầy đủ: {c.OUTPUT_DIR}/check_raw.json.")
        spark.stop()
    print(f"[02_check_raw] xong: {rows} dòng, thiếu {total('blank')}, không phải số {total('nonNumeric')}, ngoài miền {total('outOfDomain')}, "
          f"trùng {duplicates}, thiếu giờ {missing_hours}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
