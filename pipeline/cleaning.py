"""Phép biến đổi của bước 4 (làm sạch), tách riêng khỏi việc đọc/ghi HDFS để kiểm được bằng bảng nhỏ (backend/tests/test_a2.py).

`clean(raw)` nhận DataFrame đọc từ CSV với MỌI cột là chuỗi (đúng như 02_check_raw và 04_clean đọc), trả (DataFrame sạch, thống kê). Quy tắc xem 04_clean.py.
"""
import common as c

OUTPUT_COLUMNS = ["datetime_utc", "datetime_vn", "year", "month", "dayofweek", "hour", "after_2025_07_01", "aqi_label", *c.GASES,
                  "temperature_C", "humidity_pct", "humidity_clipped", "rain_mm", "wind_speed_kmh", "is_event_anomaly", "o3_was_missing"]


def clean(raw):
    from pyspark.sql import functions as F

    num = lambda name: F.col(f"`{name}`").cast("double")  # noqa: E731
    n_in = raw.count()
    df = raw.withColumn("datetime_utc", F.to_timestamp(F.col("datetime"), c.DATETIME_FORMAT))
    n_bad_dt = df.where(F.col("datetime_utc").isNull()).count()
    df = df.where(F.col("datetime_utc").isNotNull())
    # CO là đại lượng cần dự đoán: thiếu, không phải số hoặc âm thì bỏ cả dòng (không điền)
    bad_co = num("co").isNull() | (num("co") < 0)
    n_bad_co = df.where(bad_co).count()
    df = df.where(~bad_co)
    n_before_dedup = df.count()
    df = df.dropDuplicates(["datetime_utc"])
    n_dups = n_before_dedup - df.count()

    nulled, exprs = {}, {}
    for col in c.RAW_COLUMNS:
        if col == "datetime":
            continue
        lo, hi = c.DOMAIN.get(col, (None, None))
        v = num(col)
        bad = F.col(f"`{col}`").isNotNull() & (F.trim(F.col(f"`{col}`")) != "") & v.isNull()  # có chữ nhưng không phải số
        if lo is not None:
            bad = bad | (v < lo)
        if hi is not None and col != "humidity_%":  # độ ẩm > 100 xử lý riêng (cắt về 100)
            bad = bad | (v > hi)
        nulled[col] = bad
        exprs[col] = F.when(bad, F.lit(None).cast("double")).otherwise(v)
        if col == "humidity_%":
            exprs[col] = F.when(v > 100, F.lit(100.0)).otherwise(exprs[col])  # bão hòa: cắt về 100, cờ humidity_clipped đánh dấu
    counts = df.agg(*[F.count(F.when(b, 1)).alias(k) for k, b in nulled.items()],
                    F.count(F.when(num("humidity_%") > 100, 1)).alias("humidity_gt_100")).first().asDict()
    n_clipped = counts.pop("humidity_gt_100")

    keep = [k for k in exprs if k not in ("hour", "month", "dayofweek")]  # ba cột này tính lại theo giờ +07 bên dưới
    df = df.select("datetime_utc", (F.col("datetime_utc") + F.expr("interval 7 hours")).alias("datetime_vn"),
                   *[exprs[k].alias("humidity_pct" if k == "humidity_%" else k) for k in keep],
                   (num("humidity_%") > 100).cast("int").alias("humidity_clipped"))
    for k in ("aqi_label", "is_event_anomaly", "o3_was_missing"):
        df = df.withColumn(k, F.col(k).cast("int"))
    dow = (F.dayofweek("datetime_vn") + 5) % 7  # Spark: 1 = Chủ Nhật; data_dictionary: 0 = Thứ Hai
    df = (df.withColumn("year", F.year("datetime_vn")).withColumn("month", F.month("datetime_vn")).withColumn("dayofweek", dow)
          .withColumn("hour", F.hour("datetime_vn"))
          .withColumn("after_2025_07_01", (F.col("datetime_vn") >= F.lit(c.BREAK_VN).cast("timestamp")).cast("int"))
          .orderBy("datetime_utc").select(*OUTPUT_COLUMNS))
    stats = {"rowsIn": n_in, "droppedBadDatetime": n_bad_dt, "droppedBadCo": n_bad_co, "droppedDuplicates": n_dups,
             "nulledByColumn": {k: v for k, v in counts.items() if v}, "nulledTotal": sum(counts.values()), "humidityClipped": n_clipped}
    return df, stats
