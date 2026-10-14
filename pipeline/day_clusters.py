"""Phép tính của bước 6a (phân cụm ngày theo hình dạng 24 giờ), tách khỏi đọc/ghi HDFS để kiểm được trên bảng nhỏ (backend/tests/test_a3.py).

Mỗi NGÀY (theo giờ +07, chỉ ngày đủ 24 giờ) là một điểm 24 chiều. "Hình dạng" = CO từng giờ chia cho CO trung bình của chính ngày đó, nên mức CO tuyệt đối không
quyết định cụm (khác với phân cụm trên CO thô vốn sẽ chỉ tách giai đoạn mức cao/mức thấp quanh điểm gãy 01/07/2025). Mức CO của từng cụm vẫn được báo riêng.
K-Means của Spark MLlib; số cụm k chọn bằng tiêu chí ghi rõ: k có hệ số silhouette (khoảng cách Euclid bình phương) LỚN NHẤT trong k_min..k_max, hòa thì lấy k nhỏ hơn.
"""
SHAPE_COLS = [f"s{h:02d}" for h in range(24)]
RAW_COLS = [f"h{h:02d}" for h in range(24)]
K_MIN, K_MAX, SEED, MAX_ITER = 2, 8, 42, 100


def day_table(spark, df):
    """df: dữ liệu đã làm sạch (datetime_vn, hour, co, after_2025_07_01, dayofweek). Trả (DataFrame mỗi ngày đủ 24 giờ, số ngày bị loại vì thiếu giờ, tổng số ngày)."""
    from pyspark.sql import functions as F

    d = df.select(F.date_format("datetime_vn", "yyyy-MM-dd").alias("date"), "hour", "co", "after_2025_07_01", "dayofweek")
    wide = d.groupBy("date").pivot("hour", list(range(24))).agg(F.first("co")).toDF("date", *RAW_COLS)
    meta = d.groupBy("date").agg(F.countDistinct("hour").alias("n_hours"), F.max("after_2025_07_01").alias("after_2025_07_01"), F.max("dayofweek").alias("dayofweek"))
    days = wide.join(meta, "date")
    total = days.count()
    full = days.where(F.col("n_hours") == 24)
    for c in RAW_COLS:
        full = full.where(F.col(c).isNotNull())
    mean_co = sum(F.col(c) for c in RAW_COLS) / 24.0
    full = full.withColumn("mean_co", mean_co)
    for r, s in zip(RAW_COLS, SHAPE_COLS):
        full = full.withColumn(s, F.col(r) / F.col("mean_co"))
    full = full.cache()
    return full, total - full.count(), total


def select_k(spark, days, k_min=K_MIN, k_max=K_MAX, seed=SEED):
    """Chạy K-Means cho từng k, trả (bảng chọn k, k được chọn, mô hình của k đó, DataFrame có cột cluster)."""
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.evaluation import ClusteringEvaluator
    from pyspark.ml.feature import VectorAssembler

    data = VectorAssembler(inputCols=SHAPE_COLS, outputCol="features").transform(days).cache()
    evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="cluster", metricName="silhouette", distanceMeasure="squaredEuclidean")
    table, best = [], None
    for k in range(k_min, k_max + 1):
        model = KMeans(k=k, seed=seed, maxIter=MAX_ITER, featuresCol="features", predictionCol="cluster").fit(data)
        out = model.transform(data)
        sil = float(evaluator.evaluate(out))
        sizes = sorted((r["n"] for r in out.groupBy("cluster").count().withColumnRenamed("count", "n").collect()), reverse=True)
        table.append({"k": k, "silhouette": round(sil, 4), "wssse": round(float(model.summary.trainingCost), 4), "sizes": sizes})
        if best is None or sil > best[0] + 1e-12:  # hòa thì giữ k nhỏ hơn
            best = (sil, k, model, out)
    return table, best[1], best[2], best[3]


def cluster_stats(assigned, centers, k):
    """Thống kê từng cụm: kích thước, mức CO, tỷ lệ ngày sau điểm gãy, tỷ lệ cuối tuần, tâm cụm và giờ cao/thấp nhất của tâm."""
    from pyspark.sql import functions as F

    agg = {r["cluster"]: r.asDict() for r in assigned.groupBy("cluster").agg(
        F.count("*").alias("n"), F.avg("mean_co").alias("mean_co"), F.percentile_approx("mean_co", 0.5).alias("median_co"),
        F.avg("after_2025_07_01").alias("after_share"), F.avg((F.col("dayofweek") >= 5).cast("double")).alias("weekend_share")).collect()}
    out = []
    for c in range(k):
        a, ctr = agg[c], [round(float(x), 4) for x in centers[c]]
        out.append({"cluster": c, "days": a["n"], "meanDailyCo": round(a["mean_co"], 2), "medianDailyCo": round(a["median_co"], 2),
                    "afterBreakShare": round(a["after_share"], 4), "weekendShare": round(a["weekend_share"], 4),
                    "center": ctr, "peakHour": ctr.index(max(ctr)), "troughHour": ctr.index(min(ctr)), "peakToTrough": round(max(ctr) / min(ctr), 2)})
    return out


def structure_label(sil: float) -> str:
    """Quy ước đọc silhouette của Kaufman và Rousseeuw (1990): trên 0,5 cấu trúc cụm rõ; 0,25 đến 0,5 yếu; dưới 0,25 gần như không có cấu trúc cụm."""
    return "rõ" if sil > 0.5 else "yếu" if sil >= 0.25 else "gần như không có cấu trúc cụm"
