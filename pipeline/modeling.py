"""Phần Spark của bước 6b (mô hình dự đoán CO): đặc trưng, chia theo thời gian, mô hình, hai mốc so sánh, chỉ số. Tách khỏi đọc/ghi HDFS để kiểm được (backend/tests/test_a4.py).

Mục tiêu: CO của giờ t (µg/m³). Đầu vào chỉ gồm thông tin biết đến giờ t-1 và giờ trong ngày/tháng của t:
  hour, month (giờ +07 của t); temperatureC, windSpeedKmh, no, no2, o3, so2, pm25, pm10, nh3 (giá trị giờ t-1); coLag1h, coLag2h, coLag3h, coLag24h (CO các giờ t-1, t-2, t-3, t-24).
Phương án có biến chế độ thêm `regime` (0 = trước chuyển tiếp, 1 = sau). Chia luôn theo thời gian (đầu huấn luyện, cuối kiểm tra), không xáo ngẫu nhiên.
Không dò siêu tham số trên tập kiểm tra: tham số cố định ở MODEL_PARAMS; mô hình tốt nhất chọn theo RMSE trên đoạn kiểm định = 20% cuối của tập huấn luyện.
"""
import common as c

BASE_FEATURES = ["hour", "month", "temperatureC", "windSpeedKmh", "no", "no2", "o3", "so2", "pm25", "pm10", "nh3", "coLag1h", "coLag2h", "coLag3h", "coLag24h"]
LAGGED_FROM = {"temperatureC": "temperature_C", "windSpeedKmh": "wind_speed_kmh", "no": "no", "no2": "no2", "o3": "o3", "so2": "so2", "pm25": "pm2_5", "pm10": "pm10", "nh3": "nh3"}
KINDS = ["linear-regression", "random-forest", "gbt"]
NAMES = {"linear-regression": "Hồi quy tuyến tính", "random-forest": "Random Forest", "gbt": "GBT (Gradient Boosted Trees)"}
MODEL_PARAMS = {"linear-regression": "regParam=0 (bình phương tối thiểu thường), chuẩn hóa đầu vào",
                "random-forest": "numTrees=100, maxDepth=8, seed=42",
                "gbt": "maxIter=100, maxDepth=5, stepSize=0.1, seed=42"}
TRAIN_FRACTION = 0.8
SEED = 42


def build_features(df):
    """df: dữ liệu đã làm sạch, liên tục theo giờ. Trả DataFrame (datetime_utc, datetime_vn, co, BASE_FEATURES); bỏ các dòng thiếu độ trễ (24 dòng đầu)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.orderBy("datetime_utc")
    out = df.select("datetime_utc", "datetime_vn", "co", F.col("hour").alias("hour"), F.col("month").alias("month"),
                    *[F.col(src).alias(f"_{name}") for name, src in LAGGED_FROM.items()], F.col("co").alias("_co"))
    for name in LAGGED_FROM:
        out = out.withColumn(name, F.lag(f"_{name}", 1).over(w))
    for k in (1, 2, 3, 24):
        out = out.withColumn(f"coLag{k}h", F.lag("_co", k).over(w))
    out = out.select("datetime_utc", "datetime_vn", "co", *BASE_FEATURES)
    cond = F.lit(True)
    for col in BASE_FEATURES:
        cond = cond & F.col(col).isNotNull()
    return out.where(cond)


def check_hourly_continuity(df) -> int:
    """Số khoảng cách giữa hai dòng liên tiếp khác đúng 1 giờ. Phải bằng 0 thì lag theo dòng mới là lag theo giờ."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.orderBy("datetime_utc")
    gap = (F.unix_timestamp("datetime_utc") - F.lag(F.unix_timestamp("datetime_utc")).over(w))
    return df.select(gap.alias("gap")).where(F.col("gap").isNotNull() & (F.col("gap") != 3600)).count()


def add_regime(df, post_start_vn: str):
    """regime = 1 từ `post_start_vn` (yyyy-MM-dd HH:mm:ss, giờ +07) trở đi, 0 trước đó."""
    from pyspark.sql import functions as F
    return df.withColumn("regime", (F.col("datetime_vn") >= F.lit(post_start_vn).cast("timestamp")).cast("int"))


def time_split(df, fraction: float = TRAIN_FRACTION):
    """Theo thứ tự thời gian: `fraction` dòng đầu để huấn luyện, phần còn lại để kiểm tra. Trả (train, test, số dòng huấn luyện)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    n = df.count()
    cut = int(n * fraction)
    ranked = df.withColumn("_rn", F.row_number().over(Window.orderBy("datetime_utc")))
    return ranked.where(F.col("_rn") <= cut).drop("_rn"), ranked.where(F.col("_rn") > cut).drop("_rn"), cut


def estimator(kind: str):
    from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
    kw = dict(featuresCol="features", labelCol="co", predictionCol="prediction")
    if kind == "linear-regression":
        return LinearRegression(regParam=0.0, standardization=True, **kw)
    if kind == "random-forest":
        return RandomForestRegressor(numTrees=100, maxDepth=8, seed=SEED, **kw)
    if kind == "gbt":
        return GBTRegressor(maxIter=100, maxDepth=5, stepSize=0.1, seed=SEED, **kw)
    raise ValueError(kind)


def fit(kind: str, train, features: list[str]):
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import VectorAssembler
    return Pipeline(stages=[VectorAssembler(inputCols=features, outputCol="features"), estimator(kind)]).fit(train)


def metrics(pred_df, pred_col: str = "prediction") -> dict:
    """RMSE, MAE, R² (µg/m³) của cột dự đoán so với `co`."""
    from pyspark.ml.evaluation import RegressionEvaluator
    ev = RegressionEvaluator(labelCol="co", predictionCol=pred_col)
    return {m: float(ev.setMetricName(m).evaluate(pred_df)) for m in ("rmse", "mae", "r2")}


def round_metrics(m: dict) -> dict:
    return {"rmse": round(m["rmse"], 3), "mae": round(m["mae"], 3), "r2": round(m["r2"], 4)}


def hourly_mean_predictions(train, test):
    """Mốc 'trung bình theo giờ trong ngày': dự đoán CO của giờ t bằng CO trung bình của cùng giờ +07 trong tập huấn luyện."""
    from pyspark.sql import functions as F
    means = train.groupBy("hour").agg(F.avg("co").alias("prediction"))
    return test.join(means, "hour", "left")


def baselines(train, test) -> dict:
    """Hai mốc trên đúng tập kiểm tra: {'previous-hour': chỉ số, 'hourly-mean': chỉ số}. Dòng kiểm tra thiếu giờ trong tập huấn luyện bị bỏ khỏi mốc thứ hai (không điền)."""
    from pyspark.sql import functions as F
    prev = metrics(test.withColumn("prediction", F.col("coLag1h")))
    hm_df = hourly_mean_predictions(train, test).where(F.col("prediction").isNotNull())
    return {"previous-hour": prev, "hourly-mean": metrics(hm_df)}


def run_setup(train, test, features: list[str], fraction: float = TRAIN_FRACTION) -> dict:
    """Huấn luyện ba mô hình trên `train`, đo trên `test`, kèm hai mốc.

    Mô hình tốt nhất chọn theo RMSE trên đoạn kiểm định (20% cuối của train, theo thời gian) với mô hình huấn luyện trên 80% đầu của train;
    sau đó cả ba được huấn luyện lại trên toàn bộ train rồi mới đo trên test (test không tham gia chọn).
    Trả {'models': {kind: {'val': rmse, 'test': {...}}}, 'best': kind, 'baselines': {...}, 'fitted': {kind: PipelineModel}, 'trainRows', 'testRows'}.
    """
    core, val, _ = time_split(train, fraction)
    base = baselines(train, test)
    out = {"models": {}, "fitted": {}, "baselines": base}
    for kind in KINDS:
        val_rmse = metrics(fit(kind, core, features).transform(val))["rmse"]
        full = fit(kind, train, features)
        out["fitted"][kind] = full
        out["models"][kind] = {"val": val_rmse, "test": metrics(full.transform(test))}
    out["best"] = min(KINDS, key=lambda k: out["models"][k]["val"])
    out["trainRows"], out["testRows"] = train.count(), test.count()
    return out

