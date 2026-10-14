"""Bổ sung dự đoán CO vào luồng Spark (bước A5): nạp mô hình do pipeline/06b_train.py lưu, dự đoán cho mỗi bản ghi có đủ `modelInputs`.

Tách khỏi spark_kafka_consumer.py để kiểm được trên DataFrame thường (backend/tests/test_a5.py); hàm `enrich` dùng được cho cả DataFrame streaming.
Quy tắc trung thực: bản ghi không có đủ 15 đầu vào của mô hình thì `co_predicted` và `model_version` là null (không điền, không đoán); `co_actual` luôn là `co` của bản ghi.
"""
import json

from model_inputs import FEATURES


def model_inputs_schema():
    from pyspark.sql.types import DoubleType, StructField, StructType
    return StructType([StructField(f, DoubleType(), True) for f in FEATURES])


def load_model(spark, model_dir: str, reference_path: str):
    """(mô hình hồi quy đã huấn luyện, danh sách đặc trưng, phiên bản) hoặc (None, None, lý do) nếu chưa dùng được.

    Kiểm: tệp tham chiếu và mô hình đã lưu có CÙNG danh sách đặc trưng với model_inputs.FEATURES (producer dựng đúng danh sách này) và đúng thứ tự.
    """
    try:
        from pyspark.ml import PipelineModel
        ref = json.loads("\n".join(r.value for r in spark.read.text(reference_path).collect()))
        if ref.get("features") != FEATURES:
            return None, None, f"danh sách đặc trưng trong {reference_path} khác model_inputs.FEATURES: {ref.get('features')}"
        pipeline = PipelineModel.load(model_dir)
        if list(pipeline.stages[0].getInputCols()) != FEATURES:
            return None, None, "VectorAssembler của mô hình đã lưu có đặc trưng khác model_inputs.FEATURES"
        return pipeline.stages[-1], FEATURES, ref["modelVersion"]
    except Exception as e:  # chưa huấn luyện, HDFS, định dạng...
        return None, None, f"{type(e).__name__}: {str(e).splitlines()[0][:200] if str(e) else ''}"


def enrich(df, regressor, version: str):
    """df có cột `modelInputs` (struct 15 đặc trưng, có thể null) và `co`. Trả df thêm: co_actual, co_predicted, model_version, model_inputs (giữ để truy vết); bỏ modelInputs."""
    from pyspark.ml.feature import VectorAssembler
    from pyspark.sql import functions as F

    ok = F.lit(True)
    for f in FEATURES:
        ok = ok & F.col(f"modelInputs.{f}").isNotNull()
    xcols = [f"_x_{f}" for f in FEATURES]
    out = df.select("*", *[F.coalesce(F.col(f"modelInputs.{f}").cast("double"), F.lit(0.0)).alias(x) for f, x in zip(FEATURES, xcols)])
    out = VectorAssembler(inputCols=xcols, outputCol="features").transform(out)
    out = regressor.transform(out)  # thêm cột prediction; dòng thiếu đầu vào mang 0 và bị che ở dưới
    return (out.withColumn("co_actual", F.col("co"))
            .withColumn("co_predicted", F.when(ok, F.round("prediction", 1)))
            .withColumn("model_version", F.when(ok, F.lit(version)))
            .withColumnRenamed("modelInputs", "model_inputs")
            .drop(*xcols, "features", "prediction"))


def without_model(df):
    """Khi chưa có mô hình: cùng các cột đầu ra, dự đoán null."""
    from pyspark.sql import functions as F
    return (df.withColumn("co_actual", F.col("co")).withColumn("co_predicted", F.lit(None).cast("double"))
            .withColumn("model_version", F.lit(None).cast("string")).withColumnRenamed("modelInputs", "model_inputs"))
