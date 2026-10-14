"""Nạp mô hình dự đoán CO (Spark PipelineModel do pipeline/06b_train.py lưu ở /co/output/model_best) và dự đoán một dòng đầu vào.

Chỉ ĐỌC mô hình từ HDFS. Dùng chung JVM với sql_engine (SparkSession.getOrCreate), nên không thêm JVM thứ hai. Mô hình được giữ trong bộ nhớ theo phiên bản
(đổi phiên bản trong model_inputs_reference.json thì nạp lại). Không có mô hình hoặc Spark không dựng được thì ném MlUnavailable.
"""
import threading

from . import config

_lock = threading.Lock()
_spark = None
_cache: dict[str, tuple[str, object]] = {}  # uri -> (phiên bản, PipelineModel)


class MlUnavailable(RuntimeError):
    """Không dựng được Spark, không nạp được mô hình hoặc dự đoán lỗi."""


def _session():
    global _spark
    if _spark is None:
        try:
            from pyspark.sql import SparkSession

            _spark = (SparkSession.builder.appName("hcmc_co_backend_ml").master("local[2]")
                      .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC")
                      .config("spark.sql.shuffle.partitions", "2").config("spark.driver.memory", "768m").getOrCreate())
            _spark.sparkContext.setLogLevel("ERROR")
        except Exception as e:  # JVM, thiếu pyspark...
            raise MlUnavailable(f"Không dựng được Spark: {type(e).__name__}: {str(e)[:200]}") from e
    return _spark


def predict(uri: str, version: str, order: list[str], values: dict) -> float:
    """Dự đoán CO (µg/m³) cho một dòng; `order` là tên các đặc trưng, `values` đủ mọi đặc trưng."""
    with _lock:
        try:
            from pyspark.ml import PipelineModel
            from pyspark.sql.types import DoubleType, StructField, StructType

            spark = _session()
            hit = _cache.get(uri)
            if hit is None or hit[0] != version:
                _cache[uri] = (version, PipelineModel.load(uri))
            model = _cache[uri][1]
            df = spark.createDataFrame([tuple(float(values[k]) for k in order)], StructType([StructField(k, DoubleType(), False) for k in order]))
            return float(model.transform(df).select("prediction").first()["prediction"])
        except MlUnavailable:
            raise
        except Exception as e:
            _cache.pop(uri, None)
            raise MlUnavailable(f"Không nạp hoặc chạy được mô hình {version}: {type(e).__name__}: {str(e)[:200]}") from e


def stop() -> None:
    global _spark
    with _lock:
        _cache.clear()
        _spark = None  # phiên Spark dùng chung với sql_engine, do sql_engine.stop() dừng
