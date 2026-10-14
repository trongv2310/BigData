"""Spark Structured Streaming: đọc topic Kafka hcmc_co_stream, tính chỉ số tích tụ CO và mức cảnh báo, ghi Parquet vào HDFS.

Topic chứa CẢ hai luồng: cột `provider` phân biệt 'openweather' (thật), 'replay-dataset' (phát lại, mô phỏng) và 'open-meteo' (tin cũ).
Luôn lọc theo provider khi truy vấn /live, đừng trộn các nguồn vào một chuỗi.

Bước A5 (dự đoán trên luồng): nạp mô hình dự đoán CO do pipeline/06b_train.py lưu (/co/output/model_best) và, với mỗi bản ghi có đủ `modelInputs` (producer dựng, xem
model_inputs.py), thêm co_actual (= co), co_predicted, model_version, model_inputs, feature_note vào Parquet /live. Bản ghi thiếu đầu vào: co_predicted và model_version là null (không điền).
Cùng bản ghi đã làm giàu còn được ghi vào topic Kafka --enriched-topic (hcmc_co_enriched) để backend phát `reading` kèm coPredicted/modelVersion qua /ws/stream. Chưa có mô hình
thì chạy như cũ (dự đoán null) và nói rõ ở log. Đổi mô hình (phiên bản mới) thì khởi động lại job này.

Thay cho spark_streaming_consumer.py ở thư mục cha (script cũ đọc file JSON trong HDFS, không bị sửa). Cùng phép tính:
  co_accumulation_index = co / (wind_speed_kmh + 1);  canh_bao theo thang CO của OpenWeather (quyết định D3, chốt 13/10/2026):
  từ 4400 µg/m³ là WARNING, từ 12400 là DANGER (cùng giá trị với CO_WARNING_UG, CO_DANGER_UG của backend). Script cũ dùng 500/1000.

Chạy (WSL; lần đầu spark-submit tải connector Kafka từ Maven Central, ~10 MB):
  spark-submit --driver-memory 640m \\
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \\
    infra/spark_kafka_consumer.py
Mặc định ghi vào hdfs://localhost:9000/live/co_stream, checkpoint ở /live/_checkpoints/co_stream (khởi động lại thì đọc tiếp
từ offset đã xử lý, không xử lý lại). Thêm --console để in từng batch ra màn hình (thành query thứ hai).
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import enrich as en  # noqa: E402

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType

os.environ.setdefault("HADOOP_CONF_DIR", "/home/trong/hadoop/etc/hadoop")

p = argparse.ArgumentParser()
p.add_argument("--bootstrap", default="localhost:9092")
p.add_argument("--topic", default="hcmc_co_stream")
p.add_argument("--starting", default="earliest", choices=["earliest", "latest"], help="chỉ có tác dụng ở lần chạy đầu (khi chưa có checkpoint)")
p.add_argument("--out", default="hdfs://localhost:9000/live/co_stream")
p.add_argument("--checkpoint", default="hdfs://localhost:9000/live/_checkpoints/co_stream")
p.add_argument("--trigger", default="30 seconds")
p.add_argument("--warning", type=float, default=float(os.environ.get("CO_WARNING_UG", "4400")), help="µg/m³: từ mức này là WARNING")
p.add_argument("--danger", type=float, default=float(os.environ.get("CO_DANGER_UG", "12400")), help="µg/m³: từ mức này là DANGER")
p.add_argument("--model-dir", default="hdfs://localhost:9000/co/output/model_best", help="PipelineModel do 06b_train.py lưu")
p.add_argument("--model-reference", default="hdfs://localhost:9000/co/output/model_inputs_reference.json", help="tệp danh sách đặc trưng và phiên bản mô hình")
p.add_argument("--enriched-topic", default="hcmc_co_enriched", help="topic Kafka ghi bản ghi đã làm giàu (rỗng = không ghi)")
p.add_argument("--enriched-checkpoint", default="hdfs://localhost:9000/live/_checkpoints/co_enriched")
p.add_argument("--enriched-trigger", default="5 seconds")
p.add_argument("--console", action="store_true")
a = p.parse_args()

spark = (SparkSession.builder.appName("HCMC_CO_Kafka_Streaming").master("local[*]")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("datetime", StringType(), True),
    StructField("station", StringType(), True),
    StructField("co", DoubleType(), True),
    StructField("wind_speed_kmh", DoubleType(), True),
    StructField("temperature_C", DoubleType(), True),
    StructField("coObservedAt", StringType(), True),
    StructField("weatherObservedAt", StringType(), True),
    StructField("provider", StringType(), True),
    # Bước A1: bản phát lại có originalDatetime (giờ gốc của dòng lịch sử, +07); OpenWeather có owmDt, aqi và các chất khác. Tin cũ không có -> null.
    StructField("originalDatetime", StringType(), True),
    StructField("owmDt", LongType(), True),
    StructField("aqi", LongType(), True),
    StructField("no", DoubleType(), True),
    StructField("no2", DoubleType(), True),
    StructField("o3", DoubleType(), True),
    StructField("so2", DoubleType(), True),
    StructField("pm2_5", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("nh3", DoubleType(), True),
    # Bước A5: đầu vào của mô hình do producer dựng (null/không có nếu thiếu lịch sử) và ghi chú về cách dựng
    StructField("modelInputs", en.model_inputs_schema(), True),
    StructField("featureNote", StringType(), True),
    # hợp đồng 0.5.0: co có phải điểm dự báo của OpenWeather không (xem model_inputs.forecast_flag); không có trường thì null
    StructField("coIsForecast", BooleanType(), True),
])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", a.bootstrap)
       .option("subscribe", a.topic)
       .option("startingOffsets", a.starting)
       .option("failOnDataLoss", "false")
       .load())

parsed = (raw.select(F.col("topic"), F.col("partition"), F.col("offset"), F.col("timestamp").alias("kafka_timestamp"),
                     F.from_json(F.col("value").cast("string"), schema).alias("r"))
          .select("topic", "partition", "offset", "kafka_timestamp", "r.*")
          .where(F.col("co").isNotNull() & F.col("wind_speed_kmh").isNotNull()))  # tin hỏng/thiếu trường thì bỏ, không điền số giả

processed = (parsed
             .withColumn("co_accumulation_index", F.round(F.col("co") / (F.col("wind_speed_kmh") + 1.0), 2))
             .withColumn("canh_bao", F.when(F.col("co") >= a.danger, "NGUY HIEM (DANGER)")
                                      .when(F.col("co") >= a.warning, "CANH BAO (WARNING)")
                                      .otherwise("AN TOAN (SAFE)")))

regressor, features, version_or_reason = en.load_model(spark, a.model_dir, a.model_reference)
if regressor is None:
    print(f"\n!!! CHƯA CÓ MÔ HÌNH ({version_or_reason}): co_predicted sẽ là null. Chạy pipeline/06b_train.py rồi khởi động lại job này. !!!\n", flush=True)
    enriched = en.without_model(processed)
else:
    print(f"\n>>> Mô hình dự đoán CO: {version_or_reason} ({len(features)} đặc trưng) <<<\n", flush=True)
    enriched = en.enrich(processed, regressor, version_or_reason)
enriched = enriched.withColumnRenamed("featureNote", "feature_note").withColumnRenamed("coIsForecast", "co_is_forecast")

query = (enriched.writeStream.outputMode("append").format("parquet")
         .option("path", a.out).option("checkpointLocation", a.checkpoint)
         .trigger(processingTime=a.trigger).start())

if a.enriched_topic:
    # cùng bản ghi, dạng JSON giữ nguyên tên trường của producer (datetime, originalDatetime, station, co, wind_speed_kmh, temperature_C, provider, ...) + co_predicted, model_version
    json_cols = ["datetime", "originalDatetime", "station", "co", "wind_speed_kmh", "temperature_C", "coObservedAt", "weatherObservedAt", "provider", "owmDt", "aqi",
                 "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3", "co_predicted", "model_version", "co_is_forecast", "feature_note"]
    out_json = enriched.select(F.col("station").alias("key"), F.to_json(F.struct(*[F.col(c) for c in json_cols], F.col("partition").alias("kafka_partition"),
                                                                                 F.col("offset").alias("kafka_offset"))).alias("value"))
    (out_json.writeStream.format("kafka").option("kafka.bootstrap.servers", a.bootstrap).option("topic", a.enriched_topic)
     .option("checkpointLocation", a.enriched_checkpoint).trigger(processingTime=a.enriched_trigger).start())

if a.console:
    enriched.drop("model_inputs").writeStream.outputMode("append").format("console").option("truncate", False).start()

print(f"\n>>> SPARK STRUCTURED STREAMING: Kafka {a.bootstrap}/{a.topic} -> {a.out} (checkpoint {a.checkpoint}) <<<\n", flush=True)
spark.streams.awaitAnyTermination()
