"""Spark Structured Streaming: đọc topic Kafka hcmc_co_stream, tính chỉ số tích tụ CO và mức cảnh báo, ghi Parquet vào HDFS.

Thay cho spark_streaming_consumer.py ở thư mục cha (script cũ đọc file JSON trong HDFS, không bị sửa). Cùng phép tính:
  co_accumulation_index = co / (wind_speed_kmh + 1);  canh_bao theo ngưỡng 500 / 1000 µg/m³ (quyết định D3, TẠM, chưa có căn cứ chuẩn).

Chạy (WSL; lần đầu spark-submit tải connector Kafka từ Maven Central, ~10 MB):
  spark-submit --driver-memory 640m \\
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \\
    infra/spark_kafka_consumer.py
Mặc định ghi vào hdfs://localhost:9000/live/co_stream, checkpoint ở /live/_checkpoints/co_stream (khởi động lại thì đọc tiếp
từ offset đã xử lý, không xử lý lại). Thêm --console để in từng batch ra màn hình (thành query thứ hai).
"""
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

os.environ.setdefault("HADOOP_CONF_DIR", "/home/trong/hadoop/etc/hadoop")

p = argparse.ArgumentParser()
p.add_argument("--bootstrap", default="localhost:9092")
p.add_argument("--topic", default="hcmc_co_stream")
p.add_argument("--starting", default="earliest", choices=["earliest", "latest"], help="chỉ có tác dụng ở lần chạy đầu (khi chưa có checkpoint)")
p.add_argument("--out", default="hdfs://localhost:9000/live/co_stream")
p.add_argument("--checkpoint", default="hdfs://localhost:9000/live/_checkpoints/co_stream")
p.add_argument("--trigger", default="30 seconds")
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
             .withColumn("canh_bao", F.when(F.col("co") > 1000, "NGUY HIEM (DANGER)")
                                      .when(F.col("co") > 500, "CANH BAO (WARNING)")
                                      .otherwise("AN TOAN (SAFE)")))

query = (processed.writeStream.outputMode("append").format("parquet")
         .option("path", a.out).option("checkpointLocation", a.checkpoint)
         .trigger(processingTime=a.trigger).start())

if a.console:
    processed.writeStream.outputMode("append").format("console").option("truncate", False).start()

print(f"\n>>> SPARK STRUCTURED STREAMING: Kafka {a.bootstrap}/{a.topic} -> {a.out} (checkpoint {a.checkpoint}) <<<\n", flush=True)
spark.streams.awaitAnyTermination()
