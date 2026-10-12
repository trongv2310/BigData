#!/bin/bash
# Quản lý phần dữ liệu SỐNG: producer Open-Meteo -> Kafka và consumer Spark Structured Streaming (Kafka -> HDFS Parquet).
# Chạy trong WSL:  bash infra/live_stack.sh start | stop | status
# Điều kiện: HDFS và Kafka đang chạy (Kafka cần Java 17, xem infra/README.md); topic hcmc_co_stream đã có.
# Backend chạy riêng với STREAM_SOURCE=kafka (xem infra/README.md). Script này không đụng backend, Kafka, HDFS.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOGDIR="${LIVE_LOG_DIR:-/tmp}"
PRODUCER_PID="$LOGDIR/live_producer.pid"
SPARK_PID="$LOGDIR/spark_kafka.pid"

alive() { [ -f "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null; }

start() {
  if alive "$PRODUCER_PID"; then echo "producer đã chạy (pid $(cat "$PRODUCER_PID"))"; else
    (cd "$ROOT" && setsid nohup python3 infra/live_producer.py --interval "${LIVE_INTERVAL:-60}" > "$LOGDIR/live_producer.log" 2>&1 < /dev/null & echo $! > "$PRODUCER_PID")
    echo "producer: đã khởi động, log $LOGDIR/live_producer.log"
  fi
  if alive "$SPARK_PID"; then echo "spark consumer đã chạy (pid $(cat "$SPARK_PID"))"; else
    (cd "$ROOT" && setsid nohup spark-submit --driver-memory "${SPARK_DRIVER_MEMORY:-640m}" --conf spark.ui.port=4040 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 infra/spark_kafka_consumer.py \
      > "$LOGDIR/spark_kafka.log" 2>&1 < /dev/null & echo $! > "$SPARK_PID")
    echo "spark consumer: đã khởi động (lần đầu tải connector Kafka ~1 phút), log $LOGDIR/spark_kafka.log"
  fi
}

stop() {
  for f in "$PRODUCER_PID" "$SPARK_PID"; do
    if alive "$f"; then kill "$(cat "$f")" && echo "đã dừng pid $(cat "$f")"; else echo "không chạy: $f"; fi
    rm -f "$f"
  done
  pkill -f 'infra/spark_kafka_[c]onsumer.py' 2>/dev/null   # spark-submit sinh tiến trình JVM con
}

status() {
  alive "$PRODUCER_PID" && echo "producer: chạy (pid $(cat "$PRODUCER_PID"))" || echo "producer: KHÔNG chạy"
  if pgrep -f 'infra/spark_kafka_[c]onsumer.py' > /dev/null; then echo "spark consumer: chạy"; else echo "spark consumer: KHÔNG chạy"; fi
  [ -f "$LOGDIR/live_producer.log" ] && echo "--- 3 dòng cuối log producer:" && tail -n 3 "$LOGDIR/live_producer.log" | cut -c1-200
}

case "${1:-status}" in
  start) start ;;
  stop) stop ;;
  status) status ;;
  *) echo "dùng: bash infra/live_stack.sh start|stop|status"; exit 2 ;;
esac
