#!/bin/bash
# Quản lý phần dữ liệu SỐNG và PHÁT LẠI: producer OpenWeather -> Kafka, producer phát lại -> Kafka, consumer Spark (Kafka -> HDFS Parquet).
# Chạy trong WSL:  bash infra/live_stack.sh start | stop | status
# Điều kiện: HDFS và Kafka đang chạy (Kafka cần Java 17, xem infra/README.md); topic hcmc_co_stream đã có.
# Biến môi trường:
#   OWM_API_KEY      khóa OpenWeather (BẮT BUỘC cho luồng dữ liệu thật; thiếu thì bỏ qua producer OpenWeather và báo rõ, không thoát lỗi)
#   WITH_REPLAY=0    không bật luồng phát lại (mặc định bật)
#   WITH_OPEN_METEO=1  bật thêm producer Open-Meteo cũ (nguồn KHÁC, mặc định TẮT)
#   ENRICH=0         không ghi topic hcmc_co_enriched (Spark vẫn dự đoán và ghi Parquet /live; backend phải đọc topic thô)
#   OWM_INTERVAL (mặc định 300 giây), REPLAY_BASE_INTERVAL (mặc định 10 giây ở tốc độ 1), LIVE_INTERVAL (Open-Meteo, mặc định 60)
# Backend chạy riêng với STREAM_SOURCE=kafka (xem infra/README.md). Script này không đụng backend, Kafka, HDFS.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOGDIR="${LIVE_LOG_DIR:-/tmp}"
OWM_PID="$LOGDIR/live_producer_owm.pid"
REPLAY_PID="$LOGDIR/replay_kafka.pid"
OPENMETEO_PID="$LOGDIR/live_producer.pid"
SPARK_PID="$LOGDIR/spark_kafka.pid"

alive() { [ -f "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null; }

# Mỗi tiến trình nền có đủ chuyển hướng ngay trên lệnh nền (không bọc trong subshell), để không giữ stdout của người gọi:
# nếu không, `bash live_stack.sh start | cut ...` hoặc công cụ chụp đầu ra sẽ treo mãi cho tới khi producer/Spark kết thúc.
launch() {  # launch <pidfile> <logfile> <tên> <lệnh python...>
  local pidfile="$1" log="$2" name="$3"; shift 3
  if alive "$pidfile"; then echo "$name đã chạy (pid $(cat "$pidfile"))"; return; fi
  setsid nohup bash -c 'cd "$1" && shift && exec "$@"' _ "$ROOT" "$@" > "$log" 2>&1 < /dev/null &
  echo $! > "$pidfile"
  echo "$name: đã khởi động, log $log"
}

start() {
  if [ -n "${OWM_API_KEY:-}" ]; then
    launch "$OWM_PID" "$LOGDIR/live_producer_owm.log" "producer OpenWeather (DỮ LIỆU THẬT)" python3 infra/live_producer_owm.py --interval "${OWM_INTERVAL:-300}"
  else
    echo "!! producer OpenWeather: BỎ QUA vì thiếu OWM_API_KEY (export OWM_API_KEY=... rồi chạy lại); không có luồng dữ liệu thật"
  fi
  if [ "${WITH_REPLAY:-1}" = 1 ]; then
    launch "$REPLAY_PID" "$LOGDIR/replay_kafka.log" "producer PHÁT LẠI (mô phỏng)" python3 infra/replay_kafka_producer.py --base-interval "${REPLAY_BASE_INTERVAL:-10}"
  fi
  if [ "${WITH_OPEN_METEO:-0}" = 1 ]; then
    launch "$OPENMETEO_PID" "$LOGDIR/live_producer.log" "producer Open-Meteo (nguồn KHÁC)" python3 infra/live_producer.py --interval "${LIVE_INTERVAL:-60}"
  fi
  if alive "$SPARK_PID"; then echo "spark consumer đã chạy (pid $(cat "$SPARK_PID"))"; else
    setsid nohup bash -c 'cd "$1" && exec spark-submit --driver-memory "$2" --conf spark.ui.port=4040 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 infra/spark_kafka_consumer.py --enriched-topic "$3"' \
      _ "$ROOT" "${SPARK_DRIVER_MEMORY:-1g}" "$([ "${ENRICH:-1}" = 0 ] && echo "" || echo hcmc_co_enriched)" > "$LOGDIR/spark_kafka.log" 2>&1 < /dev/null &
    echo $! > "$SPARK_PID"
    echo "spark consumer: đã khởi động (lần đầu tải connector Kafka ~1 phút), log $LOGDIR/spark_kafka.log"
  fi
}

stop() {
  for f in "$OWM_PID" "$REPLAY_PID" "$OPENMETEO_PID" "$SPARK_PID"; do
    if alive "$f"; then kill "$(cat "$f")" && echo "đã dừng pid $(cat "$f") ($(basename "$f" .pid))"; else echo "không chạy: $(basename "$f" .pid)"; fi
    rm -f "$f"
  done
  pkill -f 'infra/spark_kafka_[c]onsumer.py' 2>/dev/null   # spark-submit sinh tiến trình JVM con
}

status() {
  for pair in "$OWM_PID:producer OpenWeather (thật):live_producer_owm.log" "$REPLAY_PID:producer phát lại (mô phỏng):replay_kafka.log" "$OPENMETEO_PID:producer Open-Meteo (nguồn khác, mặc định tắt):live_producer.log"; do
    IFS=: read -r pf label lg <<< "$pair"
    if alive "$pf"; then echo "$label: chạy (pid $(cat "$pf"))"; else echo "$label: KHÔNG chạy"; fi
    [ -f "$LOGDIR/$lg" ] && tail -n 2 "$LOGDIR/$lg" | cut -c1-200 | sed 's/^/    /'
  done
  if pgrep -f 'infra/spark_kafka_[c]onsumer.py' > /dev/null; then echo "spark consumer: chạy"; else echo "spark consumer: KHÔNG chạy"; fi
}

case "${1:-status}" in
  start) start ;;
  stop) stop ;;
  status) status ;;
  *) echo "dùng: bash infra/live_stack.sh start|stop|status"; exit 2 ;;
esac
