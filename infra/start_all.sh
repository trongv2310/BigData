#!/bin/bash
# Khởi động toàn bộ phần chạy trong WSL, đúng thứ tự, mỗi bước có kiểm tra: HDFS -> Kafka -> (YARN) -> Airflow -> dữ liệu sống -> backend.
# Chạy trong WSL:  bash infra/start_all.sh
# Biến tùy chọn:   WITH_YARN=1 (bật YARN, mặc định tắt để lấy RAM)   WITH_AIRFLOW=0 (không bật Airflow)
#                  BACKEND_ALLOW_WRITES=1 (bật cửa sổ lệnh, mặc định tắt)   ALLOWED_ORIGINS=... (mặc định cho phép cổng 5173-5175)
# Frontend chạy riêng trên Windows (xem docs/DEMO.md bước 7). Script không sửa các script cũ ở thư mục cha hay trong ~.
# Đã thử khởi động nguội (wsl --shutdown rồi chạy script này) ngày 13/10/2026, xem docs/HANDOFF.md P18.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOGDIR="${LIVE_LOG_DIR:-/tmp}"
J17=/usr/lib/jvm/java-17-openjdk-amd64
HDFS_BIN="$HOME/hadoop/bin/hdfs"
KAFKA_BIN=/home/trong/kafka/bin
ORIGINS="${ALLOWED_ORIGINS:-http://localhost:5173,http://127.0.0.1:5173,http://localhost:5174,http://127.0.0.1:5174,http://localhost:5175,http://127.0.0.1:5175}"
FAIL=0

step() { echo; echo "== $*"; }
warn() { echo "!! $*"; FAIL=1; }
kafka() { JAVA_HOME=$J17 PATH=$J17/bin:$PATH "$KAFKA_BIN/$1" "${@:2}"; }

# 1. HDFS (4 DataNode): script của người dùng, cần sudo cho ssh (hiện không cần mật khẩu)
step "1. HDFS"
if jps 2>/dev/null | grep -qw NameNode; then
  echo "NameNode đã chạy"
else
  bash "$HOME/start-4nodes.sh" > "$LOGDIR/hdfs_start.log" 2>&1 || true
  echo "đã chạy ~/start-4nodes.sh (log $LOGDIR/hdfs_start.log)"
fi
timeout 180 "$HDFS_BIN" dfsadmin -safemode wait > /dev/null 2>&1   # sau khởi động nguội NameNode ở safe mode một lúc
live="$("$HDFS_BIN" dfsadmin -report 2>/dev/null | grep -o 'Live datanodes ([0-9]*)')"
echo "$live; safe mode: $("$HDFS_BIN" dfsadmin -safemode get 2>/dev/null)"
case "$live" in *"(4)"*) ;; *) warn "HDFS chưa đủ 4 DataNode";; esac

# 2. Kafka (KRaft, Java 17). Dữ liệu ở /home/trong/kafka-data nên topic còn sau khi khởi động lại WSL.
step "2. Kafka"
if pgrep -f "[k]afka.Kafka" > /dev/null; then
  echo "Kafka đã chạy"
else
  JAVA_HOME=$J17 PATH=$J17/bin:$PATH "$KAFKA_BIN/kafka-server-start.sh" -daemon /home/trong/kafka/config/server.properties
  echo "đã gửi lệnh khởi động Kafka"
fi
ok=0
for _ in $(seq 1 24); do
  if kafka kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic hcmc_co_stream > "$LOGDIR/kafka_topic.txt" 2>&1; then ok=1; break; fi
  sleep 5
done
if [ "$ok" = 1 ]; then
  head -1 "$LOGDIR/kafka_topic.txt" | cut -c1-120
  echo "end offset: $(kafka kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic hcmc_co_stream 2>/dev/null | tail -1)"
else
  warn "Kafka chưa trả lời hoặc thiếu topic hcmc_co_stream (xem infra/README.md nếu thư mục dữ liệu bị mất)"
fi

# 3. YARN (tùy chọn)
if [ "${WITH_YARN:-0}" = 1 ]; then
  step "3. YARN"
  setsid nohup "$HOME/hadoop/bin/yarn" --daemon start resourcemanager > /dev/null 2>&1 < /dev/null &
  sleep 3
  setsid nohup "$HOME/hadoop/bin/yarn" --daemon start nodemanager > /dev/null 2>&1 < /dev/null &
  sleep 8
  pgrep -af '[R]esourceManager|[N]odeManager' | cut -c1-70
else
  step "3. YARN: bỏ qua (WITH_YARN=1 để bật)"
fi

# 4. Airflow (tùy chọn, mặc định bật)
if [ "${WITH_AIRFLOW:-1}" = 1 ]; then
  step "4. Airflow"
  bash "$ROOT/infra/airflow.sh" start || warn "Airflow không lên"
else
  step "4. Airflow: bỏ qua (WITH_AIRFLOW=0)"
fi

# 5. Dữ liệu sống: producer Open-Meteo -> Kafka và Spark Structured Streaming -> HDFS /live
step "5. Dữ liệu sống"
bash "$ROOT/infra/live_stack.sh" start
for _ in $(seq 1 12); do pgrep -f 'infra/spark_kafka_[c]onsumer.py' > /dev/null && grep -q "SPARK STRUCTURED STREAMING" "$LOGDIR/spark_kafka.log" 2>/dev/null && break; sleep 5; done
grep -q "SPARK STRUCTURED STREAMING" "$LOGDIR/spark_kafka.log" 2>/dev/null && echo "Spark consumer đã chạy truy vấn streaming" || warn "Spark consumer chưa sẵn sàng (xem $LOGDIR/spark_kafka.log)"

# 6. Backend (đọc Kafka, chỉ đọc mặc định)
step "6. Backend"
if pgrep -f "uvicorn app.main:ap[p]" > /dev/null; then
  echo "backend đã chạy"
else
  cd "$ROOT/backend" || exit 1
  STREAM_SOURCE=kafka FLOWS_EXTRA=1 ALLOWED_ORIGINS="$ORIGINS" BACKEND_ALLOW_WRITES="${BACKEND_ALLOW_WRITES:-}" \
    setsid nohup python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000 > "$LOGDIR/backend.log" 2>&1 < /dev/null &
  echo "đã khởi động backend, log $LOGDIR/backend.log"
fi
ok=0
for _ in $(seq 1 15); do curl -s -m 3 -o /dev/null localhost:8000/api/services && { ok=1; break; }; sleep 2; done
[ "$ok" = 1 ] || warn "backend không trả lời trên cổng 8000"

# Tóm tắt
step "Tóm tắt (từ /api/services)"
curl -s -m 10 localhost:8000/api/services > "$LOGDIR/services.json" 2>/dev/null
python3 - "$LOGDIR/services.json" <<'PY' || warn "không đọc được /api/services"
import sys, json
for s in json.load(open(sys.argv[1]))["services"]:
    print(f'  {s["name"]:<16} {s["state"]:<8} {(s.get("detail") or "")[:62]}')
PY
echo
[ "${BACKEND_ALLOW_WRITES:-}" = 1 ] && echo "Cửa sổ lệnh BẬT, token: $(grep -o 'một lần): .*' "$LOGDIR/backend.log" | sed 's/.*: //')"
echo "Frontend (Windows): cd frontend; \$env:VITE_API_BASE='http://localhost:8000'; \$env:VITE_STREAM='ws'; npm run dev -- --port 5173 --strictPort"
sleep 3   # giữ shell sống thêm chút cho các tiến trình nền vừa tách phiên
[ "$FAIL" = 0 ] && echo "XONG: không có cảnh báo." || echo "XONG, nhưng có cảnh báo (dòng bắt đầu bằng !!)."
exit "$FAIL"
