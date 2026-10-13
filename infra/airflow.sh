#!/bin/bash
# Bật/tắt Airflow (standalone: api-server, scheduler, dag-processor, triggerer trong một lệnh) cho DAG hcmc_co_pipeline.
# Chạy trong WSL:  bash infra/airflow.sh start | stop | status
# - Không sửa ~/airflow/airflow.cfg: tắt ví dụ mẫu bằng biến môi trường AIRFLOW__CORE__LOAD_EXAMPLES=False.
# - DAG: sao chép (không di chuyển) test_airflow_dag.py của thư mục cha vào ~/airflow/dags/hcmc_co_pipeline.py nếu chưa có.
# - Cần khoảng 1 GB RAM trống (đã đo: còn ~1,3 GB trống khi Airflow chạy cùng HDFS, Kafka, Spark, backend, YARN tắt).
# - UI: http://localhost:8080; tài khoản và mật khẩu sinh ra nằm ở ~/airflow/simple_auth_manager_passwords.json.generated.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOGDIR="${LIVE_LOG_DIR:-/tmp}"
PIDFILE="$LOGDIR/airflow.pid"
HEALTH="http://localhost:8080/api/v2/monitor/health"

up() { [ "$(curl -s -m 3 -o /dev/null -w '%{http_code}' "$HEALTH")" = "200" ]; }

start() {
  if up; then echo "Airflow đã chạy ($HEALTH trả 200)"; return 0; fi
  mkdir -p "$HOME/airflow/dags"
  [ -f "$HOME/airflow/dags/hcmc_co_pipeline.py" ] || cp "$ROOT/../test_airflow_dag.py" "$HOME/airflow/dags/hcmc_co_pipeline.py"
  setsid nohup bash -c 'cd "$HOME" && AIRFLOW__CORE__LOAD_EXAMPLES=False exec airflow standalone' > "$LOGDIR/airflow.log" 2>&1 < /dev/null &
  echo $! > "$PIDFILE"
  echo "Airflow: đang khởi động, log $LOGDIR/airflow.log"
  # Giữ shell này sống cho tới khi Airflow trả lời: tiến trình nền khởi động từ một shell thoát ngay có thể bị WSL dọn mất.
  for _ in $(seq 1 18); do
    sleep 5
    if up; then echo "Airflow đã sẵn sàng: http://localhost:8080"; return 0; fi
  done
  echo "Airflow chưa trả lời sau 90 giây, xem $LOGDIR/airflow.log"; return 1
}

stop() {
  if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then kill "$(cat "$PIDFILE")" && echo "đã gửi tín hiệu dừng cho pid $(cat "$PIDFILE")"; else echo "không có pid Airflow đang chạy"; fi
  rm -f "$PIDFILE"
  sleep 5
  pkill -f '[a]irflow (standalone|api-server|scheduler|dag-processor|triggerer)' 2>/dev/null   # tiến trình còn sót (mẫu này không khớp chính script)
  return 0
}

status() {
  if up; then echo "Airflow: chạy"; curl -s -m 5 "$HEALTH" | head -c 400; echo; else echo "Airflow: KHÔNG chạy"; fi
}

case "${1:-status}" in
  start) start ;;
  stop) stop ;;
  status) status ;;
  *) echo "dùng: bash infra/airflow.sh start|stop|status"; exit 2 ;;
esac
