#!/bin/bash
# Bật/tắt Airflow (standalone: api-server, scheduler, dag-processor, triggerer trong một lệnh) và cài hai DAG:
#   hcmc_co_pipeline        DAG THẬT (bước 9): infra/airflow_dags/hcmc_co_pipeline.py, nối các bước pipeline bằng spark-submit; liên kết tượng trưng vào ~/airflow/dags; TẠM DỪNG, không lịch.
#   hcmc_co_pipeline_sample DAG mẫu cũ (số liệu cố định, test_airflow_dag.py của thư mục cha, KHÔNG sửa): bản cài đặt chỉ khác dòng dag_id vì id gốc `hcmc_co_pipeline` trùng DAG thật.
# Chạy trong WSL:  bash infra/airflow.sh start | stop | status | install   (install chỉ cài DAG, không bật Airflow; dùng cho `airflow dags test`)
# - Không sửa ~/airflow/airflow.cfg: tắt ví dụ mẫu bằng biến môi trường AIRFLOW__CORE__LOAD_EXAMPLES=False.
# - DAG thật được liên kết (ln -s) nên sửa trong repo là Airflow thấy ngay; DAG mẫu là bản sao đã đổi id, chỉ tạo nếu chưa có.
# - Cần khoảng 1 GB RAM trống (đã đo: còn ~1,3 GB trống khi Airflow chạy cùng HDFS, Kafka, Spark, backend, YARN tắt).
# - UI: http://localhost:8080; tài khoản và mật khẩu sinh ra nằm ở ~/airflow/simple_auth_manager_passwords.json.generated.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOGDIR="${LIVE_LOG_DIR:-/tmp}"
PIDFILE="$LOGDIR/airflow.pid"
HEALTH="http://localhost:8080/api/v2/monitor/health"

up() { [ "$(curl -s -m 3 -o /dev/null -w '%{http_code}' "$HEALTH")" = "200" ]; }

install_dags() {
  local d="$HOME/airflow/dags" orig="" c
  # DAG mẫu gốc của người dùng (thư mục cha của repo; worktree Orca nằm chỗ khác nên cho đặt bằng SAMPLE_DAG)
  for c in "${SAMPLE_DAG:-}" "$ROOT/../test_airflow_dag.py" "/mnt/f/HK1_Nam04/BigData/test_airflow_dag.py"; do
    if [ -n "$c" ] && [ -f "$c" ]; then orig="$c"; break; fi
  done
  mkdir -p "$d"
  # Bản cũ của ~/airflow/dags/hcmc_co_pipeline.py là bản SAO của DAG mẫu (do script này cài). Chỉ thay nếu vẫn giống hệt file gốc của người dùng; nếu người dùng đã sửa thì dừng, không đè.
  if [ -f "$d/hcmc_co_pipeline.py" ] && [ ! -L "$d/hcmc_co_pipeline.py" ]; then
    if [ -n "$orig" ] && cmp -s "$d/hcmc_co_pipeline.py" "$orig"; then rm -f "$d/hcmc_co_pipeline.py"
    else echo "!! $d/hcmc_co_pipeline.py không phải bản sao nguyên văn của DAG mẫu và không phải liên kết; không đè. Đổi tên hoặc xóa nó rồi chạy lại."; return 1; fi
  fi
  ln -sfn "$ROOT/infra/airflow_dags/hcmc_co_pipeline.py" "$d/hcmc_co_pipeline.py"
  # DAG mẫu cũ: cùng nội dung, chỉ đổi dag_id để không trùng DAG thật (file gốc ở thư mục cha không bị đụng)
  if [ ! -f "$d/hcmc_co_pipeline_sample.py" ] && [ -n "$orig" ]; then
    sed 's/dag_id="hcmc_co_pipeline"/dag_id="hcmc_co_pipeline_sample"/' "$orig" > "$d/hcmc_co_pipeline_sample.py"
  fi
  echo "DAG đã cài: hcmc_co_pipeline (thật, tạm dừng), hcmc_co_pipeline_sample (mẫu cũ)"
}

start() {
  if up; then echo "Airflow đã chạy ($HEALTH trả 200)"; return 0; fi
  install_dags || return 1
  setsid nohup bash -c 'cd "$HOME" && HCMC_CO_REPO="$1" AIRFLOW__CORE__LOAD_EXAMPLES=False exec airflow standalone' _ "$ROOT" > "$LOGDIR/airflow.log" 2>&1 < /dev/null &
  echo $! > "$PIDFILE"
  echo "Airflow: đang khởi động, log $LOGDIR/airflow.log"
  # Giữ shell này sống cho tới khi Airflow trả lời: tiến trình nền khởi động từ một shell thoát ngay có thể bị WSL dọn mất.
  for _ in $(seq 1 18); do
    sleep 5
    if up; then echo "Airflow đã sẵn sàng: http://localhost:8080"; return 0; fi
  done
  echo "Airflow chưa trả lời sau 90 giây, xem $LOGDIR/airflow.log"; return 1
}

LEFTOVER='[a]irflow (standalone|api[-_]server|scheduler|dag[-_]processor|triggerer|worker|serve-l)'  # không khớp chính script này

alive_count() { pgrep -f "$LEFTOVER" | wc -l; }

stop() {
  # Chỉ dừng `airflow standalone` thì scheduler, api-server và các worker thành tiến trình mồ côi (đã gặp: 35 tiến trình còn sống,
  # scheduler và worker bỏ qua SIGTERM). Nên dừng cả nhóm tiến trình (standalone chạy trong phiên riêng nhờ setsid), rồi SIGKILL nếu còn sót.
  if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    pg="$(ps -o pgid= -p "$(cat "$PIDFILE")" | tr -d ' ')"
    kill -TERM -- "-$pg" 2>/dev/null && echo "đã gửi SIGTERM cho nhóm tiến trình $pg"
  else
    echo "không có pid Airflow trong $PIDFILE, dừng theo tên tiến trình"
    pkill -TERM -f "$LEFTOVER" 2>/dev/null
  fi
  rm -f "$PIDFILE"
  for _ in $(seq 1 10); do [ "$(alive_count)" = "0" ] && break; sleep 2; done
  if [ "$(alive_count)" != "0" ]; then
    echo "còn $(alive_count) tiến trình Airflow bỏ qua SIGTERM, gửi SIGKILL"
    pkill -KILL -f "$LEFTOVER" 2>/dev/null
    sleep 2
  fi
  echo "Airflow đã dừng (còn $(alive_count) tiến trình)"
}

status() {
  if up; then echo "Airflow: chạy"; curl -s -m 5 "$HEALTH" | head -c 400; echo; else echo "Airflow: KHÔNG chạy"; fi
}

case "${1:-status}" in
  start) start ;;
  install) install_dags ;;
  stop) stop ;;
  status) status ;;
  *) echo "dùng: bash infra/airflow.sh start|stop|status|install"; exit 2 ;;
esac
