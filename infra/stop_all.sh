#!/bin/bash
# Dừng sạch phần chạy trong WSL theo thứ tự ngược với start_all.sh: backend -> dữ liệu sống -> Airflow -> (YARN) -> Kafka -> HDFS.
# Chạy trong WSL:  bash infra/stop_all.sh          # dừng hết, kể cả Kafka và HDFS (nên làm trước khi tắt máy hoặc wsl --shutdown)
#                  KEEP_INFRA=1 bash infra/stop_all.sh   # chỉ dừng backend, dữ liệu sống, Airflow; giữ HDFS, Kafka
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "== backend"; pkill -f "uvicorn app.main:ap[p]"; sleep 2
echo "== dữ liệu sống"; bash "$ROOT/infra/live_stack.sh" stop
echo "== Airflow"; bash "$ROOT/infra/airflow.sh" stop
if pgrep -f '[R]esourceManager|[N]odeManager' > /dev/null; then
  echo "== YARN"
  "$HOME/hadoop/bin/yarn" --daemon stop nodemanager; "$HOME/hadoop/bin/yarn" --daemon stop resourcemanager
fi
if [ "${KEEP_INFRA:-0}" != 1 ]; then
  echo "== Kafka"; bash "$HOME/stop-kafka.sh"; sleep 6
  echo "== HDFS"; bash "$HOME/stop-4nodes.sh" 2>&1 | grep -v 'No such file or directory'
fi
sleep 3
echo "== còn chạy:"; pgrep -af "[j]ava|[u]vicorn|[a]irflow (standalone|api|scheduler|worker)|[l]ive_producer" | cut -c1-100 || true
echo "(hết)"
