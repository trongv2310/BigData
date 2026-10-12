# backend/ (Claude)

FastAPI + uvicorn, chạy **trong WSL Ubuntu** (cùng Hadoop/Kafka/Spark để gọi được `localhost:9870`, `9092`, `4040`). Python 3.10; thư viện cần có đã sẵn trong WSL (xem `requirements.txt`).

## Chạy

```bash
# trong WSL; HDFS phải đang chạy (bash ~/start-4nodes.sh)
cd /mnt/c/Users/Trong/orca/workspaces/co-hcmc-dashboard/feat-backend/backend    # hoặc đường dẫn repo của bạn
python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000
# bật cửa sổ lệnh (chạy lệnh thật!): BACKEND_ALLOW_WRITES=1 python3 -m uvicorn ... rồi chép token in ra console
# giả lập luồng khi chưa có producer (để dev giao diện): STREAM_MODE=replay python3 -m uvicorn ...
```

Biến môi trường: xem `config.py` và `.env.example` (uvicorn đọc bằng `--env-file ../.env`).

## Kiểm tra

```bash
cd backend && python3 -W ignore -m unittest discover tests     # test hợp đồng, không cần Hadoop (dùng NameNode giả)
python3 backend/tools/check_live.py http://127.0.0.1:8000       # backend đang chạy: so từng route với openapi.yaml và với mock
```

## Nguyên tắc

- Chỉ đọc. Client HDFS chỉ gọi GET và các op đọc của WebHDFS; mã ghi (sau này) phải qua `app/writeguard.py`.
- Mọi route trả đúng schema trong `docs/contract/openapi.yaml`; lỗi theo schema `Error`.
- Bản ghi của luồng `/stream_input` được đổi sang `AqiReading` ở `app/reading.py` (ngưỡng trạng thái là quyết định D3, tạm).

## Nguồn dữ liệu từng route

| Route | Nguồn |
|---|---|
| `/api/services` | gọi thật: NameNode JMX, cổng HTTP DataNode, YARN REST, TCP Kafka, Spark UI `:4040-4043`, Airflow `/health` |
| `/api/hdfs/*` | NameNode JMX (`/jmx`) và WebHDFS (`/webhdfs/v1`) |
| `/api/kafka/topics` | kafka-python (admin + offset), chỉ đọc |
| `/api/spark/applications` | Spark REST `:4040/api/v1` |
| `/api/aqi/*` | bộ Kaggle (D4), cộng 7 giờ vì cột `datetime` gốc là UTC |
| `/api/predict` | heuristic từ bộ Kaggle (`app/predict.py`), không phải ML |
| `/api/sql` | SparkSQL `local[2]` trong tiến trình backend, view `aqi` từ HDFS `/raw/Dataset.csv` |
| `/ws/terminal` | pty bash; TẮT mặc định, bật bằng `BACKEND_ALLOW_WRITES=1`; token in ra console lúc khởi động |
| `/api/flows` | định nghĩa trong `app/flows.py` (trùng mock) |
| `/ws/stream` | `live`: file mới trong `/stream_input`, đổi trạng thái dịch vụ, `kafka.stats`, `hdfs.op` + `flow.step` từ log NameNode; `replay`: phát lại `mock/stream-events.ndjson` (giả lập) |
