# backend/ (Claude)

FastAPI + uvicorn, chạy **trong WSL Ubuntu** (cùng Hadoop/Kafka/Spark để gọi được `localhost:9870`, `9092`, `4040`). Python 3.10; thư viện cần có đã sẵn trong WSL (xem `requirements.txt`).

## Chạy

```bash
# trong WSL; HDFS phải đang chạy (bash ~/start-4nodes.sh)
cd /mnt/c/Users/Trong/orca/workspaces/co-hcmc-dashboard/feat-backend/backend    # hoặc đường dẫn repo của bạn
python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000
# bật cửa sổ lệnh (chạy lệnh thật!): BACKEND_ALLOW_WRITES=1 python3 -m uvicorn ... rồi chép token in ra console
# giả lập luồng khi chưa có producer (để dev giao diện): STREAM_MODE=replay python3 -m uvicorn ...
# dữ liệu SỐNG qua Kafka (producer: infra/live_producer.py): STREAM_SOURCE=kafka python3 -m uvicorn ...   (mặc định hdfs)
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
| `/api/aqi/*` | bộ Kaggle (D4), cộng 7 giờ vì cột `datetime` gốc là UTC; riêng `/api/aqi/latest[?source=]` (đọc `READING_TOPIC`, MẶC ĐỊNH `hcmc_co_enriched` đã có `coPredicted`/`modelVersion`/`coIsForecast` do Spark thêm nên Spark phải chạy; đường lui `READING_TOPIC=hcmc_co_stream` là topic thô, xem `infra/README.md`) lấy tin mới nhất của luồng (hoặc của đúng một nguồn, bộ nhớ đệm theo nguồn trong `app/kafka_stream.py`) theo `STREAM_SOURCE` (Kafka và/hoặc `/stream_input`), không có thì dòng cuối bộ Kaggle (`source: replay-dataset`) |
| `/api/predict` | có mô hình đã huấn luyện thì dùng mô hình (`kind=ml`: `app/ml_predict.py`, `app/ml_engine.py`, nạp PipelineModel từ HDFS bằng JVM Spark dùng chung với `/api/sql`; đầu vào bỏ trống điền bằng trung vị huấn luyện và báo ở `filledInputs`); chưa có mô hình thì heuristic từ bộ Kaggle (`app/predict.py`), không phải ML |
| `/api/replay/status`, `POST /api/replay/control` | luồng PHÁT LẠI (mô phỏng): đọc nhịp tim `status.json`, ghi lệnh `control.json` trong `REPLAY_STATE_DIR` (không qua HDFS/Kafka/Spark; `REPLAY_CONTROL=0` để chỉ xem); producer: `infra/replay_kafka_producer.py` (`app/replay.py`) |
| `/api/pipeline/steps` | 9 bước pipeline, đọc `/co/output/step_summary.json` trên HDFS (chỉ đọc; các script trong `pipeline/` ghi file này). Chưa có file thì cả 9 bước `notStarted`; HDFS tắt hoặc file hỏng thì 503, không đoán (`app/pipeline_steps.py`) |
| `/api/analysis/sql`, `/api/analysis/day-clusters` | kết quả bước 5 và 6a: trả nguyên văn `/co/output/sql_analysis.json`, `day_clusters.json` (+ `stability` từ `day_clusters_stability.json`, null nếu chưa chạy); chưa có thì 404 `ANALYSIS_NOT_READY`, HDFS tắt hoặc tệp hỏng thì 503 (`app/routes/analysis.py`) |
| `/api/model/metrics` | chỉ số mô hình dự đoán CO: trả nguyên văn `/co/output/model_metrics.json` (`app/routes/model.py`); chưa huấn luyện thì 404 `MODEL_NOT_TRAINED`, HDFS tắt hoặc tệp hỏng thì 503 |
| `/api/sql` | SparkSQL `local[2]` trong tiến trình backend, view `aqi` từ HDFS `/raw/Dataset.csv` |
| `/ws/terminal` | pty bash; TẮT mặc định, bật bằng `BACKEND_ALLOW_WRITES=1`; token in ra console lúc khởi động |
| `/api/flows` | định nghĩa trong `app/flows.py` (trùng mock) |
| `/ws/stream` | `live`: file mới trong `/stream_input`, đổi trạng thái dịch vụ, `kafka.stats`, `hdfs.op` + `flow.step` từ log NameNode, `spark.progress` suy ra từ Spark REST (job/stage của từng micro-batch, xem `app/spark_progress.py`); `replay`: phát lại `mock/stream-events.ndjson` (giả lập) |
