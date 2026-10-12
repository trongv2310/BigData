# backend/ (Claude)

Dự kiến: Python 3.10+, FastAPI + uvicorn, chạy trong WSL cùng Hadoop/Kafka/Spark để gọi được `localhost:9870`, `9092`, `4040`.

Nguồn dữ liệu cho từng route (xem `docs/contract/openapi.yaml`):
- `/api/hdfs/*`: NameNode JMX (`/jmx`) và WebHDFS (`/webhdfs/v1`).
- `/api/kafka/*`: kafka-python admin hoặc `kafka-topics.sh`/`kafka-consumer-groups.sh`.
- `/api/spark/*`: Spark REST `http://localhost:4040/api/v1`.
- `/api/aqi/*`: đọc `/raw/Dataset.csv` (hoặc file Parquet đã xử lý), nhớ cộng 7 giờ vì cột datetime gốc là UTC.
- `/ws/stream`, `/ws/terminal`: xem `docs/contract/events.md`.

Chưa có mã. Làm theo `docs/TASKS.md`, giai đoạn P1.
