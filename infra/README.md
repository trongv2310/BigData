# infra/

Môi trường hiện có của dự án (không do repo này cài):

| Thứ | Vị trí / địa chỉ |
|---|---|
| WSL Ubuntu | đĩa ảo `WSL_Ubuntu/ext4.vhdx` |
| Hadoop 3.3.6 | `/home/trong/hadoop`, HDFS `hdfs://localhost:9000`, NameNode UI `:9870`, DataNode `:9864`, YARN RM `:8088` |
| Kafka | `/home/trong/kafka`, `localhost:9092`, topic `hcmc_co_stream` (`start-kafka.sh`, `stop-kafka.sh`) |
| Spark | PySpark `local[*]` (`spark_streaming_consumer.py`) |
| Airflow | DAG `hcmc_co_pipeline` |
| HDFS | `/stream_input` (file JSON của luồng), `/raw/Dataset.csv` |

Quy ước: các script pipeline cũ nằm ở thư mục cha `BigData/`. Repo này **sao chép** khi cần, không di chuyển, để đồ án đang nộp không bị hỏng.

## Dữ liệu SỐNG qua Kafka (D1 phương án b)

```
Open-Meteo API ──▶ infra/live_producer.py ──▶ Kafka hcmc_co_stream ─┬─▶ backend (STREAM_SOURCE=kafka) ──▶ /ws/stream, /api/aqi/latest ──▶ dashboard
                                                                     └─▶ infra/spark_kafka_consumer.py ──▶ HDFS /live/co_stream (Parquet)
```

- Chạy: `bash infra/live_stack.sh start` (producer + Spark consumer), backend: `STREAM_SOURCE=kafka python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000`. Dừng: `bash infra/live_stack.sh stop`. Xem: `bash infra/live_stack.sh status`.
- **Tần suất thật của nguồn:** Open-Meteo cập nhật CO mỗi 3600 giây và thời tiết mỗi 900 giây (trường `interval` của API), nên một bản ghi mới xuất hiện tối đa 15 phút một lần. Producer hỏi API mỗi 60 giây nhưng chỉ gửi khi thời điểm quan sát đổi. Không có số liệu nào được điền giả khi API thiếu.
- **Nguồn dữ liệu:** CO của Open-Meteo là số của mô hình chất lượng không khí (CAMS), không phải cảm biến ở một trạm; giá trị khác hẳn bộ Kaggle/OpenWeather (Open-Meteo trả 2508 µg/m³ lúc 23:00 ngày 12/10/2026; trung bình bộ Kaggle từ 07/2025 là ~300, bộ này kết thúc ở 03/2026, không có số cùng thời điểm để so), nên `source: live-open-meteo` luôn tách khỏi `replay-dataset` (AGENTS.md mục 5) và ngưỡng 500/1000 (D3, tạm) gắn `DANGER` cho các giá trị này.
- Spark cần connector Kafka: lần đầu `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` tải ~10 MB từ Maven Central vào `~/.ivy2`. Kết quả ghi vào `hdfs://localhost:9000/live/co_stream` (checkpoint `/live/_checkpoints/co_stream`); khởi động lại thì đọc tiếp từ offset đã xử lý.
- Script cũ ở thư mục cha (`sensor_producer_*.py`, `spark_streaming_consumer.py`) không bị sửa và vẫn chạy được với `STREAM_SOURCE=hdfs`.

## Chạy tay các thành phần (đã chạy thật 2026-10-12, trong WSL)

| Thành phần | Lệnh | Ghi chú |
|---|---|---|
| Kafka | `JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PATH=$JAVA_HOME/bin:$PATH /home/trong/kafka/bin/kafka-server-start.sh -daemon /home/trong/kafka/config/server.properties` | Cần Java 17 chỉ cho lệnh Kafka. Nếu `/tmp/kraft-combined-logs` mất (WSL khởi động lại) phải `kafka-storage.sh format --standalone -t <uuid> -c config/server.properties` rồi tạo lại topic `hcmc_co_stream`. |
| YARN | `setsid nohup ~/hadoop/bin/yarn --daemon start resourcemanager` (NodeManager do `start-yarn.sh`) | `start-yarn.sh` trong phiên ngắn làm ResourceManager bị `SIGHUP`. |
| Backend | `ALLOWED_ORIGINS=... nohup python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000 &` | Thêm cổng frontend vào `ALLOWED_ORIGINS` nếu không phải 5173. |
| Producer mô phỏng | `python3 infra/replay_producer.py --count 300 --interval 5` | Ghi `/stream_input/replay_<epoch>_<n>.json`; dọn bằng `hdfs dfs -rm '/stream_input/replay_*.json'`. MÔ PHỎNG, không phải đo thật. |
| Spark Streaming | `cd BigData && spark-submit --driver-memory 640m spark_streaming_consumer.py` | Đọc `/stream_input`, in ra console (không ghi HDFS), UI `:4040`. Mỗi micro-batch một file. |
| Frontend | node của Windows (WSL không có node): `VITE_API_BASE=http://localhost:8000 VITE_STREAM=ws npm run dev` | |

Bộ nhớ: máy WSL có ~7,7 GB; khi chạy HDFS (5 JVM), YARN, Kafka và Spark Streaming thì chỉ còn ~0,9 GB khả dụng, chưa đủ chỗ cho Airflow.

Docker: chưa thấy trên máy, nên chưa có `docker-compose`. TODO nếu sau này cần.
