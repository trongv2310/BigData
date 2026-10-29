# Giám sát và dự đoán nồng độ CO tại TP. Hồ Chí Minh

Đồ án học phần Big Data (Nhóm 1): pipeline dữ liệu và bảng điều khiển cho chuỗi nồng độ Carbon Monoxide (CO) theo giờ tại TP. Hồ Chí Minh, giai đoạn 04/2022 – 03/2026.

Hệ thống lưu dữ liệu trên **HDFS**, làm sạch và phân tích bằng **Spark / Spark SQL**, huấn luyện mô hình hồi quy bằng **Spark MLlib**, nhận dữ liệu mới qua **Kafka** và **Spark Structured Streaming**, điều phối bằng **Airflow**, và hiển thị bằng dashboard **React** nối **FastAPI**.

**Video demo:** [Google Drive](https://drive.google.com/drive/folders/1zo35LY3iG5_KXxUbwpKRqNwcox_IBbDG?usp=sharing)

## Kiến trúc

```
CSV lịch sử ──▶ HDFS /co/raw ──▶ Spark (kiểm tra, làm sạch) ──▶ /co/processed (Parquet)
                                        │
                                        ├─▶ Spark SQL ─▶ /co/output/sql_analysis.json
                                        ├─▶ K-Means phân cụm ngày ─▶ day_clusters*.json
                                        └─▶ MLlib (Linear, Random Forest, GBT) ─▶ model_best, model_metrics.json

OpenWeather / phát lại bộ dữ liệu ─▶ Kafka (hcmc_co_stream) ─▶ Spark Structured Streaming
        ├─▶ HDFS /live/co_stream (Parquet)
        └─▶ Kafka hcmc_co_enriched (có dự đoán) ─▶ FastAPI ─▶ REST + WebSocket ─▶ Dashboard React

Airflow DAG hcmc_co_pipeline điều phối các bước batch phía trên.
```

Pipeline batch gồm 9 bước hiển thị ở tab **Pipeline** của dashboard: chuẩn bị HDFS, tải dữ liệu lên, Spark đọc, làm sạch, Spark SQL, phân cụm và mô hình, lưu kết quả, Kafka, Airflow.

## Công nghệ

| Thành phần | Phiên bản đã dùng |
|---|---|
| Hệ điều hành | Windows 11, WSL2 Ubuntu 22.04 |
| Java | OpenJDK 17 |
| Hadoop (HDFS) | 3.3.6, một NameNode và bốn DataNode trên một máy |
| Spark / PySpark | 3.5.1, chế độ `local[*]` |
| Kafka | 4.1.x (KRaft), cổng 9092 |
| Airflow | 3.3.2 |
| Backend | Python 3.10, FastAPI, uvicorn |
| Frontend | React, Vite, TypeScript |

## Cấu trúc thư mục

```
backend/        FastAPI: REST, WebSocket, đọc HDFS/Kafka/Spark
frontend/       Dashboard React + Vite + TypeScript
pipeline/       Các bước batch (01 đến 09): tải lên, kiểm tra, làm sạch, SQL, phân cụm, huấn luyện
infra/          Producer, consumer Spark Streaming, Airflow DAG, script khởi động, cấu hình mẫu (infra/reproduce)
docs/contract/  Hợp đồng API: openapi.yaml, events.md, validate_contract.py
mock/           Máy chủ dữ liệu mẫu đúng hợp đồng (cổng 8010)
storyboard/     Kịch bản các video giải thích
video/          Mã dựng video (Remotion)
```

## Chạy nhanh bằng dữ liệu mẫu (không cần Hadoop)

Cách này đủ để xem giao diện và các màn hình. Dữ liệu là **dữ liệu mẫu của mock server**, không phải kết quả pipeline thật.

Yêu cầu: Python 3.10+, Node.js bản LTS mới (20 trở lên; frontend dùng Vite 8).

```bash
# Cửa sổ 1 – mock API tại http://127.0.0.1:8010
pip install pyyaml "jsonschema>=4.18"
python docs/contract/validate_contract.py        # kiểm tra hợp đồng, phải báo OK
python mock/serve.py
```

```bash
# Cửa sổ 2 – giao diện tại http://localhost:5173
cd frontend
npm ci
npm run dev -- --port 5173
```

## Chạy hệ thống thật (WSL2)

Cần HDFS, Kafka, Spark và Airflow đã cài trong WSL. Các script trong `infra/` dùng đường dẫn của máy nhóm (`/home/trong`, `/mnt/f/...`): máy khác phải sửa lại cho phù hợp. Cấu hình HDFS mẫu và hướng dẫn dựng môi trường nằm ở `infra/reproduce/README.md`.

**1. Khởi động hạ tầng và backend**

```bash
bash ~/start-4nodes.sh                 # NameNode + 4 DataNode (bản mẫu: infra/reproduce/start-4nodes.sh.sample)
hdfs dfsadmin -safemode get            # phải là OFF trước khi ghi
export OWM_API_KEY=...                 # tùy chọn, chỉ trong shell, không ghi vào repo
bash infra/start_all.sh                # HDFS, Kafka, producer, Spark consumer, backend (cổng 8000)
```

**2. Chạy frontend (PowerShell, thư mục `frontend`)**

```powershell
npm ci
$env:VITE_API_BASE="http://localhost:8000"
$env:VITE_STREAM="ws"
npm run dev -- --port 5173 --strictPort
```

Nếu không đặt hai biến trên, giao diện mặc định nối vào mock server cổng 8010.

**3. Chạy pipeline batch**

Chọn một trong hai cách, không chạy đồng thời vào cùng `CO_ROOT`.

```bash
# Cách A: Airflow (DAG thật hcmc_co_pipeline, tạm dừng khi tạo, không có lịch)
export HCMC_CO_REPO=$(pwd)
bash infra/airflow.sh install
airflow dags test hcmc_co_pipeline --conf '{"co_root": "/co_demo", "overwrite": false}'
```

```bash
# Cách B: từng bước
export CO_ROOT=/co_demo
python3 pipeline/01_prepare_upload.py --local /duong/dan/hcmc_aqi_dataset_after_preprocess_final.csv
spark-submit --driver-memory 1g pipeline/02_check_raw.py
spark-submit --driver-memory 1g pipeline/04_clean.py
spark-submit --driver-memory 1g pipeline/05_sql_analysis.py
spark-submit --driver-memory 1g pipeline/06a_cluster_days.py
spark-submit --driver-memory 1g pipeline/06a_cluster_stability.py
spark-submit --driver-memory 2g pipeline/06b_train.py
hdfs dfs -cat /co_demo/output/model_metrics.json
```

Đầu ra đã tồn tại thì pipeline dừng (mã 2) thay vì ghi đè; chọn `CO_ROOT` mới hoặc sao lưu trước khi dùng `--overwrite`. Khi đổi `CO_ROOT`, backend và consumer streaming cũng cần trỏ về cùng vùng (biến `MODEL_*`, `PIPELINE_SUMMARY_PATH`, `ANALYSIS_*`, `HDFS_RAW_PATH`; xem `backend/README.md`).

**4. Streaming**

```bash
spark-submit --driver-memory 1g \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  infra/spark_kafka_consumer.py \
  --model-dir hdfs://localhost:9000/co/output/model_best \
  --model-reference hdfs://localhost:9000/co/output/model_inputs_reference.json
```

## Cổng dịch vụ

| Thành phần | Cổng |
|---|---|
| Backend (FastAPI) | 8000 |
| Mock server | 8010 |
| Frontend (Vite) | 5173 |
| NameNode UI / RPC | 9870 / 9000 |
| DataNode | 9864 |
| Kafka | 9092 |
| Airflow UI | 8080 |

## Kiểm thử

```bash
python docs/contract/validate_contract.py        # hợp đồng API và mock
cd backend && python3 -W ignore -m unittest discover tests
cd frontend && npm run lint && npm run build
```

## Lưu ý về dữ liệu

- Cột `datetime` của bộ dữ liệu lịch sử là **UTC**; giao diện và API dùng giờ Việt Nam (+07:00).
- CO trung bình giảm mạnh sau **01/07/2025** (từ 1.343,80 xuống 303,33 µg/m³). Nguyên nhân chưa xác định; huấn luyện dùng riêng giai đoạn sau mốc này.
- Luồng `replay-dataset` là **phát lại dữ liệu lịch sử để mô phỏng**; nhãn "trạm" chỉ là nhãn gán luân phiên (`isSimulatedStation: true`), không phải trạm đo thật. Luồng `live-openweather` là nguồn khác và không được trộn vào cùng một đường biểu đồ.
- Ngưỡng `SAFE / WARNING / DANGER` theo thang CO của OpenWeather (WARNING từ 4.400 µg/m³, DANGER từ 12.400 µg/m³); đây không phải quy chuẩn của Việt Nam.
- Tab "Báo cáo Giám sát CO" là trang minh họa với số trạm và điểm nóng cố định trong mã, không lấy từ pipeline.
- Dữ liệu lịch sử chỉ khoảng 3 MB và hệ thống chạy trên một máy, nên đồ án chưa đánh giá hiệu năng ở quy mô lớn.

## Bảo mật

- Không đưa khóa API vào repo: `OWM_API_KEY` chỉ đặt qua biến môi trường; `.env` và `own_key.txt` đã nằm trong `.gitignore`; chỉ `.env.example` (giá trị trống) được commit.
- Backend mặc định **chỉ đọc**. Cửa sổ lệnh trên dashboard tắt mặc định; bật bằng `BACKEND_ALLOW_WRITES=1`, chỉ nhận kết nối từ máy cục bộ (127.0.0.1), kiểm tra Origin và cần token in ra console khi backend khởi động.

## Tài liệu

- Hợp đồng API: `docs/contract/openapi.yaml`, `docs/contract/events.md`
- Hướng dẫn sử dụng: `docs/HUONG_DAN_SU_DUNG.docx`
- Kịch bản demo: `docs/DEMO.md`
- Backend: `backend/README.md` · Pipeline: `pipeline/README.md` · Hạ tầng: `infra/README.md`
