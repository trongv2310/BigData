# pipeline/

Pipeline 9 bước của giảng viên, đổi cho CO (bước 6 thay RFM và K-Means bằng phân cụm ngày + mô hình dự đoán CO). Chạy trong WSL, cần HDFS; Spark là `local[*]`.
Mỗi script ghi trạng thái và số liệu thật của chính nó vào `hdfs:///co/output/step_summary.json`; backend chỉ đọc file này để trả `GET /api/pipeline/steps`.

| Bước (id) | Script | Việc | Lệnh |
|---|---|---|---|
| 1 `prepare-hdfs`, 2 `upload-raw` | `01_prepare_upload.py` | tạo `/co/raw`, `/co/processed`, `/co/output`; upload `Dataset.csv` vào `/co/raw`, kiểm số dòng/cột/md5 | `python3 pipeline/01_prepare_upload.py` |
| 3 `read-spark` | `02_check_raw.py` | Spark đọc, kiểm schema, thiếu, giá trị lạ, trùng, thiếu giờ; ghi `/co/output/check_raw.json` | `spark-submit --driver-memory 1g pipeline/02_check_raw.py` |
| 4 `clean` | `04_clean.py` (+ `cleaning.py`) | UTC → +07, xử lý lỗi, ép kiểu, thêm `hour/month/dayofweek/year/after_2025_07_01`; ghi Parquet `/co/processed/aqi_clean` | `spark-submit --driver-memory 1g pipeline/04_clean.py` |
| 5 `spark-sql` | `05_sql_analysis.py` (+ `analysis.py`) | CO theo giờ/tháng, trước/sau 01/07/2025 (thô và cùng tháng lịch), diễn biến theo ngày quanh điểm gãy, tương quan; ghi `/co/output/sql_analysis.json` | `spark-submit --driver-memory 1g pipeline/05_sql_analysis.py` |
| 6a (của `cluster-and-model`) | `06a_cluster_days.py` (+ `day_clusters.py`) | K-Means MLlib trên hình dạng 24 giờ của từng ngày; k chọn bằng silhouette lớn nhất (k=2..8); ghi `/co/output/day_clusters` (Parquet) và `day_clusters.json`. Bước 6 ở trạng thái `partial` cho tới khi 6b xong | `spark-submit --driver-memory 1g pipeline/06a_cluster_days.py` |
| 6a (độ ổn định) | `06a_cluster_stability.py` (+ `stability.py`) | K-Means k=2..8 với 6 seed; silhouette và chỉ số Rand điều chỉnh (ARI) giữa các lần chạy cho k=2 và k=3; chi tiết k=3; ghi `/co/output/day_clusters_stability.json` và `day_clusters_k3` | `spark-submit --driver-memory 1g pipeline/06a_cluster_stability.py` |
| 6b (mô hình) và 7 `save-results` | `06b_train.py` (+ `changepoint.py`, `modeling.py`) | xác định ranh giới chuyển tiếp theo dữ liệu (khử mùa vụ rồi khớp dốc), 3 phương án chia (chính: sau chuyển tiếp; chia 80/20 gốc; toàn bộ có biến chế độ), 3 mô hình, 2 mốc; lưu `/co/output/model_best`, `model_metrics.json`, `model_test_predictions`, `model_inputs_reference.json` | `spark-submit --driver-memory 2g pipeline/06b_train.py` |
| 8 `kafka-ingest` | `08_check_stream.py` | KIỂM job Spark streaming (`infra/spark_kafka_consumer.py`): đọc `/live/co_stream`, sai số dự đoán trên luồng theo nguồn, đối chiếu với dự đoán offline; ghi bước 8 (`failed` nếu chưa có dòng nào được dự đoán) | `spark-submit --driver-memory 1g pipeline/08_check_stream.py` |
| 9 `airflow-dag` | `09_record_dag.py` (do DAG gọi) + `infra/airflow_dags/hcmc_co_pipeline.py` | DAG Airflow nối các bước trên bằng spark-submit; tác vụ cuối ghi bước 9 (`done` khi mọi tác vụ thành công, `failed` khi có tác vụ lỗi) | `airflow dags test hcmc_co_pipeline <ngày> --conf '{"co_root":"/co_airflow_test","overwrite":true}'` (xem `infra/README.md`) |

Thư mục gốc HDFS là `/co`, đổi bằng biến môi trường `CO_ROOT` (mọi script đọc qua `common.CO_ROOT`); DAG truyền tham số `co_root` vào đó để chạy thử mà không đụng kết quả đã giao ở `/co`.

Quy tắc: không script nào điền số thay thế; chạy lại an toàn (bước 1-2 không ghi đè file khác nội dung, bước 4 dừng nếu `/co/processed/aqi_clean` đã có trừ khi `--overwrite`).
Kiểm phép làm sạch: `backend/tests/test_a2.py` (bảng nhỏ có đủ loại lỗi, chạy bằng `cd backend && python3 -m unittest discover tests`).

## Dữ liệu `Dataset.csv` (đã kiểm ngày 14/10/2026)
35 064 dòng giờ liên tục từ 2022-04-01 00:00 đến 2026-03-31 23:00 UTC (không trùng, không thiếu giờ), 19 cột; giờ trong cột `datetime` là UTC, dạng `dd-MM-yy H:mm` (data_dictionary.csv ghi M/D/Y là sai).
Cột `hour/month/dayofweek` của file khớp UTC ở 100% dòng. Không ô thiếu; 34 ô `humidity_%` > 100 (100,28 đến 117,52, bốn chuỗi giờ liên tiếp). Dữ liệu đã qua tiền xử lý từ trước (o3 được điền ở 5 228 dòng `o3_was_missing=1`; 559 ô độ ẩm không nguyên dù từ điển ghi Integer, trông như nội suy).
