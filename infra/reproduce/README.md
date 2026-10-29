# Tái hiện môi trường (đã kiểm ngày 28/10/2026 trên máy nhóm; CHƯA thử trên môi trường sạch)

Phiên bản đã dùng: Ubuntu 22.04 (WSL2), OpenJDK 17.0.20.1, Hadoop 3.3.6, Kafka 2.13-4.1.2, PySpark 3.5.1 (Scala 2.12.18), Python 3.10.12, Airflow 3.3.2, FastAPI 0.136.3, uvicorn 0.54.0.

Cấu hình mẫu: `core-site.xml.sample`, `hdfs-site.xml.sample` (chép vào `~/hadoop/etc/hadoop/`, bỏ hậu tố `.sample`; đổi `/home/trong` theo máy). `start-4nodes.sh.sample` là bản sao script khởi động 1 NameNode + 4 DataNode trên một máy (cổng DataNode 9866/9876/9886/9896); nó không chứa bí mật.
Lưu ý replication = 1: mỗi block chỉ một bản, nên tắt WSL đột ngột có thể làm replica 0 byte (đã gặp 5 block checkpoint ngày 28/10/2026; xem MinhChung/BoSung/01_hdfs_safemode). Luôn chạy `stop-4nodes.sh` trước khi tắt WSL.

Thứ tự: 1) `bash ~/start-4nodes.sh`; kiểm `hdfs dfsadmin -safemode get` = OFF. 2) (tùy chọn) `bash ~/start-kafka.sh`. 3) `bash infra/airflow.sh install` rồi chạy
`airflow dags test hcmc_co_pipeline --conf '{"co_root": "/co_verify", "overwrite": false}'` (từ thư mục gốc repo, đặt HCMC_CO_REPO). 4) kiểm đầu ra: `hdfs dfs -ls /co_verify/output`, `step_summary.json`, `model_metrics.json`.
5) Backend: `cd backend && python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000`, đặt MODEL_*, PIPELINE_SUMMARY_PATH, ANALYSIS_* và HDFS_RAW_PATH về cùng vùng (ví dụ MinhChung/BoSung/04_dashboard_that/chay_backend_that.sh). 6) Giao diện: `cd frontend && npm install && VITE_API_BASE=http://localhost:8000 npm run dev`.
Khóa OpenWeather chỉ đặt qua biến môi trường OWM_API_KEY, không ghi vào repo.
