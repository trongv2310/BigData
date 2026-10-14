# pipeline/

Pipeline 9 bước của giảng viên, đổi cho CO (bước 6 thay RFM và K-Means bằng phân cụm ngày + mô hình dự đoán CO). Chạy trong WSL, cần HDFS; Spark là `local[*]`.
Mỗi script ghi trạng thái và số liệu thật của chính nó vào `hdfs:///co/output/step_summary.json`; backend chỉ đọc file này để trả `GET /api/pipeline/steps`.

| Bước (id) | Script | Việc | Lệnh |
|---|---|---|---|
| 1 `prepare-hdfs`, 2 `upload-raw` | `01_prepare_upload.py` | tạo `/co/raw`, `/co/processed`, `/co/output`; upload `Dataset.csv` vào `/co/raw`, kiểm số dòng/cột/md5 | `python3 pipeline/01_prepare_upload.py` |
| 3 `read-spark` | `02_check_raw.py` | Spark đọc, kiểm schema, thiếu, giá trị lạ, trùng, thiếu giờ; ghi `/co/output/check_raw.json` | `spark-submit --driver-memory 1g pipeline/02_check_raw.py` |
| 4 `clean` | `04_clean.py` (+ `cleaning.py`) | UTC → +07, xử lý lỗi, ép kiểu, thêm `hour/month/dayofweek/year/after_2025_07_01`; ghi Parquet `/co/processed/aqi_clean` | `spark-submit --driver-memory 1g pipeline/04_clean.py` |
| 5 đến 9 | chưa có | Bước A3 trở đi | |

Quy tắc: không script nào điền số thay thế; chạy lại an toàn (bước 1-2 không ghi đè file khác nội dung, bước 4 dừng nếu `/co/processed/aqi_clean` đã có trừ khi `--overwrite`).
Kiểm phép làm sạch: `backend/tests/test_a2.py` (bảng nhỏ có đủ loại lỗi, chạy bằng `cd backend && python3 -m unittest discover tests`).

## Dữ liệu `Dataset.csv` (đã kiểm ngày 14/10/2026)
35 064 dòng giờ liên tục từ 2022-04-01 00:00 đến 2026-03-31 23:00 UTC (không trùng, không thiếu giờ), 19 cột; giờ trong cột `datetime` là UTC, dạng `dd-MM-yy H:mm` (data_dictionary.csv ghi M/D/Y là sai).
Cột `hour/month/dayofweek` của file khớp UTC ở 100% dòng. Không ô thiếu; 34 ô `humidity_%` > 100 (100,28 đến 117,52, bốn chuỗi giờ liên tiếp). Dữ liệu đã qua tiền xử lý từ trước (o3 được điền ở 5 228 dòng `o3_was_missing=1`; 559 ô độ ẩm không nguyên dù từ điển ghi Integer, trông như nội suy).
