# Kịch bản demo (khoảng 8 phút)

Ảnh chụp (13/10/2026, backend thật) nằm trong `docs/demo/`. Kịch bản này chỉ nói những gì đã chạy và quan sát được; phần nào là suy ra hoặc mô phỏng thì có ghi rõ để nói đúng khi trình bày.

## 1. Chuẩn bị (làm trước, khoảng 2 phút; đã thử khởi động nguội ngày 13/10/2026)

**Cách nhanh (một lệnh).** Trong WSL, từ thư mục repo: `bash infra/start_all.sh`. Script chạy đúng thứ tự, kiểm tra từng bước (HDFS đủ 4 DataNode và hết safe mode, Kafka có topic, Airflow trả lời, Spark consumer chạy, backend trả lời) rồi in bảng dịch vụ và lệnh frontend. Khởi động nguội (sau `wsl --shutdown`) mất khoảng 1 phút 20 giây. Biến tùy chọn: `WITH_YARN=1` (bật YARN), `WITH_AIRFLOW=0`, `BACKEND_ALLOW_WRITES=1` (bật cửa sổ lệnh, script in token). Dừng sạch: `bash infra/stop_all.sh` (thêm `KEEP_INFRA=1` để giữ HDFS và Kafka).

Frontend (node của Windows, mở PowerShell): `cd frontend; $env:VITE_API_BASE="http://localhost:8000"; $env:VITE_STREAM="ws"; npm run dev -- --port 5173 --strictPort`, rồi mở `http://localhost:5173`. Nếu cổng 5173 đang bận (ví dụ một Vite chế độ mock đang chạy) thì lệnh báo lỗi rõ ràng; đổi sang `--port 5174` hoặc `--port 5175` (backend của `start_all.sh` cho phép cả ba cổng). Không bỏ `--strictPort`: nếu Vite tự nhảy sang cổng khác mà backend không cho phép Origin đó thì trang báo "Lỗi kết nối REST API: failed to fetch".

**Từng bước thủ công** (chi tiết và lý do ở `infra/README.md`):

1. HDFS: `bash ~/start-4nodes.sh`, rồi chờ `hdfs dfsadmin -safemode wait`.
2. Kafka (Java 17): `JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PATH=$JAVA_HOME/bin:$PATH /home/trong/kafka/bin/kafka-server-start.sh -daemon /home/trong/kafka/config/server.properties`. Dữ liệu Kafka nằm ở `/home/trong/kafka-data`, nên topic `hcmc_co_stream` còn sau khi khởi động lại WSL.
3. YARN (đang TẮT từ 13/10/2026 để lấy RAM; chỉ bật nếu muốn thẻ ResourceManager, NodeManager hiện UP, khi đó còn rất ít RAM): `WITH_YARN=1 bash infra/start_all.sh` hoặc hai lệnh `~/hadoop/bin/yarn --daemon start resourcemanager` và `... start nodemanager`.
4. Airflow (nếu muốn thẻ Airflow UP): `bash infra/airflow.sh start`.
5. Dữ liệu sống: `bash infra/live_stack.sh start` (producer Open-Meteo và Spark consumer). Producer khởi động lại không gửi trùng quan sát đã có trong topic.
6. Backend: `cd backend && STREAM_SOURCE=kafka FLOWS_EXTRA=1 ALLOWED_ORIGINS=http://localhost:5173,http://localhost:5174,http://localhost:5175 python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000` (đặt đúng cổng frontend vào `ALLOWED_ORIGINS`).

Lưu ý bộ nhớ: máy chỉ có khoảng 7,7 GB. Với YARN tắt thì còn khoảng 2 GB trống (đủ cho truy vấn SparkSQL, đã chạy từ giao diện ngày 13/10); với YARN bật chỉ còn khoảng 0,3 GB, truy vấn SparkSQL (cần thêm khoảng 1 GB) có thể làm hệ thống kill tiến trình.

Kiểm trước giờ demo: góc phải trên báo "WebSocket Kết nối"; Tổng quan hiện 6/8 dịch vụ UP khi YARN tắt (ResourceManager DOWN, NodeManager UNKNOWN); 8/8 nếu bật lại YARN.

## 2. Kịch bản

| # | Tab | Làm gì | Nói gì | Ảnh |
|---|---|---|---|---|
| 1 | Tổng quan | Chỉ số CO, tuổi dữ liệu, trạng thái cụm | Dữ liệu đi từ Open-Meteo qua Kafka. Đây là số của mô hình khí quyển (CAMS), cập nhật tối đa 15 phút một lần (CO mỗi giờ), không phải cảm biến từng giây. Nhãn "trạm mô phỏng" là có chủ ý. Mức cảnh báo theo thang CO của OpenWeather (WARNING từ 4400, DANGER từ 12400 µg/m³, quyết định D3), nên các số hiện tại đều ở mức SAFE; đây không phải quy chuẩn pháp lý của Việt Nam. Ảnh `01-tong-quan.jpg` chụp sau khi đổi ngưỡng và tắt YARN (nên ResourceManager DOWN, NodeManager UNKNOWN). | `01-tong-quan.jpg` |
| 2 | HDFS | 4 DataNode, số file, block, duyệt `/live` | Cụm HDFS thật (4 DataNode trên một máy), số đọc từ NameNode. | `02-hdfs.jpg` |
| 3 | Kafka | Topic `hcmc_co_stream`, `spark:co_stream` | Lag của Spark lấy từ checkpoint trên HDFS, không phải consumer group thật (ghi ngay trên giao diện). | `03-kafka.jpg` |
| 4 | Spark | Ứng dụng `HCMC_CO_Kafka_Streaming` | Spark Structured Streaming đọc Kafka, ghi Parquet vào `/live/co_stream`. Tiến độ micro-batch là ước tính suy ra từ Spark REST. Thẻ tiến trình chỉ hiện sau khi có batch mới kể từ lúc mở trang. | `04-spark.jpg` |
| 5 | CO & AQI | Biểu đồ tháng/ngày/giờ | Bộ Kaggle (OpenWeather), có điểm gãy 01/07/2025 (CO giảm mạnh) nên biểu đồ tách làm hai đoạn, không nối mượt. Khác nguồn với dữ liệu sống. | `05-co-aqi.jpg` |
| 6 | Sơ đồ luồng | Chọn "Kafka nhận bản ghi", rồi "Spark Structured Streaming", chờ quan sát mới | Khi producer gửi bản ghi mới: bước ACK kèm offset sáng (quan sát thật), sau đó micro-batch của Spark sáng 4 bước (suy ra từ việc job thành công). Chọn luồng HDFS để thấy ghi thật của Spark xuống `/live`. | `06a-luong-kafka-produce.jpg`, `06b-luong-spark-streaming.jpg` (chụp lúc offset 34, 07:30 ngày 13/10) |
| 7 | SparkSQL | Chọn truy vấn mẫu (ví dụ bảng `live`), chạy | Bảng `aqi` (Kaggle) và `live` (Spark ghi từ Kafka) khác nguồn, không trộn. Lần đầu mất khoảng 10 giây vì khởi động phiên Spark. | `08-sparksql-live.jpg` |
| 8 | Dự đoán CO | Chạy dự đoán | Đây là heuristic thống kê, không phải mô hình ML: nhiệt độ, gió, tháng chưa dùng vì hồi quy cho R² gần 0. Điểm cần nhấn là khoảng 10%-90%, không phải một con số. | `07-du-doan-ket-qua.jpg` |
| 9 | Cửa sổ lệnh | Chạy backend với `BACKEND_ALLOW_WRITES=1`, dán token (in ở console backend), kết nối, gõ lệnh chỉ đọc như `hdfs dfs -ls /raw` | Tắt mặc định vì shell ghi được lên HDFS/Kafka/Spark. Chỉ nhận Origin trong `ALLOWED_ORIGINS` (mặc định localhost:5173), token đổi mỗi lần khởi động backend. Không có ảnh trong repo vì đầu ra shell chứa PATH Windows của máy. Sau demo khởi động lại backend không có cờ này. | |

Nếu quan sát mới chưa tới lúc demo: Open-Meteo đổi thời tiết lúc :00, :15, :30, :45 (producer gửi trong khoảng 1 phút sau mốc đó). Có thể dùng `python3 infra/replay_producer.py --count 20 --interval 3` để thấy luồng HDFS sáng, nhưng phải nói rõ đó là dữ liệu phát lại, không phải dữ liệu sống.

## 3. Kiểm tra cuối đã làm (13/10/2026)

- Backend: 111 test qua, validator hợp đồng OK (P12).
- Frontend: `npm run lint` và `npm run build` sạch (PR #11).
- Trình duyệt, backend thật: các tab Tổng quan, HDFS, Kafka, Spark, CO & AQI, Sơ đồ luồng, SparkSQL, Dự đoán CO, Cửa sổ lệnh mở được; console không báo lỗi trong các lần xem.
- Luồng sống: quan sát 06:00 ngày 13/10 (offset 28) đi qua Kafka, backend, Spark, HDFS, và làm sáng `kafka-produce`, `spark-streaming`.
- Thu nhỏ 390 px (khung iframe, vì cửa sổ trình duyệt không thu nhỏ được): 8/9 tab không tràn ngang. Tab CO & AQI tràn ngang 432 px do dòng "Nguồn: kaggle:hcmc_aqi_dataset_after_preprocess_final.csv ..." không xuống dòng (`09-mobile-390px-co-aqi-tran-ngang.jpg`); đã ghi cho frontend trong `docs/TASKS.md`.

## 4. Chưa làm hoặc còn hạn chế

- Cửa sổ lệnh đã thử trên giao diện thật ngày 13/10/2026 (kết nối được, `echo`, `whoami`, `hdfs dfs -ls /raw` chạy đúng). Mỗi lần mở shell tương tác in một lỗi cú pháp từ `/etc/bash.bashrc` dòng 74 (PATH Windows có dấu ngoặc đơn, lỗi của cấu hình WSL, không phải của dự án); lệnh vẫn chạy bình thường.
- Airflow đã bật (13/10/2026) nhưng DAG `hcmc_co_pipeline` chỉ là DAG mẫu của bạn (số liệu cố định), đang tạm dừng, không nối với luồng Kafka/Spark thật.
- D2, D3, D4 đã chốt ngày 13/10/2026 (xem `docs/TASKS.md`). Hệ quả cần nói thật: với ngưỡng OpenWeather (WARNING từ 4400, DANGER từ 12400 µg/m³), mọi số đã thấy (Open-Meteo 1000 đến 2500, Kaggle trung bình sau 07/2025 khoảng 300) đều SAFE, nên cảnh báo hiếm khi đổi màu.
- Không có mô hình ML; dự đoán là heuristic.
- Tuổi dữ liệu tính từ giờ quan sát thời tiết; CO của Open-Meteo có thể cũ hơn tới 1 giờ.
