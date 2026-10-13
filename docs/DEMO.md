# Kịch bản demo (khoảng 8 phút)

Ảnh chụp (13/10/2026, backend thật) nằm trong `docs/demo/`. Kịch bản này chỉ nói những gì đã chạy và quan sát được; phần nào là suy ra hoặc mô phỏng thì có ghi rõ để nói đúng khi trình bày.

## 1. Chuẩn bị (làm trước, khoảng 3 phút, trong WSL)

Thứ tự (chi tiết và lý do ở `infra/README.md`):

1. HDFS: `bash ~/start-4nodes.sh`.
2. Kafka (Java 17): `JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PATH=$JAVA_HOME/bin:$PATH /home/trong/kafka/bin/kafka-server-start.sh -daemon /home/trong/kafka/config/server.properties`. Dữ liệu Kafka nằm ở `/home/trong/kafka-data`, nên topic `hcmc_co_stream` còn sau khi khởi động lại WSL.
3. YARN (nếu muốn thẻ ResourceManager, NodeManager hiện UP): `setsid nohup ~/hadoop/bin/yarn --daemon start resourcemanager`, rồi NodeManager như ghi trong `infra/README.md`.
4. Dữ liệu sống: `bash infra/live_stack.sh start` (producer Open-Meteo và Spark consumer; lần đầu Spark tải connector Kafka khoảng 1 phút).
5. Backend: `cd backend && STREAM_SOURCE=kafka FLOWS_EXTRA=1 python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000`.
6. Frontend (node của Windows): `cd frontend && $env:VITE_API_BASE="http://localhost:8000"; $env:VITE_STREAM="ws"; npm run dev`, mở `http://localhost:5173`.

Lưu ý bộ nhớ: máy chỉ có khoảng 7,7 GB; khi mọi thứ chạy thì còn khoảng 0,3 GB trống. Đừng bấm chạy truy vấn SparkSQL (cần thêm khoảng 1 GB) nếu chưa tắt bớt (ví dụ YARN), nếu không hệ thống có thể kill tiến trình.

Kiểm trước giờ demo: góc phải trên báo "WebSocket Kết nối"; Tổng quan hiện ít nhất 7/8 dịch vụ UP (Airflow đang tắt).

## 2. Kịch bản

| # | Tab | Làm gì | Nói gì | Ảnh |
|---|---|---|---|---|
| 1 | Tổng quan | Chỉ số CO, tuổi dữ liệu, trạng thái cụm | Dữ liệu đi từ Open-Meteo qua Kafka. Đây là số của mô hình khí quyển (CAMS), cập nhật tối đa 15 phút một lần (CO mỗi giờ), không phải cảm biến từng giây. Nhãn "trạm mô phỏng" là có chủ ý. Mức DANGER dùng ngưỡng 500/1000 tạm thời (D3), chưa phải chuẩn. | `01-tong-quan.jpg` |
| 2 | HDFS | 4 DataNode, số file, block, duyệt `/live` | Cụm HDFS thật (4 DataNode trên một máy), số đọc từ NameNode. | `02-hdfs.jpg` |
| 3 | Kafka | Topic `hcmc_co_stream`, `spark:co_stream` | Lag của Spark lấy từ checkpoint trên HDFS, không phải consumer group thật (ghi ngay trên giao diện). | `03-kafka.jpg` |
| 4 | Spark | Ứng dụng `HCMC_CO_Kafka_Streaming` | Spark Structured Streaming đọc Kafka, ghi Parquet vào `/live/co_stream`. Tiến độ micro-batch là ước tính suy ra từ Spark REST. Thẻ tiến trình chỉ hiện sau khi có batch mới kể từ lúc mở trang. | `04-spark.jpg` |
| 5 | CO & AQI | Biểu đồ tháng/ngày/giờ | Bộ Kaggle (OpenWeather), có điểm gãy 01/07/2025 (CO giảm mạnh) nên biểu đồ tách làm hai đoạn, không nối mượt. Khác nguồn với dữ liệu sống. | `05-co-aqi.jpg` |
| 6 | Sơ đồ luồng | Chọn "Kafka nhận bản ghi", rồi "Spark Structured Streaming", chờ quan sát mới | Khi producer gửi bản ghi mới: bước ACK kèm offset sáng (quan sát thật), sau đó micro-batch của Spark sáng 4 bước (suy ra từ việc job thành công). Chọn luồng HDFS để thấy ghi thật của Spark xuống `/live`. | `06a-luong-kafka-produce.jpg`, `06b-luong-spark-streaming.jpg` (chụp lúc offset 34, 07:30 ngày 13/10) |
| 7 | SparkSQL | Chọn truy vấn mẫu, chạy | Bảng `aqi` (Kaggle) và `live` (Spark ghi từ Kafka) khác nguồn, không trộn. Lần đầu mất khoảng 10 giây vì khởi động phiên Spark. | |
| 8 | Dự đoán CO | Chạy dự đoán | Đây là heuristic thống kê, không phải mô hình ML: nhiệt độ, gió, tháng chưa dùng vì hồi quy cho R² gần 0. Điểm cần nhấn là khoảng 10%-90%, không phải một con số. | `07-du-doan-ket-qua.jpg` |
| 9 | Cửa sổ lệnh | Chỉ giới thiệu | Tắt mặc định. Bật bằng `BACKEND_ALLOW_WRITES=1`, cần token in ra ở console backend, chỉ nhận Origin localhost:5173. Chưa chụp ảnh vì cần bật ghi. | |

Nếu quan sát mới chưa tới lúc demo: Open-Meteo đổi thời tiết lúc :00, :15, :30, :45 (producer gửi trong khoảng 1 phút sau mốc đó). Có thể dùng `python3 infra/replay_producer.py --count 20 --interval 3` để thấy luồng HDFS sáng, nhưng phải nói rõ đó là dữ liệu phát lại, không phải dữ liệu sống.

## 3. Kiểm tra cuối đã làm (13/10/2026)

- Backend: 111 test qua, validator hợp đồng OK (P12).
- Frontend: `npm run lint` và `npm run build` sạch (PR #11).
- Trình duyệt, backend thật: các tab Tổng quan, HDFS, Kafka, Spark, CO & AQI, Sơ đồ luồng, SparkSQL, Dự đoán CO, Cửa sổ lệnh mở được; console không báo lỗi trong các lần xem.
- Luồng sống: quan sát 06:00 ngày 13/10 (offset 28) đi qua Kafka, backend, Spark, HDFS, và làm sáng `kafka-produce`, `spark-streaming`.
- Thu nhỏ 390 px (khung iframe, vì cửa sổ trình duyệt không thu nhỏ được): 8/9 tab không tràn ngang. Tab CO & AQI tràn ngang 432 px do dòng "Nguồn: kaggle:hcmc_aqi_dataset_after_preprocess_final.csv ..." không xuống dòng (`09-mobile-390px-co-aqi-tran-ngang.jpg`); đã ghi cho frontend trong `docs/TASKS.md`.

## 4. Chưa làm hoặc còn hạn chế

- Truy vấn SparkSQL (bảng `live`) chưa chạy từ giao diện do thiếu RAM; API đã kiểm (P12).
- Cửa sổ lệnh chưa thử trên giao diện thật (cần `BACKEND_ALLOW_WRITES=1`).
- Airflow vẫn tắt (không đủ RAM), chỉ có DAG thử.
- Ngưỡng CO (D3), nhãn trạm (D2), nguồn nền (D4) chưa được chốt; mọi giá trị Open-Meteo hiện (khoảng 1000 đến 2500 µg/m³) đều bị gắn DANGER vì khác thang với Kaggle (trung bình sau 07/2025 khoảng 300).
- Không có mô hình ML; dự đoán là heuristic.
- Tuổi dữ liệu tính từ giờ quan sát thời tiết; CO của Open-Meteo có thể cũ hơn tới 1 giờ.
