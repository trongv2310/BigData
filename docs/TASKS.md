# Kế hoạch

Trạng thái: [ ] chưa, [~] đang làm, [x] xong. Người làm: C = Claude, A = Antigravity.

## Quyết định đang chờ người dùng

**D1 đã chốt = (b) (người dùng yêu cầu 2026-10-12): luồng thật đi qua Kafka với dữ liệu sống Open-Meteo** (xem HANDOFF P11, `infra/README.md`); luồng file-stream `/stream_input` và producer cũ vẫn chạy được với `STREAM_SOURCE=hdfs`.

**D2, D3, D4 đã chốt ngày 13/10/2026 (người dùng giao cho lead chọn phương án tối ưu).**

- ~~**D1. Luồng thật là gì?**~~ Đã chốt (b): Producer → Kafka → Spark/backend, xem trên.
- ~~**D2. Trạm giả và nguồn trộn.**~~ Chốt: giữ nhãn trạm nhưng luôn `isSimulatedStation=true` (UI phải ghi "mô phỏng"); luồng sống chỉ có một điểm "TP.HCM (Open-Meteo)", không có trạm; `source` tách `live-open-meteo` khỏi `replay-dataset`, và không trộn Open-Meteo với OpenWeather/Kaggle vào một chuỗi hay một truy vấn. Lý do: bỏ nhãn sẽ phải đổi hợp đồng và producer replay cũ của người dùng, trong khi nhãn kèm cờ mô phỏng đã đủ trung thực.
- ~~**D3. Ngưỡng CO.**~~ Chốt: thang CO của OpenWeather Air Pollution API (nguồn của bộ lịch sử), đơn vị µg/m³: Good < 4400, Fair 4400..9400, Moderate 9400..12400, Poor 12400..15400, Very Poor ≥ 15400. Hợp đồng chỉ có ba mức nên: `SAFE` < 4400, `WARNING` 4400 đến dưới 12400, `DANGER` ≥ 12400 (mốc thuộc mức cao hơn). 4400 cũng xấp xỉ hướng dẫn 24 giờ 4 mg/m³ của WHO 2021. Cấu hình bằng `CO_WARNING_UG`, `CO_DANGER_UG`. Hệ quả cần nói thật: mọi giá trị đã thấy (Kaggle tối đa vài nghìn, Open-Meteo 1000 đến 2500) đều là `SAFE`; 500/1000 cũ chỉ là ngưỡng trong script mẫu của người dùng, không có căn cứ. Script cũ `spark_streaming_consumer.py` không bị sửa nên vẫn in nhãn theo 500/1000.
- ~~**D4. Dữ liệu nền cho dashboard.**~~ Chốt: bộ Kaggle cho `/api/aqi/*` và bảng `aqi` (đã tiền xử lý, đủ 2022-04 đến 2026-04, có điểm gãy 01/07/2025 được ghi chú). Bản tự cào `owm_air_pollution_raw.csv` không dùng làm nền (ngắn hơn, CO thấp hơn Kaggle khoảng 13 %, tương quan 0,71).

## P0 — Khung dự án (C)
- [x] Hợp đồng REST + WebSocket, mock, validator, AGENTS.md, prompt.
- [ ] Người dùng thêm repo vào Orca, tạo 2 worktree.

## P1 — Có thể chạy từ đầu đến cuối với dữ liệu mẫu
- [x] C: FastAPI chạy, `/api/services` kiểm tra thật các cổng, `/ws/stream` (live: bản ghi mới trong `/stream_input` + đổi trạng thái dịch vụ; `STREAM_MODE=replay` phát lại `stream-events.ndjson`, giả lập). Xem `backend/README.md`.
- [x] A: Vite + React + TS, kiểu sinh từ openapi, khung điều hướng, trang Tổng quan đọc mock (đã gộp, đã chạy với backend thật).
- Xong khi: mở `localhost:5173` thấy trạng thái dịch vụ từ backend thật và một tin `reading` chạy trên màn hình.

## P2 — Dữ liệu thật của HDFS, Kafka, Spark
- [x] C: `/api/hdfs/*` (JMX + WebHDFS), `/api/spark/applications` — đã so với `hdfs fsck`/`dfsadmin` trên cụm thật.
- [x] C: `/api/kafka/topics` + `kafka.stats` đã thử với broker thật (2026-10-12 18:00, sau khi người dùng đồng ý `kafka-storage format --standalone`): liệt kê topic, partition, replication đúng; **chưa thử với tin nhắn và consumer group thật** (chưa có producer ghi vào Kafka).
- [x] C: `spark.progress` trên `/ws/stream` (2026-10-12, P8): suy ra từ Spark REST vì REST không lộ tiến độ micro-batch; đã chạy với Spark Streaming thật (batch 280-296 khớp từng `reading`). Số liệu là suy ra, ghi rõ trong `events.md`.
- [x] A: trang HDFS (DataNode, cây thư mục, block của file), trang Kafka, trang Spark (đã gộp; KafkaTab có thêm mục `spark:co_stream` ở PR #11).
- Xong khi: các số trên trang khớp NameNode UI `:9870`.

## P3 — Dữ liệu CO
- [x] C: `/api/aqi/series`, `/hourly-profile`, `/latest` (bộ Kaggle theo D4 tạm, đã +7 giờ; xác nhận UTC bằng tương quan với bản thô OpenWeather). Mock hai route đầu được sinh lại từ backend.
- [x] A: biểu đồ theo tháng/ngày/giờ, nhịp theo giờ, đánh dấu điểm gãy 01/07/2025, nhãn "mô phỏng" cho trạm (đã gộp, sửa theo rà soát P6).

## P4 — Sơ đồ luồng sống
- [x] C: `/api/flows` (hdfs-write có định nghĩa; các luồng khác `planned`), `hdfs.op` và `flow.step` của hdfs-write từ log NameNode thật. Luồng Kafka để `planned` vì D1 (tạm) giữ file-stream. **Chưa thấy chạy với một lệnh `hdfs dfs -put` thật** (tôi không được ghi lên HDFS): chỉ chạy lại dòng log thật cũ qua bộ theo dõi nối NameNode thật. Người dùng chạy thử: `python3 backend/tools/watch_stream.py` rồi `hdfs dfs -put` ở cửa sổ khác.
- [x] A: màn hình sơ đồ luồng: vẽ thành phần, chạy các bước theo sự kiện, phân biệt kênh data/control/background. PR #11 lọc `flow.step` theo `flowId` và vẽ thêm `kafka-produce`, `spark-streaming`; đã xem thật 13/10/2026 06:01 (HANDOFF P13).
- Xong khi: chạy `hdfs dfs -put` trong WSL và thấy các bước sáng lên đúng thứ tự.

## P5 — Cửa sổ lệnh, SQL, dự đoán, hoàn thiện
- [x] C: `/ws/terminal` (pty bash, bảo mật theo events.md, **tắt mặc định**, bật bằng `BACKEND_ALLOW_WRITES=1`), `/api/sql` (SparkSQL thật, chỉ SELECT, bảng `aqi` từ HDFS), `/api/predict` (heuristic, KHÔNG phải ML: thử nghiệm cho thấy giờ/tháng/nhiệt độ/gió gần như không dự đoán được CO).
- [x] A: xterm.js, ô nhập SQL (có bảng `live`), biểu mẫu dự đoán (đã gộp). Truy vấn `live` từ SqlTab chưa chạy trên giao diện (thiếu RAM), API đã kiểm ở P12.
- [ ] Cả hai: kịch bản demo, ảnh chụp cho báo cáo, kiểm tra cuối.

## P6 — Rà soát và tích hợp (C, người dùng đặt ra sau P5)
- [x] C: đọc diff PR #1 của frontend, chạy validator và build frontend, rebase `feat-backend` lên `origin/main`. Kết quả và yêu cầu sửa ở HANDOFF mục P6.
- [x] A: sửa các điểm ở HANDOFF mục P6 (hộp cảnh báo điểm gãy, ghi chú trạm, trạng thái rỗng của Spark), chuyển sang backend thật và xem trên trình duyệt.
- [x] C: PR của backend đã gộp vào `main`; đã đọc lại frontend P4-P5, P9, P11-P12 (HANDOFF P9, P10, P13).

## Còn lại
- [x] C: kịch bản demo và ảnh chụp (`docs/DEMO.md`, `docs/demo/`), kiểm tra cuối: console không lỗi; ở 390 px chỉ tab CO & AQI tràn ngang (xem dưới). Có ảnh luồng Kafka và Spark sáng (offset 34, 07:30 ngày 13/10).
- [ ] A: tab CO & AQI tràn ngang ở 390 px: dòng "Nguồn: kaggle:hcmc_aqi_dataset_after_preprocess_final.csv ..." (thẻ biểu đồ chuỗi thời gian) không xuống dòng (`break-all` hoặc `min-w-0` cho thẻ cha).
- [ ] A (nhẹ, từ rà soát P13): bỏ số viết cứng 1678-2508 và 35 064 trong SqlTab, sửa câu "đo quan sát thực tế" (nguồn là mô hình), chỉ giải thích lỗi `live` khi truy vấn thật sự dùng bảng `live`, reset `hasStreamReading` khi kết nối lại.
- [x] D2, D3, D4 đã chốt (xem trên).
- [ ] A: theo D3 mới, sửa đoạn chữ cố định "500 và 1000 µg/m³" ở `AqiTab.tsx` (~dòng 442) và các ghi chú ngưỡng khác (Tổng quan: "chưa có quy chuẩn chính thức"), chạy lại `npm run generate:types` (mô tả `CoStatus` trong openapi đã đổi). Chỉ hiển thị `status` backend trả; có thể nói ngưỡng theo thang OpenWeather.
- [ ] Người dùng: Airflow (không đủ RAM, xem `infra/README.md`).
