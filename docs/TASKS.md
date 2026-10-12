# Kế hoạch

Trạng thái: [ ] chưa, [~] đang làm, [x] xong. Người làm: C = Claude, A = Antigravity.

## Quyết định đang chờ người dùng

Đang áp dụng TẠM (người dùng chưa chốt, 2026-10-12): D1 = (a) giữ file-stream qua HDFS `/stream_input`, Kafka chỉ hiển thị trạng thái; D2 = giữ nhãn trạm luân phiên nhưng luôn `isSimulatedStation=true`, không trộn Open-Meteo và OpenWeather vào một chuỗi; D3 = giữ 500/1000; D4 = bộ Kaggle cho `/api/aqi/*`.

- **D1. Luồng thật là gì?** Hiện hai producer ghi file JSON thẳng vào HDFS `/stream_input`, còn Kafka chỉ mới được kiểm thử. Muốn trình diễn "Producer → Kafka → Spark" thì phải đổi producer hoặc thêm cầu nối. Chọn: (a) giữ file-stream, Kafka chỉ minh họa; (b) sửa thành Kafka thật.
- **D2. Trạm giả và nguồn trộn.** Giữ nhãn trạm luân phiên (gắn nhãn "mô phỏng") hay bỏ; và có tách luồng Open-Meteo khỏi dữ liệu OpenWeather không.
- **D3. Ngưỡng CO.** 500 và 1000 µg/m³ chưa có căn cứ. Cần tra băng mức của OpenWeather/AQI chuẩn rồi chọn.
- **D4. Dữ liệu nền cho dashboard.** Bản Kaggle hay bản tự cào `owm_air_pollution_raw.csv` (CO tương quan 0,71 với Kaggle; Kaggle thấp hơn khoảng 13 %).

## P0 — Khung dự án (C)
- [x] Hợp đồng REST + WebSocket, mock, validator, AGENTS.md, prompt.
- [ ] Người dùng thêm repo vào Orca, tạo 2 worktree.

## P1 — Có thể chạy từ đầu đến cuối với dữ liệu mẫu
- [x] C: FastAPI chạy, `/api/services` kiểm tra thật các cổng, `/ws/stream` (live: bản ghi mới trong `/stream_input` + đổi trạng thái dịch vụ; `STREAM_MODE=replay` phát lại `stream-events.ndjson`, giả lập). Xem `backend/README.md`.
- [ ] A: Vite + React + TS, kiểu sinh từ openapi, khung điều hướng, trang Tổng quan đọc mock.
- Xong khi: mở `localhost:5173` thấy trạng thái dịch vụ từ backend thật và một tin `reading` chạy trên màn hình.

## P2 — Dữ liệu thật của HDFS, Kafka, Spark
- [x] C: `/api/hdfs/*` (JMX + WebHDFS), `/api/spark/applications` — đã so với `hdfs fsck`/`dfsadmin` trên cụm thật.
- [~] C: `/api/kafka/topics` + `kafka.stats` đã viết và test giả lập, **chưa thử với broker thật** (Kafka không khởi động được: mất `/tmp/kraft-combined-logs`, cần `kafka-storage format`, chờ người dùng đồng ý).
- [ ] A: trang HDFS (DataNode, cây thư mục, block của file), trang Kafka, trang Spark.
- Xong khi: các số trên trang khớp NameNode UI `:9870`.

## P3 — Dữ liệu CO
- [x] C: `/api/aqi/series`, `/hourly-profile`, `/latest` (bộ Kaggle theo D4 tạm, đã +7 giờ; xác nhận UTC bằng tương quan với bản thô OpenWeather). Mock hai route đầu được sinh lại từ backend.
- [ ] A: biểu đồ theo tháng/ngày/giờ, nhịp theo giờ, đánh dấu điểm gãy 01/07/2025, nhãn "mô phỏng" cho trạm.

## P4 — Sơ đồ luồng sống
- [x] C: `/api/flows` (hdfs-write có định nghĩa; các luồng khác `planned`), `hdfs.op` và `flow.step` của hdfs-write từ log NameNode thật. Luồng Kafka để `planned` vì D1 (tạm) giữ file-stream. **Chưa thấy chạy với một lệnh `hdfs dfs -put` thật** (tôi không được ghi lên HDFS): chỉ chạy lại dòng log thật cũ qua bộ theo dõi nối NameNode thật. Người dùng chạy thử: `python3 backend/tools/watch_stream.py` rồi `hdfs dfs -put` ở cửa sổ khác.
- [ ] A: màn hình sơ đồ luồng: vẽ thành phần, chạy các bước theo sự kiện, phân biệt kênh data/control/background.
- Xong khi: chạy `hdfs dfs -put` trong WSL và thấy các bước sáng lên đúng thứ tự.

## P5 — Cửa sổ lệnh, SQL, dự đoán, hoàn thiện
- [x] C: `/ws/terminal` (pty bash, bảo mật theo events.md, **tắt mặc định**, bật bằng `BACKEND_ALLOW_WRITES=1`), `/api/sql` (SparkSQL thật, chỉ SELECT, bảng `aqi` từ HDFS), `/api/predict` (heuristic, KHÔNG phải ML: thử nghiệm cho thấy giờ/tháng/nhiệt độ/gió gần như không dự đoán được CO).
- [ ] A: xterm.js, ô nhập SQL, biểu mẫu dự đoán.
- [ ] Cả hai: kịch bản demo, ảnh chụp cho báo cáo, kiểm tra cuối.

## P6 — Rà soát và tích hợp (C, người dùng đặt ra sau P5)
- [x] C: đọc diff PR #1 của frontend, chạy validator và build frontend, rebase `feat-backend` lên `origin/main`. Kết quả và yêu cầu sửa ở HANDOFF mục P6.
- [ ] A: sửa các điểm ở HANDOFF mục P6 (hộp cảnh báo điểm gãy, ghi chú trạm, trạng thái rỗng của Spark), chuyển sang backend thật và xem trên trình duyệt.
- [ ] C: mở PR của `feat-backend` vào `main` (chờ người dùng đồng ý), đọc lại frontend P4-P5 khi có.
