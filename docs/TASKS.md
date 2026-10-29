# Kế hoạch

Trạng thái: [ ] chưa, [~] đang làm, [x] xong. Người làm: C = Claude, A = Antigravity.

## Quyết định đang chờ người dùng

**D1 đã chốt = (b) (người dùng yêu cầu 2026-10-12): luồng thật đi qua Kafka với dữ liệu sống Open-Meteo** (xem HANDOFF P11, `infra/README.md`); luồng file-stream `/stream_input` và producer cũ vẫn chạy được với `STREAM_SOURCE=hdfs`.

**D2, D3, D4 đã chốt ngày 13/10/2026 (người dùng giao cho lead chọn phương án tối ưu).**

**D5 (người dùng yêu cầu 14/10/2026): luồng DỮ LIỆU THẬT đổi từ Open-Meteo sang OpenWeather Air Pollution API** (cùng nguồn gốc với bộ Kaggle nên mô hình dự đoán áp dụng được); dashboard hiển thị hai luồng song song: PHÁT LẠI (`replay-dataset`) và DỮ LIỆU THẬT (`live-openweather`). Producer Open-Meteo giữ lại nhưng không bật mặc định. Vì vậy các câu về "luồng sống = Open-Meteo" ở D1, D2 phía dưới và trong HANDOFF P11 đến P18 là lịch sử, chỉ còn đúng cho tới khi làm xong Bước A1.

**D6 (người dùng chốt 14/10/2026) — cách chia dữ liệu cho A4:** (a) cách chính: huấn luyện và kiểm tra trong giai đoạn sau điểm gãy (chia 80/20 theo thời gian trong đó); (b) báo thêm chia 80/20 theo yêu cầu gốc trên toàn bộ dữ liệu; (c) báo thêm huấn luyện toàn bộ có biến đánh dấu chế độ, kiểm tra trên 20% cuối. Bỏ các ngày chuyển tiếp (khoảng 18 đến 30/06/2025) khỏi tập huấn luyện, xác định ranh giới theo dữ liệu chứ không theo mốc 01/07. Báo cáo phải ghi: nguyên nhân điểm gãy dữ liệu chưa xác định, không kết luận CO ngoài đời giảm thật. Cho phép chạy lại `04_clean.py` (đã làm, bản cũ ở `/co/processed_old`).

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
- [x] Khởi động nguội từ đầu đến đuôi (13/10/2026): `bash infra/start_all.sh` một lệnh, 3 lỗi gặp đã sửa (HANDOFF P18).
- [x] C: kịch bản demo và ảnh chụp (`docs/DEMO.md`, `docs/demo/`), kiểm tra cuối: console không lỗi; ở 390 px chỉ tab CO & AQI tràn ngang (xem dưới). Có ảnh luồng Kafka và Spark sáng (offset 34, 07:30 ngày 13/10).
- [x] A (PR #14, đã gộp): tab CO & AQI tràn ngang ở 390 px: dòng "Nguồn: kaggle:hcmc_aqi_dataset_after_preprocess_final.csv ..." (thẻ biểu đồ chuỗi thời gian) không xuống dòng (`break-all` hoặc `min-w-0` cho thẻ cha).
- [x] A (PR #14, đã gộp) (nhẹ, từ rà soát P13): bỏ số viết cứng 1678-2508 và 35 064 trong SqlTab, sửa câu "đo quan sát thực tế" (nguồn là mô hình), chỉ giải thích lỗi `live` khi truy vấn thật sự dùng bảng `live`, reset `hasStreamReading` khi kết nối lại.
- [x] D2, D3, D4 đã chốt (xem trên).
- [x] A (PR #16, đã gộp): theo D3 mới, sửa đoạn chữ cố định "500 và 1000 µg/m³" ở `AqiTab.tsx` (~dòng 442) và các ghi chú ngưỡng khác (Tổng quan: "chưa có quy chuẩn chính thức"), chạy lại `npm run generate:types` (mô tả `CoStatus` trong openapi đã đổi). Chỉ hiển thị `status` backend trả; có thể nói ngưỡng theo thang OpenWeather.
- [x] Airflow đã bật (13/10/2026, `bash infra/airflow.sh start`; DAG mẫu đang tạm dừng, xem `infra/README.md`) và dashboard báo UP.
- [x] Cửa sổ lệnh đã thử trên giao diện thật (xem `docs/DEMO.md`).

## Kế hoạch 14/10/2026 — bám pipeline 9 bước của giảng viên, đổi cho CO (C = Claude)
Mỗi bước một nhánh từ `main`, một PR nhỏ; làm xong bước nào thì dừng, người dùng bảo mới làm bước kế.
- [x] **Bước 0 — Hợp đồng 0.2.0** (PR #20 đã gộp 14/10) (nhánh `feat/contract-step0`): `originalDatetime`/`coPredicted`/`modelVersion`, `/api/aqi/latest?source=`, `/api/replay/*`, `/api/pipeline/steps`, `/api/model/metrics`, `/api/predict` kind=ml, mock, validator mở rộng. Validator OK, 126 test backend qua, mock thử 17 URL; chờ người dùng gộp, frontend chưa chạy lại `generate:types` (xem HANDOFF 14/10).
- [~] **A1 — Luồng sống OpenWeather** (nhánh `feat/live-owm-a1`, PR chờ người dùng gộp): đã chạy thật OpenWeather (offset 52) → Kafka → Spark → HDFS `/live` → `/api/aqi/latest?source=`, và phát lại qua Kafka có điều khiển (`/api/replay/*`). Còn mở: chưa thấy số liệu khí OpenWeather đổi trong cửa sổ đo 11 phút (nhịp cập nhật thật chưa biết), chưa thấy bản ghi `live-openweather` thật đến qua `/ws/stream`. Xem HANDOFF 14/10 (A1).
- [~] **A2 — HDFS và làm sạch (bước 1-4)** (nhánh `feat/pipeline-a2`, PR chờ người dùng gộp): đã chạy thật `pipeline/01_prepare_upload.py`, `02_check_raw.py`, `04_clean.py` trên HDFS (35 064 dòng, 34 ô độ ẩm > 100 bị cắt, 28 481/6 583 dòng trước/sau 01/07/2025) và `GET /api/pipeline/steps` thật. Còn mở: xin phép chạy lại `04_clean.py --overwrite` để `step_summary.json` khớp mã cuối (HANDOFF 14/10, A2).
- [x] **A3 — Phân tích (bước 5, 6a)** (PR #26 đã gộp 14/10): xem HANDOFF A3.
- [~] **A3b — theo trả lời của người dùng cho A3** (nhánh `feat/pipeline-a3b`): làm sạch chạy lại (bản cũ ở `/co/processed_old`), độ ổn định K-Means 6 seed (k=2 không ổn định, k=3 ổn định), trạng thái `partial`, `GET /api/analysis/sql` và `/api/analysis/day-clusters` (hợp đồng 0.3.0, mock là kết quả thật). Việc cho frontend: `docs/PROMPTS.md` P19. (PR #27 chờ gộp)
- [~] **A4 — Mô hình dự đoán (bước 6b, 7)** (nhánh `feat/model-a4`, xây trên PR #27, PR chờ gộp): đã chạy thật 3 mô hình × 3 phương án theo D6; phương án chính (sau chuyển tiếp): cả ba thắng mốc CO giờ trước (RMSE 37,1 đến 41,3 so với 43,9; chọn Random Forest theo kiểm định, −11,3%); chia 80/20 gốc và có biến chế độ: không mô hình nào thắng mốc. Chuyển tiếp theo dữ liệu 13/05 đến 11/07/2025 (không duy nhất). `/api/model/metrics` và `/api/predict` ml chạy trên HDFS thật. Việc cho frontend: P20. Chưa kiểm mô hình trên dữ liệu sống (A5).
- [x] **A5 — Dự đoán trên luồng sống (bước 8)** (nhánh `feat/stream-a5`, PR #29 đã gộp): consumer Spark nạp mô hình A4, dự đoán cho cả phát lại lẫn dữ liệu thật, ghi `co_actual`, `co_predicted`, `model_version` vào `/live` và vào topic `hcmc_co_enriched`; `/ws/stream` và `/api/aqi/latest` trả `coPredicted`, `modelVersion`. Độ trễ CO: producer dựng `modelInputs` (phát lại từ bộ dữ liệu; dữ liệu thật từ `air_pollution/history`). Xem HANDOFF 14/10 (A5); việc cho frontend: P21.
- [~] **A5b — `coIsForecast` (hợp đồng 0.5.0) và backend mặc định đọc `hcmc_co_enriched`** (nhánh `feat/co-is-forecast`, PR chờ gộp; theo trả lời của người dùng cho A5): xem HANDOFF 14/10; sau khi gộp frontend phải chạy `cd frontend && npm run generate:types` (PROMPTS P21). Gộp xong mới làm A6.
- [x] **A6 — Airflow (bước 9)** (PR #32 đã gộp; 14/10 DAG đã chạy thật trên `/co`, bước 9 `done`, trang Pipeline 9/9): DAG `hcmc_co_pipeline` (`infra/airflow_dags/`) nối bước 1-2, 3, 4, 5, 6a, 6a bổ sung, 6b bằng spark-submit; tạm dừng, không lịch, `overwrite` mặc định false. DAG mẫu cũ GIỮ NGUYÊN nhưng bản cài đổi id thành `hcmc_co_pipeline_sample` (trùng id, xem HANDOFF 14/10 A6). Đã chạy thật bằng `airflow dags test` ở `co_root=/co_airflow_test` (8 tác vụ thành công, metrics giống hệt `/co`) và đường lỗi (`overwrite=false`: `clean` dừng mã 2, `record_dag_failure` chạy). Đã xóa `/co_airflow_test`, `/co_backup_before_a6` và `/co/processed_old` trên HDFS theo cho phép của người dùng (14/10); `/co` còn `raw`, `processed`, `output`, trang Pipeline 9/9.
- [ ] A (frontend, sau Bước 0): `generate:types`; hai cột phát lại / dữ liệu thật; điều khiển phát lại; trang bước pipeline; trang chỉ số mô hình; `filledInputs` ở dự đoán; nhãn "dữ liệu mẫu" khi mock trả `X-Mock: true`.
- [x] C (14/10/2026, người dùng yêu cầu): hợp đồng 0.6.0 `ServiceStatus.state = disabled` cho YARN/Airflow tắt chủ động; tab Tổng quan hiện "Tắt chủ động"; viết lại tab Dự đoán cho mô hình ML (ô CO giờ trước, chất khác, điền từ bản ghi thật). Xem HANDOFF 23:30.
- [ ] Người dùng: chạy lại test backend và giao diện với backend thật trong WSL; chụp lại ảnh `docs/demo/`.

## Video hướng dẫn (C, người dùng yêu cầu 24/10/2026)
- [x] Kịch bản 8 video (`storyboard/00_quy_uoc.md` đến `08_…`), người dùng đã duyệt.
- [x] Video 02 (HDFS) làm thử bằng Remotion trong `video/`: giọng edge-tts `vi-VN-HoaiMyNeural`, mỗi cảnh một mp3, phụ đề `.srt`, component dùng chung (Node, Arrow, Packet, Block, Counter, Callout, Ledger, Terminal). Xem `video/README.md`.
- [x] Đổi giọng đọc sang ElevenLabs `eleven_v3`, giọng Viên, lời rút gọn, Whisper kiểm thuật ngữ (25/10/2026, xem HANDOFF).
- [ ] Chờ người dùng nghe và duyệt bản ElevenLabs của video 02 trước khi làm video 01, 03 đến 08.
