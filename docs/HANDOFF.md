# Sổ bàn giao

Mỗi agent thêm mục mới **lên đầu** mục của mình sau mỗi phiên làm việc. Ngắn gọn, đủ để agent kia và người dùng không phải hỏi lại.

Mẫu:

```
### YYYY-MM-DD HH:MM — việc đã làm
- Xong: ...
- Chạy thế nào: lệnh
- Chưa làm / đang giả lập: ...
- Cần bên kia: ... (nêu tên route/trường/sự kiện)
- Vấn đề phát hiện: ...
```

## Backend (Claude)

### 2026-10-12 — P1: backend chạy, /api/services thật, /ws/stream
- Xong: FastAPI trong `backend/` (cổng 8000). `GET /api/services` gọi thật từng cổng (NameNode JMX, 4 cổng HTTP DataNode, YARN REST, TCP Kafka, Spark UI 4040-4043, Airflow /health). `ws://localhost:8000/ws/stream`: lúc nối gửi trạng thái 8 dịch vụ (`service.state`), sau đó `service.state` khi có đổi và `reading` cho mỗi file mới trong HDFS `/stream_input`. Lỗi trả đúng schema `Error` (kể cả 404/400 mặc định của FastAPI).
- Chạy thế nào (trong WSL): `cd backend && python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000`. Cần HDFS đang chạy (`bash ~/start-4nodes.sh`). Test: `cd backend && python3 -W ignore -m unittest discover tests`; kiểm backend đang chạy: `python3 backend/tools/check_live.py`.
- **Đang giả lập (UI phải ghi chú):** `STREAM_MODE=replay` phát lại `mock/stream-events.ndjson` với ts/seq mới. Các sự kiện `hdfs.op`, `flow.step`, `spark.progress`, `kafka.stats` trong chế độ này là GIẢ. Chế độ mặc định `live` KHÔNG phát `hdfs.op`/`flow.step`/`spark.progress` (chưa làm, P4 trở đi). Khi `replay`, trường `detail` của dịch vụ Backend ghi "luồng /ws/stream: replay".
- Quyết định tạm (chưa được người dùng chốt): D1 giữ file-stream HDFS `/stream_input`; D2 nhãn trạm luân phiên nhưng `isSimulatedStation` luôn `true`, kể cả luồng Open-Meteo (nhãn "TP.HCM (Open-Meteo)", `source: live-open-meteo`); D3 giữ 500/1000, `status` do backend tính theo logic `spark_streaming_consumer.py`; D4 Kaggle.
- Chưa kiểm được trên máy thật: đường đọc bản ghi mới của `/stream_input` chỉ được test với NameNode giả, vì `/stream_input` đang rỗng và backend không được ghi lên HDFS. Cần người dùng chạy một producer (`sensor_producer_replay_dataset.py` hoặc `..._live_api.py`) để thấy `reading` thật.
- Phát hiện về môi trường (khác với mock): cụm có **4 DataNode** (cổng HTTP 9864/9874/9884/9894; mock ghi 1 node); `dfs.replication` mặc định là 1; tên DataNode dạng `DESKTOP-HHFANUM.localdomain:9866`. ResourceManager/NodeManager, Kafka, Airflow đang tắt trên máy; Spark UI chỉ tồn tại khi có ứng dụng. Múi giờ WSL là Asia/Bangkok (+07), nên chuỗi `datetime` không có offset của producer được đọc là giờ +07.
- Cần frontend: `service.state` đầu kết nối là ảnh chụp cả 8 dịch vụ (không phải "đổi trạng thái"); coi `seq` bắt đầu từ 0 mỗi kết nối.

### 2026-10-12 — Khung dự án
- Xong: openapi.yaml, events.md, validator, mock (14 route + luồng sự kiện), AGENTS.md.
- Chạy thế nào: `python docs/contract/validate_contract.py` và `python mock/serve.py`.
- Chưa làm: toàn bộ backend thật. WebSocket chưa có trong mock.
- Cần frontend: bắt đầu P1 với mock, báo thiếu trường tại mục Frontend.
- Lưu ý: giờ trong mock `aqi-hourly-profile.json` đã đổi sang giờ địa phương (+7).

## Frontend (Antigravity)

### 2026-10-12 15:30 — Hoàn tất P1, P2, P3 Frontend
- Xong:
  + P1: Dự án Vite + React 19 + TypeScript + Tailwind CSS; sinh kiểu tự động từ `docs/contract/openapi.yaml` (`npm run generate:types`); khung điều hướng responsive hỗ trợ di động; lớp `StreamSource` với `ReplaySource` (đọc `mock/stream-events.ndjson`) và `WebSocketSource` (kết nối `/ws/stream` với backoff tái kết nối 1-15s); thanh Ticker sự kiện trực tiếp; trang Tổng quan đọc `/api/services`, cập nhật `service.state`, thẻ số liệu CO trực tiếp, cảnh báo trạm mô phỏng, nguồn dữ liệu và trạng thái CO từ backend.
  + P2: Trang HDFS (tóm tắt cụm `/api/hdfs/summary`, danh sách `/api/hdfs/datanodes`, duyệt cây tệp tin `/api/hdfs/files`, modal xem blocks `/api/hdfs/blocks`, lịch sử `hdfs.op`); trang Kafka (`/api/kafka/topics`, theo dõi `kafka.stats`); trang Spark (`/api/spark/applications`, theo dõi `spark.progress`).
  + P3: Trang Phân tích CO & AQI: Biểu đồ chuỗi thời gian theo giờ/ngày/tháng (`/api/aqi/series`), biểu đồ nhịp sinh học 24 giờ (`/api/aqi/hourly-profile`); ĐẶC BIỆT: Tách rời và đánh dấu điểm gãy dữ liệu ngày 01/07/2025, tuyệt đối không nối mượt qua điểm này; ghi chú cảnh báo trạm mô phỏng luân phiên; hiển thị nguồn dữ liệu (`source`).
- Chạy thế nào:
  + Mock server: `python mock/serve.py` (cổng 8010)
  + Frontend: `cd frontend && npm install && npm run dev` (truy cập `http://localhost:5173`)
  + Build production: `cd frontend && npm run build`
- Chưa làm / đang giả lập: P4 (Sơ đồ luồng sống) và P5 (Terminal xterm.js, SQL, dự đoán CO). Đang chạy với mock server 8010 và ReplaySource phát lại luồng sự kiện.
- Cần bên kia: Backend thật (FastAPI) triển khai các route REST và `/ws/stream`. Frontend đã sẵn sàng chuyển sang backend thật qua `VITE_API_BASE=http://localhost:8000` và `VITE_STREAM=ws`.
- Vấn đề phát hiện: Không có. Schema và contract hoàn toàn khớp, build TypeScript sạch sẽ không có lỗi.

