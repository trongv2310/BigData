# Kênh thời gian thực

Hợp đồng REST nằm ở `openapi.yaml`. File này mô tả hai WebSocket. Schema của từng sự kiện là `StreamEvent` trong `openapi.yaml` và được `validate_contract.py` kiểm tra bằng `mock/stream-events.ndjson`.

## 1. `ws://localhost:8000/ws/stream` (chỉ đọc, server đẩy xuống)

Mỗi tin là một JSON:

```json
{ "type": "reading", "ts": "2026-10-12T12:00:01+07:00", "seq": 1, "data": { } }
```

| type | Khi nào phát | data |
|---|---|---|
| `reading` | Có bản ghi đo mới trong luồng (cả hai luồng, phân biệt bằng `source`, xem dưới) | `AqiReading` |
| `hdfs.op` | Có thao tác ghi trên HDFS (mkdir, create, addBlock, close, rename, delete) | op, path, blockId, datanode, bytes |
| `flow.step` | Một bước của luồng (xem `/api/flows/{id}`) bắt đầu, xong hoặc lỗi | flowId, runId, step, state |
| `spark.progress` | Mỗi micro-batch của Structured Streaming | queryName, batchId, numInputRows, tốc độ. Backend thật không có số của Spark (REST không lộ StreamingQueryProgress) nên **suy ra** từ job/stage: `queryName` là tên ứng dụng, tốc độ xử lý chưa tính lập kế hoạch và commit, tốc độ vào = 0 khi chưa có batch trước (xem `backend/app/spark_progress.py`) |
| `kafka.stats` | Định kỳ vài giây | topic, messagesPerSecond, lagTotal |
| `service.state` | Một dịch vụ đổi trạng thái up/down | `ServiceStatus` |

Hai luồng `reading` cùng đi trên kênh này (hợp đồng 0.2.0):
- `source = replay-dataset`: PHÁT LẠI, mô phỏng từ bộ lịch sử. `datetime` là lúc phát; thời điểm đo gốc nằm ở `originalDatetime`. Có thể tạm dừng và đổi tốc độ bằng `POST /api/replay/control`; tốc độ 60× của một bản ghi mỗi `baseIntervalSeconds` vẫn phải nằm dưới giới hạn 20 tin/giây.
- `source = live-openweather`: DỮ LIỆU THẬT, `originalDatetime = null`. Không bị `replay/control` ảnh hưởng. Tần suất do nguồn quyết định (số đo chính xác sẽ ghi ở infra/README.md sau Bước A1), không phải theo giây.
- `source = live-open-meteo`: nguồn khác, không bật mặc định.
- Giao diện lọc theo `source` để vẽ hai cột riêng và không trộn hai nguồn vào một đường biểu đồ.
- `coPredicted` và `modelVersion` đi cùng `reading` khi đã có mô hình (Bước A5); trước đó, và khi chưa đủ độ trễ CO, chúng là `null`.
- Không có sự kiện `replay.status`: giao diện lấy trạng thái phát lại bằng `GET /api/replay/status` (sau mỗi lần điều khiển và định kỳ).

Quy tắc:
- `seq` tăng dần cho cả kết nối. Thấy nhảy số thì frontend gọi lại REST tương ứng để lấy trạng thái đầy đủ.
- Frontend phải chịu được loại `type` lạ (bỏ qua, không sập).
- Mất kết nối: frontend tự nối lại, chờ tăng dần 1 s, 2 s, 4 s, tối đa 15 s, và hiện trạng thái "mất kết nối".
- Backend không đẩy quá khoảng 20 tin/giây; nếu nhiều hơn thì gộp.
- Trường hợp bước `flow.step` không đến từ dữ liệu thật mà do backend mô phỏng, backend phải ghi rõ trong `docs/HANDOFF.md` để UI ghi chú "minh họa".

### Phát lại sự kiện khi dev

Mock server chưa có WebSocket. Trong lúc đó frontend đọc `mock/stream-events.ndjson` (mỗi dòng một sự kiện) và phát lại từng dòng cách nhau khoảng 1 giây ở phía trình duyệt, qua một lớp `StreamSource` có hai cách cài: `ReplaySource` (đọc file) và `WebSocketSource`. Đổi một biến môi trường `VITE_STREAM=ws|replay` là chuyển được, mã giao diện không đổi.

## 2. `ws://localhost:8000/ws/terminal` (hai chiều, cửa sổ lệnh)

Backend mở một pty chạy `bash` trong WSL, thư mục gốc là thư mục dự án. Tin nhắn là JSON text.

Client gửi:
```json
{ "type": "input",  "data": "hdfs dfs -ls /\n" }
{ "type": "resize", "cols": 120, "rows": 30 }
```
Server gửi:
```json
{ "type": "output", "data": "Found 3 items\r\n..." }
{ "type": "exit",   "code": 0 }
```

Frontend dùng xterm.js, gửi `resize` khi khung đổi kích thước, và chỉ hiển thị `output`. `data` là chuỗi UTF-8 gồm cả mã điều khiển ANSI.

### Bảo mật bắt buộc (cửa sổ lệnh này chạy lệnh thật bằng quyền của người dùng)

1. Backend chỉ lắng nghe `127.0.0.1`, không bao giờ `0.0.0.0`.
2. Kiểm tra header `Origin` phải thuộc danh sách cho phép (`http://localhost:5173`, `http://127.0.0.1:5173`); sai thì đóng kết nối.
3. Khi khởi động backend tạo một token ngẫu nhiên, in ra console. Frontend gửi `?token=...` khi nối; sai thì đóng với mã 4401. Người dùng dán token vào giao diện một lần.
4. Không ghi lệnh người dùng gõ vào log công khai và không đưa vào repo.
5. Giới hạn: một phiên tại một thời điểm, tự đóng sau 30 phút không hoạt động.
6. Không có endpoint REST chạy lệnh tùy ý.
7. Vì shell ghi được lên HDFS/Kafka/Spark, cửa sổ lệnh **tắt mặc định**; chỉ bật khi chạy backend với `BACKEND_ALLOW_WRITES=1`.

Mã đóng kết nối: sai Origin hoặc client không phải loopback bị từ chối ngay lúc bắt tay (HTTP 403, trình duyệt thấy mã 1006); sau khi nối được thì `4401` sai hoặc thiếu token, `4403` cửa sổ lệnh đang tắt, `4409` đã có phiên khác đang mở, `4408` hết hạn vì không hoạt động. Lý do bằng chữ nằm trong `reason`. Lúc backend khởi động token được in ra console (`[backend] Token cửa sổ lệnh ...`). `POST /api/sql` chỉ nhận một câu SELECT.
