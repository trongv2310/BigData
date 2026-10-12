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

### 2026-10-12 — P2: /api/hdfs/*, /api/kafka/topics, /api/spark/applications
- Xong: `GET /api/hdfs/summary|datanodes|files|blocks` (NameNode JMX + WebHDFS, chỉ đọc), `GET /api/spark/applications` (Spark REST, quét cổng 4040-4043), `GET /api/kafka/topics`, và sự kiện `kafka.stats` trên `/ws/stream` (mỗi 5 giây, bỏ qua lần đo đầu).
- Đã kiểm thật trên cụm của người dùng: số liệu khớp `hdfs dfsadmin -report`, `hdfs fsck /raw/Dataset.csv -files -blocks -locations` (4 block, đúng blockId và DataNode); `check_live.py` báo OK schema và không lệch cấu trúc so với mock. Spark kiểm bằng một ứng dụng PySpark thử (không đụng HDFS), thấy đúng id/tên/số job.
- **Chưa kiểm với Kafka thật:** Kafka không chạy được (thư mục `/tmp/kraft-combined-logs` đã mất sau khi WSL khởi động lại; muốn chạy phải `kafka-storage format`, tức tạo cụm mới, tôi chưa làm). Mã Kafka chỉ được test với dữ liệu giả và với trường hợp broker tắt (trả 503 `KAFKA_UNREACHABLE`). Không rõ `kafka-python-ng 2.2.3` tương thích Kafka 4.1.2 (KRaft); sẽ biết khi chạy thật.
- Quy ước cần frontend biết:
  - Tên DataNode = khóa của NameNode `host:xferPort` (ví dụ `DESKTOP-HHFANUM.localdomain:9866`); `HdfsBlock.locations[]` dùng đúng chuỗi này, nên ghép trực tiếp với `/api/hdfs/datanodes`. Cả 4 DataNode cùng một host, chỉ khác cổng.
  - `filesTotal` = số file **và thư mục** (như NameNode UI "files and directories"). `blocksTotal` là số block, không phải số bản sao (4 DataNode giữ tổng 38 bản sao của 24 block vì có file replication > 1).
  - `capacityTotalBytes` = tổng 4 DataNode, mỗi cái báo cả đĩa host (~1 TB), nên ~4 TB là số thật nhưng đĩa vật lý chỉ có một.
  - `defaultBlockSizeBytes` là 128 MiB (cấu hình cụm), nhưng `/raw/Dataset.csv` được ghi với block 1 MiB, lấy từ `blockSizeBytes` của từng file.
  - Lỗi: 404 `NOT_FOUND`; 400 `BAD_PATH` (không bắt đầu bằng `/` hoặc có `..`), `NOT_A_DIRECTORY`, `NOT_A_FILE`; 503 `HDFS_UNREACHABLE`, `KAFKA_UNREACHABLE`. Spark không chạy thì `/api/spark/applications` trả `{"applications": []}` (200), không phải lỗi, vì UI Spark chỉ tồn tại khi có ứng dụng.
  - Topic nội bộ Kafka (tên bắt đầu `__`) bị ẩn. `SparkApplication.state` chỉ có `running` hoặc `completed` (Spark REST không báo failed ở mức ứng dụng).
- Phát hiện cho người dùng: trong HDFS `/raw` có file thừa tên `Dataset.csv` (có ký tự xuống dòng ở cuối tên, do một lệnh `hdfs dfs -put` từ file .sh/.bat CRLF). Backend trả nguyên tên; UI nên hiển thị an toàn. Thư mục `/stream_input` đang trống.
- Chưa làm: `spark.progress` (Structured Streaming không lộ tiến độ micro-batch qua REST của UI; cần listener hoặc đọc log, để sau).

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

(chưa có)
