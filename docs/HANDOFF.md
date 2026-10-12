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

### 2026-10-12 — P4: /api/flows, hdfs.op và flow.step thật từ log NameNode
- Xong: `GET /api/flows`, `GET /api/flows/hdfs-write` (các id khác trả 404 `NOT_FOUND`; `hdfs-read`, `kafka-produce`, `spark-streaming`, `sparksql` vẫn `planned`). Trên `/ws/stream` (chế độ live) có thêm `hdfs.op` và `flow.step`. Backend đọc (chỉ đọc file) log NameNode `~/hadoop/logs/hadoop-*-namenode-*.log`, bắt đầu từ cuối file nên không phát lại lịch sử. Không đổi cấu hình Hadoop nào.
- Thử thế nào: `python3 backend/tools/watch_stream.py` (backend phải chạy), rồi ở cửa sổ WSL khác: `hdfs dfs -put -f file.json /stream_input/`.
- **Quan sát thật hay suy ra** (UI cần ghi chú "suy ra" cho các bước dưới, theo events.md):
  - Thật: bước 2 (dòng `allocate`), bước 7 (dòng `completeFile`), bước 8 (đổi tên `._COPYING_`: kiểm file tên cuối đã tồn tại qua WebHDFS, hoặc dòng audit `rename` nếu audit bật), `hdfs.op` `addBlock` và `close`, `hdfs.op` `rename`.
  - Suy ra: bước 1 (log mặc định không ghi `create`; suy từ lần cấp block đầu tiên của file, vì không thể cấp block cho file chưa tạo), bước 3 (cùng dòng allocate), bước 4, 5, 6: sau khi file đóng, bảng block của NameNode (WebHDFS) xác nhận mỗi block có độ dài và có DataNode giữ bản sao, nên suy ra dữ liệu đã tới DataNode, được ACK, DataNode đã báo blockReceived. Không có gói tin nào được quan sát, và log của DataNode 2-4 không được ghi (chúng chạy bằng `hdfs datanode` trực tiếp, log đi vào /dev/null).
  - Không phát: bước 9 (checkpoint nền, không gắn với một lượt ghi).
- Quy ước cho frontend:
  - Chỉ có state `done` (không có `started`): backend biết sau khi bước xảy ra. UI coi `done` đứng một mình là bước đã xong.
  - Thứ tự phát của file nhiều block: 1, rồi (2,3) cho từng block, rồi (4,5,6) cho từng block, 7, 8. File một block (như `/stream_input/*.json`) ra đúng 1..8. Mỗi lượt ghi một `runId` (`run-xxxxxxxx`), gắn theo đường dẫn file.
  - `hdfs.op.path` là đường dẫn đang ghi, có đuôi `._COPYING_` cho đến lúc đổi tên; `rename` mang đường dẫn cuối. `addBlock.datanode` là DataNode đầu pipeline, cùng dạng tên với `/api/hdfs/datanodes` (`host:cổng`); `close.bytes` là tổng độ dài các block, null nếu NameNode chưa trả được.
  - Thành phần `datanode-1` trong định nghĩa luồng là "DataNode đầu pipeline"; tên thật nằm ở `hdfs.op.datanode`. Nhãn bước 4 đã bỏ chuyện "cổng 9866" vì cụm có 4 DataNode (mock `flow-hdfs-write.json` và `flows.json` đã đổi theo, có test giữ backend và mock không lệch).
  - Mọi ghi của Spark (thư mục `_temporary`, `_SUCCESS`, parquet) cũng ra sự kiện; UI nên lọc theo `path` nếu chỉ muốn thấy `/stream_input`.
- `create`, `mkdir`, `delete` chỉ phát khi bật audit log của NameNode (mặc định tắt: `hdfs.audit.logger=INFO,NullAppender`). Mã đọc audit đã viết và test với dòng mẫu đúng định dạng, **chưa thử với audit log thật**. Bật audit là đổi cấu hình và khởi động lại NameNode, nên để người dùng quyết định (cần đặt `HDFS_AUDIT_LOGGER=INFO,RFAAUDIT` cho NameNode).
- Kiểm tra đã làm: 61 test; chạy lại các dòng log thật của lượt `put /raw/Dataset.csv` (2026-10-01) qua bộ theo dõi nối NameNode thật cho ra bước 1..8, 4 `addBlock` đúng blockId/DataNode, `close.bytes` = 3 179 004 (đúng kích thước file). Chưa chạy với một `put` thật đang diễn ra và chưa nhìn qua giao diện.
- Chưa làm: `flow.step` cho các luồng khác (đọc HDFS, Kafka, Spark); luồng Kafka chờ quyết định D1 chính thức.

### 2026-10-12 — P3: /api/aqi/series, /hourly-profile, /latest (bộ Kaggle, D4 tạm)
- Xong: ba route, đúng schema, kiểm bằng `check_live.py` và 43 test (`cd backend && python3 -W ignore -m unittest discover tests`). Đọc file `hcmc_aqi_dataset_after_preprocess_final.csv` (đường dẫn `AQI_CSV_PATH`); không có file thì đọc `/raw/Dataset.csv` trên HDFS, đã kiểm hai nguồn cho kết quả y hệt.
- **Mock đã đổi** (`mock/aqi-series-month.json`, `mock/aqi-hourly-profile.json`): sinh lại từ chính mã backend bằng `backend/tools/regen_mock_aqi.py` để mock và backend cùng số. Ghi chú cũ trong mock sai ("đỉnh quanh 18 giờ, thấp nhất 7 giờ" là của dữ liệu chưa đổi sang giờ VN). Số thật theo giờ VN: trước 01/07/2025 đỉnh 23 giờ, thấp nhất 13 giờ; từ 01/07/2025 đỉnh 7 giờ, thấp nhất 14 giờ. Backend tự sinh `note` từ số tính được, đừng chép cứng lại. Chuỗi `source` không còn slug `kienuit/...` vì tôi không xác minh được nó.
- Quy ước:
  - `series`: `t` là thời điểm bắt đầu ô (giờ VN, +07:00), giá trị là trung bình làm tròn 2 chữ số. Ô nằm trong `[from, to]` theo thời điểm bắt đầu; `from`/`to` không có offset thì coi là +07:00. Mặc định `metric=co`, `granularity=month` cho 48 điểm (2022-04 đến 2026-03).
  - Ô mà bộ dữ liệu phủ dưới 50% bị bỏ (sau khi +7 giờ, tháng 04/2026 chỉ còn 7 giờ và ngày 01/04/2026 còn 7 giờ; ngày đầu 01/04/2022 có 17 giờ nên giữ). `granularity=hour` không giới hạn nên khoảng toàn bộ trả ~35 000 điểm; nên truyền `from`/`to`.
  - Tham số sai trả 400 `BAD_REQUEST` (schema `Error`); không nạp được dữ liệu trả 503 `DATASET_UNAVAILABLE`.
  - `latest`: ưu tiên bản ghi của file mới nhất trong HDFS `/stream_input`. Nếu thư mục trống hoặc HDFS tắt thì trả **dòng cuối bộ dữ liệu** (`source: replay-dataset`, `station: "Bộ dữ liệu Kaggle (không có trạm)"`, `datetime` là 2026-04-01T06:00+07:00, tức là cũ). Hiện `/stream_input` trống nên là trường hợp này. UI nên hiển thị `datetime` và `source`, không gọi là "trực tiếp".
- Phát hiện về dữ liệu (cần người dùng biết):
  - Cột `datetime` của Kaggle là UTC: tương quan CO giữa Kaggle và `datetime_utc` của OpenWeather cao nhất ở độ lệch 0 giờ (0,711), giảm dần ở ±1, ±2 giờ. Vì vậy phải +7 giờ.
  - Các cột `hour`, `month`, `dayofweek` trong file tính theo **giờ UTC**, không phải giờ VN. Khi làm `/api/predict` (P5) không được trộn chúng với giờ địa phương.
  - Điểm gãy không phải một bước đơn: theo trung bình tháng CO đã giảm từ 03/2025 (02/2025: 1327, 03/2025: 657), đứng ở ~650 đến 06/2025, rồi giảm nữa còn ~266 vào 07/2025. Mốc 01/07/2025 vẫn là một lần giảm rõ (654 → 266), nhưng biểu đồ không nên khẳng định đó là thay đổi duy nhất. Chưa kết luận được nguyên nhân (đổi nguồn mô hình của OpenWeather hay thay đổi thật); đừng viết lý do lên giao diện.
  - 35 064 dòng liên tục từng giờ, không thiếu, không trùng, không ô trống.
- Cần frontend: biểu đồ CO không nối liền qua 01/07/2025 (như AGENTS.md mục 5) và ghi `source` của chuỗi; trạm luôn có nhãn "mô phỏng" (`isSimulatedStation` luôn `true`, kể cả bản ghi live Open-Meteo).
- Tôi đã làm gì trên máy của người dùng khi thử: bật HDFS bằng `bash ~/start-4nodes.sh` (đang để chạy), chạy một ứng dụng PySpark thử `spark.range(1000).count()` trong 75 giây (không đụng HDFS), không bật Kafka, không ghi gì lên HDFS/Kafka/Spark.

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
- Phát hiện cho người dùng: trong HDFS `/raw` có file thừa tên `Dataset.csv
` (có ký tự xuống dòng ở cuối tên, do một lệnh `hdfs dfs -put` từ file .sh/.bat CRLF). Backend trả nguyên tên; UI nên hiển thị an toàn. Thư mục `/stream_input` đang trống.
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
