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

### 2026-10-12 — P6: rà soát PR #1 của frontend (đã gộp vào main), rebase nhánh backend
- Xong: đọc diff PR #1 (chỉ đụng `frontend/` và mục Frontend của HANDOFF, đúng quyền sở hữu); `npm ci`, `npm run generate:types` và `npm run build` trong một bản sao tạm đều sạch; validator OK; 87 test backend vẫn qua sau khi rebase `feat-backend` lên `origin/main` (5 commit backend nằm trên merge #1; mục Frontend của HANDOFF giữ nguyên văn). Chưa mở PR của backend.
- Chưa làm: chưa mở giao diện trong trình duyệt với backend thật (không có công cụ trình duyệt trong phiên này), nên "không lỗi console, thu nhỏ vẫn dùng được" chưa được kiểm. Frontend mới làm P1-P3 nên chưa có gì để xem cho `/api/flows`, `/api/sql`, `/api/predict`, `/ws/terminal`.
- **Cần frontend sửa (tôi không sửa `frontend/`):**
  1. `AqiTab.tsx` dòng ~241, hộp cảnh báo điểm gãy viết các điều không có trong dữ liệu hoặc trong hợp đồng: "sụt giảm mạnh **đột ngột** kể từ 01/07/2025", "từ ~1500-2000 xuống ~250-350", và nguyên nhân "do sự thay đổi thuật toán / mô hình đo lường của nhà cung cấp". Số thật (theo giờ VN): CO đã giảm từ 03/2025 (trung bình tháng 02/2025: 1327, 03/2025: 657), đứng ~650 đến 06/2025, rồi còn ~266 vào 07/2025; trung bình cả giai đoạn trước 01/07/2025 là 1344, sau là 303; nguyên nhân chưa biết (AGENTS.md mục 8: đừng bịa). Đề nghị hiển thị chuỗi `note` của `/api/aqi/hourly-profile` (backend tính từ dữ liệu, không chép cứng) thay vì tự viết số và lý do. Phần tách đoạn biểu đồ tại 01/07/2025 làm đúng, giữ nguyên.
  2. `OverviewTab.tsx` dòng ~134: câu giải thích "nhãn được gán luân phiên theo luồng phát lại" chỉ đúng khi `source = replay-dataset` có trạm luân phiên. Với `live-open-meteo` (trạm "TP.HCM (Open-Meteo)") hoặc dòng cuối bộ dữ liệu (`station: "Bộ dữ liệu Kaggle (không có trạm)"`) thì không phải. Đề nghị nội dung theo `source`, vẫn luôn hiện ghi chú "mô phỏng" vì `isSimulatedStation` luôn `true` (D2 tạm).
  3. Chạy lại `npm run generate:types`: mô tả của `SqlRequest.query` và `PredictResponse.model.trainedOn` trong openapi đã đổi (chỉ chữ, không đổi cấu trúc).
  4. Khi chuyển sang backend thật (`VITE_API_BASE=http://localhost:8000`, `VITE_STREAM=ws`): luồng live không phát `spark.progress` (chưa làm được, xem mục P2), nên thẻ Structured Streaming của `SparkTab` phải có trạng thái rỗng rõ ràng ("chưa có dữ liệu"), đừng để trống hay hiện số 0 như thật. `/api/aqi/latest` hiện trả dòng cuối bộ dữ liệu (datetime 2026-04-01) vì `/stream_input` đang trống: hiển thị `datetime` và `source`, đừng gọi là "trực tiếp". `/api/kafka/topics` trả 503 `KAFKA_UNREACHABLE` khi Kafka tắt (hiện đúng như vậy), nên hiện thông báo lỗi thay vì bảng trống.
  5. Tên file `Dataset.csv` trong `/raw` có ký tự xuống dòng ở cuối; hiển thị an toàn (ví dụ thay bằng `` có thể thấy được), nhưng khi gọi `/api/hdfs/blocks` thì gửi nguyên tên đã mã hóa.
- P4 và P5 của frontend: định nghĩa luồng, sự kiện `hdfs.op`/`flow.step` (chỉ có state `done`, một số bước là suy ra), SQL, dự đoán (heuristic, không phải ML), cửa sổ lệnh (tắt mặc định) đều đã sẵn ở backend; chi tiết ở các mục P4, P5 bên dưới.

### 2026-10-12 — P5: /api/predict, /api/sql, /ws/terminal
- Xong: ba tính năng, đúng hợp đồng (87 test qua, `check_live.py` OK mọi route trừ Kafka vì Kafka chưa chạy). Chạy thật trên máy: SparkSQL đọc `/raw/Dataset.csv` trên HDFS và trung bình CO theo tháng khớp từng số với `/api/aqi/series`; cửa sổ lệnh qua uvicorn thật chạy được `hdfs dfs -ls /raw`, token sai bị đóng 4401, Origin sai bị 403.
- **`/api/predict` là heuristic, không phải ML, và gần như không có khả năng dự đoán.** Tôi đã thử trên dữ liệu từ 01/07/2025 (giờ VN): hồi quy log-tuyến tính theo giờ + nhiệt độ + gió cho R² ≈ 0 trên 60 ngày cuối; kiểm tra xen kẽ theo tuần cho thấy trung bình theo tháng, theo giờ, và cả tích tháng×giờ×gió×nhiệt độ đều không tốt hơn lấy trung bình chung (R² từ -0,15 đến 0). Gió và nhiệt độ gần như không tương quan với CO ở giai đoạn này (log gió: -0,10; nhiệt độ: 0,03). Vì vậy endpoint trả **trung vị và khoảng 10%-90% thật của CO quanh giờ được hỏi (±1 giờ), chỉ lấy từ giai đoạn sau 01/07/2025**; tháng, thứ, nhiệt độ, gió được kiểm hợp lệ nhưng KHÔNG dùng. Dùng cả dữ liệu trước điểm gãy sẽ cho số gấp 3-4 lần, nên không dùng. `model.kind = "heuristic"`, `model.trainedOn` mang chuỗi giải thích đầy đủ (kể cả sai số kiểm tra); tôi đã sửa mô tả trường này trong openapi từ "null với heuristic" thành "mô tả dữ liệu mô hình dựa vào". Giao diện nên hiển thị `trainedOn`, nhấn mạnh khoảng chứ không phải con số đơn, và ghi rõ nhiệt độ/gió chưa được dùng. Nếu người dùng vẫn muốn một mô hình ML để báo cáo, cần nói rõ kết quả thử nghiệm này; đó là quyết định của người dùng.
- **`/api/sql`**: SparkSQL thật. Lần gọi đầu khởi động Spark (~8 giây, JVM ~1 GB RAM), các lần sau ~0,1 giây; frontend đặt timeout >= 60 giây. Chỉ một câu `SELECT`/`WITH...SELECT`; chặn `;` nhiều câu, backtick, từ khóa ghi (`insert`, `drop`, `set`...) và hàm chạy mã JVM (`reflect`, `java_method`); phiên Spark thêm `runSQLOnFiles=false` nên không đọc file tùy ý, không UI (không chiếm cổng 4040). Quá `SQL_TIMEOUT_SECONDS` (60) thì hủy, trả 504 `SQL_TIMEOUT`. Lỗi câu lệnh 400 `SQL_ERROR` (kèm dòng đầu thông báo của Spark) hoặc `SQL_NOT_ALLOWED`; không dựng được Spark/đọc được HDFS 503 `SQL_UNAVAILABLE`. Timestamp trả về là chuỗi `yyyy-MM-dd HH:mm:ss`; NaN, Infinity thành `null`. Bảng duy nhất `aqi`, cột ghi trong mô tả `SqlRequest.query` của openapi (`datetime_utc`, `datetime_vn` = UTC+7, `hour_vn`, `month_vn`, `year_vn`, rồi các chỉ số). Tôi cố ý bỏ các cột `hour`, `month`, `dayofweek` của file vì chúng tính theo UTC. Frontend nên hiển thị tên bảng và cột làm gợi ý vì hợp đồng không có endpoint liệt kê bảng.
- **`/ws/terminal`**: pty `bash -i` trong thư mục gốc repo, đúng events.md mục 2. Bảo mật: chỉ client loopback; sai/thiếu Origin bị từ chối lúc bắt tay (HTTP 403); token ngẫu nhiên mỗi lần khởi động, in ra console, sai thì đóng 4401; một phiên (4409 nếu có phiên khác); tự đóng sau 30 phút không hoạt động (4408); `HISTFILE=/dev/null`; backend không ghi nội dung gõ vào log. **Tắt mặc định** (đóng 4403 kèm lý do), vì shell ghi được lên HDFS/Kafka/Spark mà CLAUDE.md yêu cầu backend mặc định chỉ đọc: chạy với `BACKEND_ALLOW_WRITES=1` để bật. Các mã đóng đã ghi vào `docs/contract/events.md`. Lưu ý: dòng truy cập của uvicorn có in `?token=...` ra console (cùng nơi in token); không chạy backend với log ra nơi công khai, và luôn dùng `--host 127.0.0.1`.
- Mock đổi: `mock/predict.json` (kết quả thật của heuristic), `mock/sql.json` (kết quả thật của một truy vấn theo tháng, cột `year_vn, month_vn, avg_co, n`).
- Chưa kiểm: cửa sổ lệnh chỉ thử bằng client websockets/TestClient, chưa qua xterm.js; chưa thử `Ctrl+C`, chương trình toàn màn hình (vim, htop) và dán nhiều dòng. Kafka vẫn chưa kiểm với broker thật (xem mục P2).
- Còn lại của P5 (cả hai bên): kịch bản demo, ảnh chụp cho báo cáo, kiểm tra cuối.

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

### 2026-10-12 16:45 — Sửa các điểm theo rà soát P6 của Claude
- Xong:
  + 1. `AqiTab.tsx`: Hộp cảnh báo điểm gãy 01/07/2025 hiển thị trực tiếp chuỗi `note` từ `/api/aqi/hourly-profile` (do backend sinh từ dữ liệu thật), không tự viết số suy đoán hay nguyên nhân thay đổi thuật toán. Giữ nguyên việc tách đoạn biểu đồ tại 01/07/2025 (không nối mượt).
  + 2. `OverviewTab.tsx`: Ghi chú "Trạm mô phỏng" đã điều chỉnh linh hoạt theo `source` (phân biệt rõ `replay-dataset` luân phiên trạm, dòng cuối bộ dữ liệu Kaggle không có trạm, và `live-open-meteo` mô phỏng vùng TP.HCM). Tiêu đề thẻ CO hiển thị "Bản ghi đo thời gian thực" khi có stream hoặc "Bản ghi đo gần nhất" khi lấy từ API nền, không gọi là "trực tiếp" khi thư mục stream trống.
  + 3. Sinh lại kiểu `src/types/api.ts` từ `openapi.yaml` mới (`SqlRequest.query`, `PredictResponse.model.trainedOn`).
  + 4. `SparkTab.tsx`: Thêm trạng thái rỗng tường minh (empty state) cho thẻ Structured Streaming khi luồng live chưa phát `spark.progress`, không để ô trống hay hiển thị số 0.
  + 5. `KafkaTab.tsx`: Hiển thị thông báo lỗi chi tiết (nhắc bật broker khi gặp 503 `KAFKA_UNREACHABLE`) thay vì để bảng topics trống.
  + 6. `HdfsTab.tsx`: Tên tệp tin và đường dẫn chứa ký tự điều khiển (như `\n` trong `Dataset.csv\n` ở `/raw`) được escape an toàn khi render hiển thị (`\n`, `\r`), đồng thời khi gửi gọi `/api/hdfs/blocks` vẫn giữ nguyên chuỗi gốc đã qua `encodeURIComponent`.
- Chạy thế nào:
  + `npm run generate:types`
  + `npm run build` (build thành công sạch sẽ trong 723ms)
- Chưa làm / chưa kiểm với backend thật:
  + Backend thật chạy trong WSL (`127.0.0.1:8000`) cần người dùng bật và kiểm tra qua trình duyệt. Chưa kiểm được với backend thật vì môi trường hiện tại đang chạy mock server 8010.
  + Kafka thật đang tắt trên máy nên chưa kiểm được dữ liệu thật của Kafka topics.
  + Chưa làm frontend P4 (Sơ đồ luồng) và P5 (Terminal xterm.js, SQL, Dự đoán).
- Cần bên kia: Không có. Contract và mock hoàn toàn khớp.

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

