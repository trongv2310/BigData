# Câu lệnh để dán vào agent

Trong Orca: thêm repo, tạo hai worktree (nhánh `feat/backend` cho Claude Code, `feat/frontend` cho Antigravity), mở mỗi agent trong worktree của nó rồi dán câu lệnh tương ứng. Antigravity có thể không tự đọc `AGENTS.md`, nên prompt của nó yêu cầu đọc rõ ràng.

## Claude (backend, lead) — worktree feat/backend

```
Bạn là Claude, backend và lead của dự án này. Đọc AGENTS.md, CLAUDE.md, docs/TASKS.md, docs/HANDOFF.md, docs/contract/openapi.yaml và docs/contract/events.md trước.
Việc đầu tiên: làm P1 phần backend trong docs/TASKS.md. Dựng FastAPI trong backend/, cổng 8000, chạy trong WSL; /api/services kiểm tra thật các cổng (9870, 9864, 8088, 9092, 4040); /ws/stream phát lại mock/stream-events.ndjson mỗi giây.
Mọi route phải đúng schema trong openapi.yaml; chạy docs/contract/validate_contract.py khi đụng hợp đồng. Không sửa frontend/. Cuối phiên ghi docs/HANDOFF.md. Nếu cần quyết định D1 đến D4, hỏi tôi thay vì tự chọn.
```

## Antigravity (frontend) — worktree feat/frontend

```
Bạn là agent frontend của dự án này. Trước hết hãy đọc các file sau trong repo (bạn không tự biết chúng): AGENTS.md, docs/TASKS.md, docs/HANDOFF.md, docs/contract/openapi.yaml, docs/contract/events.md, và các file trong mock/.
Chỉ được sửa trong frontend/ và mục "Frontend" của docs/HANDOFF.md. Không sửa hợp đồng; thiếu trường thì ghi yêu cầu vào HANDOFF.
Việc đầu tiên: làm P1 phần frontend. Dựng Vite + React + TypeScript trong frontend/, sinh kiểu từ docs/contract/openapi.yaml, đọc dữ liệu từ mock server (python mock/serve.py, cổng 8010) qua biến VITE_API_BASE, và dựng lớp StreamSource với ReplaySource đọc mock/stream-events.ndjson (VITE_STREAM=replay|ws). Trang Tổng quan hiện trạng thái dịch vụ và tin reading mới nhất. Nhãn trạm trong reading là nhãn mô phỏng (isSimulatedStation) và phải được ghi chú trên giao diện. Không tự tính status CO.
Giao diện tiếng Việt, dùng được ở độ rộng điện thoại. Cuối phiên ghi docs/HANDOFF.md và mở PR.
```

## Antigravity (frontend) — P4 và P5 (backend đã sẵn sàng, đã chạy thật trên máy người dùng 2026-10-12)

```
Bạn là agent frontend. Đọc lại AGENTS.md, docs/TASKS.md, docs/HANDOFF.md (mục Backend P4, P5, P7, P8 và mục Frontend của bạn), docs/contract/openapi.yaml và docs/contract/events.md. Chỉ sửa frontend/ và mục "Frontend" của docs/HANDOFF.md; thiếu trường thì ghi yêu cầu vào HANDOFF, đừng tự suy ra.
Việc: làm P4 và P5 phần frontend, thêm các tab vào khung hiện có, sinh lại kiểu bằng npm run generate:types.
1. Sơ đồ luồng (P4): GET /api/flows và /api/flows/hdfs-write để vẽ thành phần và các bước; chạy các bước theo sự kiện flow.step (chỉ có state "done", gom theo runId; bước 1..8) và hdfs.op trên /ws/stream. Ghi chú trên giao diện bước nào là quan sát thật, bước nào là suy ra (HANDOFF P4). Các luồng có status "planned" hiện là "chưa có". Đừng giả định reading đến sau bước 8 (có thể đến trước bước 4-6). Chạy thử: python3 infra/replay_producer.py --count 20 --interval 3 trong WSL rồi xem các bước sáng lên.
2. SQL: ô nhập, POST /api/sql (chỉ SELECT, bảng aqi, cột ghi trong mô tả SqlRequest.query), hiện bảng kết quả, rowCount, truncated, elapsedMs. Đặt timeout >= 60 giây (lần gọi đầu khởi động Spark ~8-10 giây). Hiện thông báo lỗi 400 SQL_ERROR / SQL_NOT_ALLOWED, 504 SQL_TIMEOUT, 503 SQL_UNAVAILABLE.
3. Dự đoán: biểu mẫu POST /api/predict (hour, month, temperatureC, windSpeedKmh, dayOfWeek tùy chọn). Đây là heuristic, KHÔNG phải ML: hiện model.kind, model.trainedOn, nhấn mạnh khoảng interval chứ không phải con số đơn, và ghi rõ nhiệt độ, gió, tháng chưa được dùng (HANDOFF P5).
4. Cửa sổ lệnh: xterm.js nối ws://localhost:8000/ws/terminal?token=...; người dùng dán token một lần (backend in ra console khi khởi động). Cửa sổ lệnh TẮT mặc định (đóng 4403, hiện lý do) và chỉ bật khi backend chạy với BACKEND_ALLOW_WRITES=1; xử lý các mã đóng 4401, 4403, 4408, 4409 và gửi resize khi khung đổi kích thước. Backend chỉ nhận Origin http://localhost:5173 hoặc http://127.0.0.1:5173 (ALLOWED_ORIGINS), nên chạy frontend ở cổng 5173.
Cũng làm (HANDOFF P7, P8): chuyển thẻ Spark sang ghi "ước tính" cho tốc độ nạp/xử lý và ghi rằng Query là tên ứng dụng; sửa nhật ký sự kiện hiện mỗi tin hai lần (StreamContext.tsx cleanup phải gọi source.stop(); StrictMode chạy effect hai lần); xử lý các cảnh báo lint.
Không bịa số liệu; dữ liệu mô phỏng phải có nhãn. Cuối phiên: npm run build sạch, xem trên trình duyệt với VITE_API_BASE=http://localhost:8000 VITE_STREAM=ws (backend thật trong WSL), ghi docs/HANDOFF.md và mở PR.
```

## Antigravity (frontend) — P11-P12: dữ liệu sống qua Kafka (backend đã sẵn sàng và đã chạy thật)

```
Bạn là agent frontend. Đọc lại AGENTS.md, docs/HANDOFF.md (mục Backend P11 và P12), docs/contract/openapi.yaml và events.md. Chỉ sửa frontend/ và mục "Frontend" của HANDOFF; thiếu trường thì ghi yêu cầu, đừng tự suy ra. Sau khi sửa: npm run generate:types, lint và build sạch, xem trên trình duyệt với backend thật (VITE_API_BASE=http://localhost:8000 VITE_STREAM=ws), rồi mở PR.
Backend đã đưa Kafka vào luồng thật: Open-Meteo -> Kafka -> backend/Spark -> HDFS. Việc cho giao diện:
1. FlowsTab phải LỌC flow.step theo flowId: hiện context gom mọi flow.step theo runId rồi gắn vào các bước của hdfs-write, nên khi backend phát luồng khác thì sơ đồ HDFS sáng sai. Lưu (flowId, runId) và chỉ dùng các run của luồng đang chọn. Sau khi xong, ghi vào HANDOFF để lead bật FLOWS_EXTRA=1 ở backend.
2. Vẽ hai luồng mới (GET /api/flows/kafka-produce và /api/flows/spark-streaming khi status là available; kind mới: kafka-broker, spark, storage): kafka-produce có 2 bước (bước 2 ACK kèm offset là quan sát thật, bước 1 gửi là suy ra), spark-streaming có 4 bước và tất cả đều SUY RA từ việc micro-batch (job Spark) thành công. runId có dạng "kafka-p0-o7" và "spark-batch-12". Ghi chú rõ thật/suy ra như hdfs-write.
3. Tổng quan: với source = live-open-meteo, hiện tuổi của dữ liệu ("quan sát cách đây N phút") và nhắc rằng nguồn cập nhật tối đa 15 phút một lần (CO mỗi giờ, thời tiết mỗi 15 phút) và là số của mô hình; đừng gọi là "đo trực tiếp từng giây". Tiêu đề thẻ chỉ nên là "thời gian thực" khi vừa có sự kiện reading mới.
4. SqlTab: thêm bảng `live` (dữ liệu sống Spark ghi từ Kafka, các cột trong mô tả SqlRequest.query của openapi) vào thẻ tham khảo và vài truy vấn mẫu; ghi chú không trộn với bảng aqi (khác nguồn: Open-Meteo đo được 1678-2508 µg/m³ ngày 12-13/10/2026, trung bình Kaggle từ 07/2025 là ~300). Bảng live có thể chưa tồn tại: lỗi SQL_ERROR 400 khi đó là bình thường, hiện thông báo dễ hiểu.
5. KafkaTab: consumerGroups giờ có thể có mục tên "spark:co_stream" (độ trễ lấy từ checkpoint của Spark, không phải consumer group thật): ghi chú như vậy.
Không bịa số liệu; dữ liệu mô phỏng hoặc suy ra phải có nhãn.
```

## Antigravity (frontend) — P15: ngưỡng CO mới (D3 đã chốt)

```
Bạn là agent frontend. Đọc lại AGENTS.md mục 5, docs/TASKS.md (D3 đã chốt, mục "Còn lại"), docs/HANDOFF.md (Backend P15) và openapi.yaml (CoStatus). Chỉ sửa frontend/ và mục "Frontend" của HANDOFF.
Backend đổi ngưỡng CO sang thang OpenWeather: SAFE < 4400, WARNING từ 4400, DANGER từ 12400 µg/m³ (ví dụ 1280 µg/m³ giờ là SAFE). Việc: (1) sửa đoạn chữ cố định "500 và 1000 µg/m³ ... kế thừa từ spark_streaming_consumer.py" ở AqiTab.tsx (~dòng 442) thành mô tả đúng; chỉ ghi ngưỡng như thông tin (thang CO của OpenWeather), vẫn hiển thị status do backend trả, không tự tính, không gọi là quy chuẩn Việt Nam; có thể nói 4400 xấp xỉ hướng dẫn 24 giờ 4 mg/m³ của WHO 2021. (2) Sửa ghi chú ở Tổng quan "chưa có quy chuẩn chính thức" cho khớp (đừng khẳng định đó là quy chuẩn). (3) npm run generate:types, lint và build sạch, xem trên trình duyệt với backend thật, ghi HANDOFF và mở PR.
```

## Lead duyệt PR của frontend

```
Đọc diff PR frontend. Kiểm tra: chỉ đổi trong frontend/; kiểu sinh từ openapi chứ không viết tay; không tự suy trường ngoài hợp đồng; nhãn mô phỏng và ghi chú điểm gãy 01/07/2025 có mặt; không có khóa API; chạy được với mock. Báo lại danh sách việc cần sửa hoặc gộp.
```
