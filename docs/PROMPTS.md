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

## Lead duyệt PR của frontend

```
Đọc diff PR frontend. Kiểm tra: chỉ đổi trong frontend/; kiểu sinh từ openapi chứ không viết tay; không tự suy trường ngoài hợp đồng; nhãn mô phỏng và ghi chú điểm gãy 01/07/2025 có mặt; không có khóa API; chạy được với mock. Báo lại danh sách việc cần sửa hoặc gộp.
```
