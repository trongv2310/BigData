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

## Lead duyệt PR của frontend

```
Đọc diff PR frontend. Kiểm tra: chỉ đổi trong frontend/; kiểu sinh từ openapi chứ không viết tay; không tự suy trường ngoài hợp đồng; nhãn mô phỏng và ghi chú điểm gãy 01/07/2025 có mặt; không có khóa API; chạy được với mock. Báo lại danh sách việc cần sửa hoặc gộp.
```
