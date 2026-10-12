# frontend/ (Antigravity)

Dự kiến: Vite + React + TypeScript, xterm.js cho cửa sổ lệnh, biểu đồ nhẹ (uPlot hoặc Chart.js). Đọc dữ liệu từ `VITE_API_BASE` (mặc định mock `http://localhost:8010`) và `VITE_STREAM=replay|ws`.

Quy tắc: kiểu dữ liệu sinh từ `docs/contract/openapi.yaml` (ví dụ `npx openapi-typescript`), không tự viết tay. Không tự tính trạng thái CO, chỉ hiển thị `status` do backend trả.

Chưa có mã. Làm theo `docs/TASKS.md`, giai đoạn P1.
