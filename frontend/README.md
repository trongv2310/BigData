# Bảng điều khiển Giám sát Khí CO TP.HCM — Frontend

Ứng dụng Frontend cho đồ án Big Data giám sát nồng độ CO tại Thành phố Hồ Chí Minh.
Xây dựng bằng **React 19 + TypeScript + Vite + Tailwind CSS + Chart.js**.

## 1. Tính năng đã hoàn thành (P1, P2, P3)

- **P1 — Tổng quan & Luồng Dữ liệu**:
  - Tích hợp lớp `StreamSource`: tự động chuyển đổi giữa `ReplaySource` (đọc `mock/stream-events.ndjson`) và `WebSocketSource` (`ws://localhost:8000/ws/stream`) qua biến môi trường `VITE_STREAM`.
  - Thanh sự kiện trực tiếp (Event Ticker) hiển thị mọi tin tức mới trong luồng (`reading`, `hdfs.op`, `spark.progress`, `kafka.stats`, `service.state`).
  - Trang Tổng quan: Bản ghi đo CO thời gian thực, cảnh báo rõ ràng **Trạm mô phỏng** khi `isSimulatedStation: true`, trạng thái CO (`status`), các chỉ số môi trường (gió, nhiệt độ, chỉ số tích tụ CO), nguồn dữ liệu (`source`).
  - Lưới trạng thái toàn bộ dịch vụ Big Data (NameNode, DataNode, YARN, Kafka, Spark, Airflow, Backend) tự động cập nhật khi có sự kiện `service.state`.
- **P2 — Giám sát Hạ tầng Big Data**:
  - **HDFS**: Tóm tắt cụm NameNode JMX, thanh phần trăm dung lượng, danh sách DataNodes (dung lượng, block count, heartbeat), trình duyệt cây thư mục WebHDFS tương tác, modal xem phân rã Blocks của file và vị trí DataNodes, nhật ký thao tác `hdfs.op`.
  - **Kafka**: Broker kết nối, tốc độ gửi tin realtime và tổng độ trễ từ `kafka.stats`, danh sách topics, số partitions, replication factor, consumer groups và lag.
  - **Spark**: Tiến trình Structured Streaming micro-batch từ `spark.progress`, danh sách ứng dụng Spark, số jobs running/succeeded/failed, liên kết Spark UI.
- **P3 — Phân tích Dữ liệu CO & AQI**:
  - Chuỗi thời gian nồng độ khí theo tháng / ngày / giờ (`/api/aqi/series`).
  - **Bắt buộc**: Tách rời và đánh dấu điểm gãy dữ liệu ngày **01/07/2025** (CO giảm mạnh do đổi nguồn/mô hình đo), **không nối mượt** qua điểm gãy kèm chú thích khoa học rõ ràng.
  - Nhịp sinh học 24 giờ (`/api/aqi/hourly-profile`): So sánh chu kỳ ngày - đêm giữa 2 giai đoạn (trước và sau 01/07/2025).

## 2. Hướng dẫn chạy

### Chạy với Mock Server (Mặc định)
```bash
# Terminal 1: Chạy mock server (cổng 8010)
python mock/serve.py

# Terminal 2: Chạy frontend (cổng 5173)
cd frontend
npm install
npm run dev
```

### Chuyển sang Backend thật (Cổng 8000)
Tạo hoặc cập nhật file `frontend/.env`:
```env
VITE_API_BASE=http://localhost:8000
VITE_STREAM=ws
VITE_WS_URL=ws://localhost:8000/ws/stream
```

### Sinh lại kiểu TypeScript từ hợp đồng
```bash
npm run generate:types
```

### Kiểm tra build production
```bash
npm run build
```
