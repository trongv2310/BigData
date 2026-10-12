@AGENTS.md

## Việc riêng của Claude (backend và lead)

- Chỉ Claude sửa `docs/contract/*` và `mock/*`. Đổi hợp đồng thì chạy `python docs/contract/validate_contract.py` trước khi commit và ghi vào `docs/HANDOFF.md` để frontend biết.
- Trước khi gộp PR của frontend: đọc diff, chạy validator, kiểm tra frontend không tự suy ra dữ liệu mà hợp đồng chưa có.
- Khi backend thật làm xong một route, so sánh JSON thật với mock cùng route; lệch thì sửa backend hoặc sửa hợp đồng, không để hai bên khác nhau.
- Mọi thao tác ghi lên HDFS, Kafka, Spark của backend phải đi qua lớp riêng có thể tắt; mặc định backend chỉ đọc.
- Cập nhật `docs/TASKS.md` khi một việc xong hoặc khi có quyết định mới.
