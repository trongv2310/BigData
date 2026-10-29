# Quy ước chung cho loạt video Big Data

Áp dụng cho mọi file `01_…` đến `08_…`. Bước này chỉ là kịch bản, chưa có mã dựng video.

## Nguồn số liệu

- `F:\HK1_Nam04\BigData\learning_bigdata_interactive.html` (Bách khoa Big Data CO, 9 chương), gọi tắt là **[TĐ]**.
- `co-hcmc-dashboard/docs/HANDOFF.md` **[HO]** và `co-hcmc-dashboard/infra/README.md` **[INFRA]**.
- Mọi con số trên màn hình phải có trong ba nguồn trên. Hằng số mặc định của Hadoop, Kafka, Spark (128 MiB, 3 giây, 7 ngày…) là **mặc định của phần mềm**, không phải số đo trên cụm; khi hiện, ghi nhỏ "mặc định".
- Ví dụ đời thường chỉ mang tính minh họa. Số trong ví dụ đời thường lấy đúng như [TĐ] đã dùng (10 000 bài thi, 2 000 cửa hàng, 200 mili giây…); ví dụ đời thường nào [TĐ] không có số thì giữ định tính, không thêm số mới.

## Những câu không được nói sai

| Chủ đề | Được nói | Không được nói |
|---|---|---|
| Cụm chạy chính | 4 DataNode chạy trên **một máy** WSL2/Ubuntu (khoảng 7,7 GB RAM), cổng 9864, 9874, 9884, 9894 | "cụm nhiều máy", "chịu lỗi phần cứng" |
| Cụm Docker | Cụm Docker 5 DataNode là **môi trường thử nghiệm của thành viên 1 và 3**, không phải cụm chạy chính; mọi số liệu trong video lấy từ cụm 4 DataNode | dùng số của cụm Docker, gọi cụm Docker là cụm của đồ án |
| Điểm gãy 01/07/2025 | CO giảm 77,4% (1 343,8 xuống 303,3 µg/m³), có ở cả bộ Kaggle lẫn bản raw OpenWeather; **nguyên nhân chưa xác định** | "OpenWeather đổi hiệu chuẩn", "không khí sạch hơn", bất kỳ nguyên nhân nào khác |
| Kích thước dữ liệu | 35 064 dòng, 3 179 004 byte (khoảng 3 MB) là dữ liệu nhỏ; đồ án dựng kiến trúc chạy được khi dữ liệu lớn lên | "dữ liệu lớn" khi nói về bộ Kaggle |
| Luồng phát lại | "mô phỏng", "phát lại"; nhãn trạm là nhãn gán xoay vòng | "thời gian thực", "trạm đo thật" |
| Ngưỡng cảnh báo | thang CO của OpenWeather: WARNING từ 4 400, DANGER từ 12 400 µg/m³; 4 400 xấp xỉ hướng dẫn 24 giờ 4 mg/m³ của WHO 2021 | "quy chuẩn Việt Nam", "theo WHO" |
| Giờ | cột `datetime` của Kaggle là UTC; hiển thị giờ Việt Nam = UTC + 7 | hiện giờ UTC mà không ghi chú |

## Quy ước hình ảnh (dùng xuyên suốt 8 video)

- **Màu theo thành phần (chốt 24/10/2026, dùng chung mọi video, mã ở `video/src/theme/palette.ts`):** HDFS xanh dương `#3B82F6`; Kafka xanh lá `#22C55E`; Spark cam `#F97316`; MLlib tím `#A855F7`; Airflow xanh ngọc `#06B6D4`; backend, dashboard xám `#94A3B8`.
- **Màu theo nguồn dữ liệu** (khác màu thành phần để không nhầm "dữ liệu thật" với "Kafka"): hồng `#F472B6` = dữ liệu thật OpenWeather; trắng xám `#CBD5E1` = luồng phát lại, luôn kèm nhãn "MÔ PHỎNG".
- **Màu trạng thái:** vàng = đang chạy; xanh lá nhạt = xong, sống; đỏ = lỗi, chết, mất; xám mờ = đã xử lý hoặc chưa tới lượt.
- **Hình khối cố định:** NameNode là quầy thủ thư có cuốn sổ; DataNode là kệ sách đánh số cổng; block là viên gạch có mã; Kafka topic là cuốn sổ kẻ dòng có số offset ở lề; Spark driver là bếp trưởng đội mũ cao, executor là phụ bếp; Airflow là bảng phân công có mũi tên.
- **Nhãn "minh họa":** khi vẽ điều không đúng với cụm đồ án (replication 3, nhiều partition, nhiều máy) phải có góc nhãn "Minh họa, cụm đồ án dùng …".
- **Nhãn "suy ra":** bước nào dashboard suy ra chứ không quan sát trực tiếp (ví dụ blockReceived, 4 bước của micro-batch) vẽ viền đứt và ghi "suy ra".
- **Biểu đồ CO theo thời gian:** luôn có vạch đỏ ở 01/07/2025, hai đoạn **không nối** qua vạch, chú thích "Điểm gãy, nguyên nhân chưa xác định".
- **Định dạng số:** dấu cách ngăn nghìn (35 064), dấu phẩy thập phân (38,91), đơn vị µg/m³.
- Mỗi cảnh có ô "Đời thường" (biểu tượng nhà) và ô "Trong đồ án" (biểu tượng khung xanh như [TĐ]) để người xem nhận ra hai lớp ví dụ.

## Cấu trúc mỗi file

1. Thời lượng mục tiêu và bảng phủ khái niệm (mỗi khái niệm: ví dụ đời thường + ví dụ đồ án).
2. Các cảnh: thời gian, lời thoại, hoạt hình, số trên màn hình.
3. Điểm cần người duyệt xem lại (nếu có).
