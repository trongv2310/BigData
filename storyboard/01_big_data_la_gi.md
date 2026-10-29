# Video 1 — Big Data là gì: 5V, batch và stream, kiến trúc Lambda

- **Thời lượng mục tiêu:** khoảng 5 phút 30 giây
- **Người xem:** sinh viên chưa biết gì về Big Data
- **Nguồn:** [TĐ] chương 1, chương 8 (Sơ đồ toàn cảnh), chương 7 (Bộ dữ liệu)

## Bảng phủ khái niệm

| Khái niệm | Ví dụ đời thường | Ví dụ trong đồ án |
|---|---|---|
| Big Data | Quán cà phê ghi sổ tay so với chuỗi 2 000 cửa hàng gửi giao dịch từng giây | Bộ Kaggle 35 064 giờ đo CO, nói thật là dữ liệu nhỏ |
| Mở rộng dọc / ngang | Chấm 10 000 bài thi: một giáo viên siêu nhanh hay 50 giáo viên | 4 DataNode trên một máy WSL2, "giả lập phân tán" |
| Volume | Kho hàng không vừa một nhà kho | 3 179 004 byte, 19 cột |
| Velocity | Máy POS gửi từng giây | Phát lại 1 bản ghi mỗi 10 giây, OpenWeather hỏi mỗi 300 giây |
| Variety | Hóa đơn giấy, tin nhắn, ảnh trộn lẫn | CSV, JSON từ API, Parquet, JSON qua WebSocket |
| Veracity | Sổ ghi giờ theo múi khác | `datetime` là UTC, o3 điền sẵn 14,9%, điểm gãy giữa 2025 |
| Value | Biết món nào sắp hết ngay lúc này | Dự đoán CO giờ kế tiếp, cảnh báo khi vượt ngưỡng |
| Batch / stream | Tính lãi cuối tháng / chặn giao dịch thẻ trong 200 mili giây | 9 bước pipeline khoảng 8 phút / mỗi bản ghi qua Kafka và Spark |
| Kiến trúc Lambda | Báo in buổi sáng, tin nóng trên điện thoại, trang chủ tòa báo | Batch layer HDFS /co, speed layer Kafka + Spark Streaming, serving layer FastAPI + dashboard |

---

## Cảnh 1 — Mở đầu: một con số CO (0:00–0:25)

**Lời thoại:**
"Đây là một con số: 719,17 microgam CO trên mỗi mét khối không khí ở TP.HCM, ghi ngày 14 tháng 10 năm 2026. Con số này đi từ một máy chủ thời tiết, qua Kafka, Spark và HDFS, rồi hiện lên màn hình dashboard. Loạt video này sẽ giải thích từng chặng của hành trình đó. Trước hết: Big Data là gì?"

**Hoạt hình:**
- Nền tối. Một chấm xanh lá (dữ liệu thật) xuất hiện ở góc trái trên, nhãn "OpenWeather".
- Chấm bay theo đường cong qua ba biểu tượng xám lần lượt sáng lên: Kafka (cuốn sổ), Spark (mũ bếp trưởng), HDFS (kệ sách), rồi tới dashboard (màn hình). Mỗi biểu tượng chuyển xám → màu khi chấm đi qua.
- Tới màn hình, chấm nở thành con số lớn.
- Cả cảnh thu nhỏ lại thành một biểu tượng ở góc, nhường chỗ cho tiêu đề.

**Số trên màn hình:** `CO 719,17 µg/m³` · `14/10/2026` · chữ nhỏ "dữ liệu thật, OpenWeather".

---

## Cảnh 2 — Big Data là gì (0:25–1:05)

**Lời thoại:**
"Big Data là dữ liệu mà muốn lưu và xử lý được thì phải chia cho nhiều máy cùng làm, vì một máy không còn đủ dung lượng, đủ tốc độ hoặc đủ linh hoạt.
Hãy so một quán cà phê ghi hóa đơn vào sổ tay mỗi ngày, với một chuỗi 2 000 cửa hàng mà mỗi máy tính tiền gửi giao dịch về trung tâm từng giây, và trung tâm cần biết ngay món nào sắp hết. Quán cà phê là dữ liệu bình thường. Chuỗi 2 000 cửa hàng là Big Data: vừa lớn, vừa đến liên tục, vừa đến từ nhiều loại máy."

**Hoạt hình:**
- Bên trái: một quán nhỏ, một cuốn sổ, tờ hóa đơn rơi vào sổ chậm rãi, mỗi 2 giây một tờ.
- Bên phải: bản đồ có các chấm cửa hàng bật lên dần đến khi kín màn hình, bộ đếm chạy tới "2 000". Từ mỗi chấm, các tia sáng nhỏ bắn về một tòa nhà trung tâm ở giữa, dày đặc.
- Tòa nhà trung tâm rung nhẹ, chuyển vàng rồi đỏ: một máy không kham nổi.
- Chữ "Big Data" hiện ra phía trên bên phải.

**Số trên màn hình:** `2 000 cửa hàng` · `mỗi giây`.

---

## Cảnh 3 — Mở rộng dọc và mở rộng ngang (1:05–1:45)

**Lời thoại:**
"Khi một máy không kham nổi, có hai cách. Mở rộng dọc là mua máy mạnh hơn. Mở rộng ngang là thêm nhiều máy thường và chia việc.
Tưởng tượng phải chấm 10 000 bài thi. Thuê một giáo viên siêu nhanh thì rồi cũng chạm giới hạn và rất đắt. Chia cho 50 giáo viên, mỗi người 200 bài, thì cần thêm một tổ trưởng phát bài, thu điểm, và chia lại phần của ai bị ốm. Tổ trưởng là máy chủ điều phối; chia lại bài là chịu lỗi. Big Data chọn mở rộng ngang.
Trong đồ án, cụm chạy chính có 4 DataNode, nhưng cả 4 là 4 tiến trình trên cùng một máy WSL2 Ubuntu, khác cổng. Đúng giao thức, đúng cách chia dữ liệu, nhưng không có lợi ích phần cứng. Ta gọi đó là cụm giả lập phân tán."

**Hoạt hình:**
- Một chồng giấy cao ghi "10 000 bài". Bên trái: một người, chồng giấy giảm chậm, đồng hồ quay nhanh; thanh "giới hạn" màu đỏ hiện ngang đầu.
- Bên phải: chồng giấy tách thành 50 xấp nhỏ bay tới 50 người xếp lưới 10 × 5; một người đội mũ "tổ trưởng" đứng giữa. Một người chuyển đỏ (ốm), xấp bài của người đó bay sang hai người bên cạnh.
- Chuyển cảnh: lưới 50 người co lại thành 4 hình kệ sách nằm **trong cùng một khung laptop** nhãn "WSL2 Ubuntu, khoảng 7,7 GB RAM". Bốn kệ có nhãn cổng lần lượt hiện ra.
- Một dải ghi chú nhỏ trượt lên từ đáy: "Cụm Docker 5 DataNode: môi trường thử nghiệm của thành viên 1 và 3, không phải cụm chạy chính."

**Số trên màn hình:** `10 000 bài` · `50 giáo viên × 200 bài` · `4 DataNode` · `9864 · 9874 · 9884 · 9894` · `7,7 GB RAM`.

---

## Cảnh 4 — Năm chữ V (1:45–3:05)

**Lời thoại:**
"Người ta hay mô tả Big Data bằng năm chữ V. Ta xem từng chữ với dữ liệu thật của đồ án.
Volume, khối lượng: dữ liệu có vừa một máy không? Bộ Kaggle có 35 064 dòng, mỗi dòng một giờ, 19 cột, file khoảng 3 megabyte. Nói thật: đây là dữ liệu nhỏ, laptop đọc chưa đến một giây.
Velocity, tốc độ: dữ liệu đến liên tục. Luồng phát lại gửi một bản ghi mỗi 10 giây; luồng thật hỏi OpenWeather mỗi 300 giây.
Variety, đa dạng: cùng một hệ thống có CSV lịch sử, JSON từ API, Parquet trên HDFS và JSON qua WebSocket.
Veracity, độ tin cậy, là chữ V quan trọng nhất của đồ án. Cột giờ là giờ UTC chứ không phải giờ Việt Nam. Cột ozone đã bị điền sẵn ở 14,9% số dòng. Và giữa năm 2025 có một điểm gãy, CO tụt hẳn xuống.
Value, giá trị: từ tất cả những thứ đó, đồ án dự đoán CO của giờ kế tiếp và cảnh báo khi vượt ngưỡng."

**Hoạt hình:**
- Năm chữ V lớn xếp hàng ngang, mỗi chữ một màu nhạt. Mỗi lần đọc tới, chữ đó phóng to vào giữa, các chữ còn lại mờ đi.
- **Volume:** một file CSV vẽ như tờ giấy dài cuộn xuống, bộ đếm dòng chạy 0 → 35 064. Bên cạnh, một cái cân: file 3 MB đặt lên đĩa cân, kim chỉ gần như không nhúc nhích; nhãn "dữ liệu nhỏ".
- **Velocity:** một băng chuyền. Chấm xanh dương nhãn "MÔ PHỎNG" rơi lên băng mỗi nhịp (đồng hồ nhỏ ghi 10 s); chấm xanh lá rơi thưa hơn nhiều (đồng hồ ghi 300 s).
- **Variety:** bốn thẻ bài lật lên lần lượt: CSV (bảng có dòng kẻ), JSON (ngoặc nhọn), Parquet (các cột dọc), WebSocket (hai mũi tên ngược chiều).
- **Veracity:** một đồng hồ hiện `00:00 UTC`, mũi tên "+7 giờ", đồng hồ quay tới `07:00`. Tiếp theo: bảng 20 dòng, 3 ô cột o3 tô vàng nhấp nháy, nhãn "điền sẵn 14,9%". Cuối: đường CO theo tháng chạy từ trái sang phải, tới vạch đỏ 01/07/2025 thì **dừng hẳn**, đoạn sau bắt đầu lại ở mức thấp, không có nét nối; chú thích "nguyên nhân chưa xác định".
- **Value:** một đèn tín hiệu xanh lá với chữ "SAFE", bên cạnh mũi tên chỉ về "giờ kế tiếp" và con số dự đoán màu tím.

**Số trên màn hình:** `35 064 dòng` · `19 cột` · `3 179 004 byte (≈ 3 MB)` · `1 bản ghi / 10 s` · `hỏi mỗi 300 s` · `UTC + 7` · `o3 điền sẵn 14,9%` · `01/07/2025` · `1 343,8 → 303,3 µg/m³`.

---

## Cảnh 5 — Câu hỏi khó: 3 MB thì cần gì Hadoop? (3:05–3:30)

**Lời thoại:**
"Bạn sẽ hỏi: 3 megabyte thì cần gì Hadoop? Câu trả lời thẳng: dữ liệu hiện tại nhỏ. Đồ án dùng nó để dựng và chứng minh một kiến trúc chạy được khi dữ liệu lớn lên, khi có nhiều trạm, nhiều năm, đo từng phút. Mọi bước chạy trên công cụ phân tán thật, nên thay dữ liệu lớn hơn thì không phải viết lại."

**Hoạt hình:**
- Dấu hỏi lớn rơi xuống giữa màn hình.
- Tờ giấy 3 MB đặt vào một khung kiến trúc rỗng (bốn ô: HDFS, Kafka, Spark, Airflow); bốn ô sáng lên lần lượt.
- Mũi tên "dữ liệu lớn lên" kéo dài sang phải; tờ giấy nhân lên thành chồng cao, khung kiến trúc giữ nguyên hình dạng, chỉ thêm kệ sách ở hàng dưới.

**Số trên màn hình:** `3 MB` · nhãn "kiến trúc không đổi".

---

## Cảnh 6 — Xử lý theo lô và theo luồng (3:30–4:20)

**Lời thoại:**
"Có hai cách xử lý dữ liệu. Theo lô: gom dữ liệu đã có rồi tính một lần cho cả khối. Theo luồng: tính liên tục trên từng bản ghi ngay khi nó đến.
Ngân hàng tính lãi cuối tháng cho mọi tài khoản: đó là theo lô. Ngân hàng chặn một giao dịch thẻ bất thường trong 200 mili giây: đó là theo luồng.
Trong đồ án, phần lô là 9 bước pipeline: đọc file CSV trên HDFS, làm sạch, phân tích, huấn luyện mô hình. Chạy lại từ đầu bằng Airflow mất khoảng 8 phút. Phần luồng: mỗi bản ghi CO mới đi qua Kafka, Spark dùng mô hình vừa huấn luyện để dự đoán, rồi ghi xuống HDFS và đẩy lên dashboard."

**Hoạt hình:**
- Màn hình chia đôi, nhãn "THEO LÔ" và "THEO LUỒNG".
- Trái: một cái thùng lớn; tờ giấy rơi vào thùng suốt cảnh, đến khi đầy thì cả thùng trượt vào một cỗ máy, cỗ máy chạy và nhả ra một bảng kết quả. Đồng hồ cạnh cỗ máy ghi "phút đến giờ".
- Phải: băng chuyền; mỗi tờ giấy đi qua một cổng nhỏ, cổng chớp một lần và nhả ra một dấu tích ngay. Đồng hồ ghi "mili giây đến vài giây".
- Hai ví dụ ngân hàng hiện dưới mỗi bên: cuốn lịch lật tới "cuối tháng"; thẻ ngân hàng bị chặn với chữ "200 ms".
- Chuyển sang đồ án: bên trái thùng đổi nhãn thành `/co/raw/Dataset.csv`, cỗ máy thành dãy 9 ô bước sáng dần từ 1 đến 9, đồng hồ "≈ 8 phút". Bên phải băng chuyền đổi thành Kafka → Spark → HDFS `/live` + dashboard, chấm xanh dương và xanh lá đi qua.
- Một mũi tên tím nối từ cuối dãy 9 bước sang cổng Spark bên luồng, nhãn `/co/output/model_best`.

**Số trên màn hình:** `200 ms` · `9 bước` · `≈ 8 phút` · `/co/raw/Dataset.csv` · `/co/output/model_best` · `/live/co_stream`.

---

## Cảnh 7 — Kiến trúc Lambda (4:20–5:15)

**Lời thoại:**
"Ghép hai cách đó lại, ta có kiến trúc Lambda: hai đường chạy song song trên cùng dữ liệu, đường lô chậm mà chính xác, đường tốc độ nhanh mà xấp xỉ, rồi gộp ở tầng phục vụ.
Hãy nghĩ tới một tòa báo. Báo in mỗi sáng là tầng lô: đầy đủ, đã kiểm duyệt nhưng trễ một ngày. Tin nóng trên điện thoại là tầng tốc độ: nhanh, có khi chưa đủ chi tiết. Trang chủ của tòa báo ghép cả hai là tầng phục vụ.
Đồ án bám đúng mô hình này. Tầng lô: HDFS, Spark 9 bước, Airflow, ra mô hình đã huấn luyện. Tầng tốc độ: Kafka và Spark Streaming. Tầng phục vụ: backend FastAPI ở cổng 8000 và dashboard React ở cổng 5173. Điểm nối hai tầng là thư mục model_best. Hệ quả cần nhớ: huấn luyện lại mô hình thì phải khởi động lại job streaming, nó mới dùng phiên bản mới."

**Hoạt hình:**
- Ba dải ngang xếp chồng, chữ Lambda lớn mờ phía sau: trên "Batch layer", giữa "Speed layer", dưới cùng bên phải "Serving layer".
- Ví dụ đời thường: tờ báo gấp rơi vào dải trên (kèm đồng hồ "trễ 1 ngày"); điện thoại rung ở dải giữa với thông báo "TIN NÓNG"; cả hai trượt vào một khung trình duyệt ở dải dưới.
- Chuyển sang đồ án: tờ báo đổi thành khối "HDFS /co · Spark 9 bước · Airflow", điện thoại đổi thành "Kafka hcmc_co_stream · Spark Streaming", khung trình duyệt đổi thành "FastAPI :8000 · Dashboard :5173".
- Mũi tên tím nét đứt chạy từ khối lô xuống khối tốc độ, nhãn `model_best`, kèm chú thích "nạp lúc khởi động".
- Cuối: khối lô chớp "huấn luyện lại", khối tốc độ hiện biểu tượng ↻ "khởi động lại job".

**Số trên màn hình:** `:8000` · `:5173` · `hcmc_co_stream` · `/co/output/model_best`.

---

## Cảnh 8 — Tổng kết và lộ trình (5:15–5:30)

**Lời thoại:**
"Tóm lại: Big Data là chia việc cho nhiều máy; năm chữ V giúp ta hỏi đúng câu; lô và luồng là hai nhịp xử lý; Lambda ghép chúng lại. Video sau, ta mở kệ sách đầu tiên: HDFS."

**Hoạt hình:**
- Bốn thẻ tóm tắt bay vào thành hàng, rồi thu về sơ đồ Lambda nhỏ.
- Ô HDFS trong sơ đồ sáng lên và phóng to, chuyển sang video 2.

**Số trên màn hình:** không có số mới.

---

## Điểm cần duyệt

- Cảnh 3 có nhắc cụm Docker 5 DataNode theo ghi chú của người dùng. Trong repo chỉ ghi "Không có Docker" (AGENTS.md, infra/README.md), nên câu này dựa trên ghi chú người dùng, không có trong tài liệu dự án.
- Cảnh 1 chỉ ghi ngày 14/10/2026 cho bản ghi 719,17, không ghi giờ: nguồn không có giờ quan sát của bản ghi này.
