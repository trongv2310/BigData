# Video 5 — Spark SQL và Structured Streaming: micro-batch, trigger, checkpoint

- **Thời lượng mục tiêu:** khoảng 6 phút
- **Nguồn:** [TĐ] chương 4 (Spark SQL, Structured Streaming, Micro-batch, Trigger, Checkpoint), chương 1 (Schema-on-read), chương 2 (Nhiều file nhỏ), chương 3 (Ngữ nghĩa giao nhận), chương 6 (Chặn SQL nguy hiểm); `mock/analysis-sql.json` (kết quả thật bước 5 chép từ HDFS) cho số CO theo giờ

## Bảng phủ khái niệm

| Khái niệm | Ví dụ đời thường | Ví dụ trong đồ án |
|---|---|---|
| Spark SQL | Viết phiếu yêu cầu theo mẫu chuẩn, thủ kho tự chọn đường lấy hàng | Bước 5: CO theo giờ, trước và sau 01/07/2025 |
| Chặn SQL nguy hiểm | Quầy tra cứu chỉ cho xem, không cho mang sách đi | `sql_guard.py`: chỉ một câu SELECT, bảng `aqi` và `live` |
| Structured Streaming | Sổ điểm danh dài thêm mỗi ngày, công thức tính vẫn như cũ | `infra/spark_kafka_consumer.py` (`HCMC_CO_Kafka_Streaming`) |
| Micro-batch | Xe buýt đưa đón chạy theo chuyến, chở ai đang chờ | `spark-batch-26` lúc 06:01 ngày 13/10/2026; 1 259 giờ qua luồng ở 60× |
| Trigger | Giờ xuất bến của xe buýt | 30 giây cho Parquet `/live`, 5 giây cho topic làm giàu |
| Nhiều file nhỏ | Mỗi chuyến xe phát một tờ biên lai riêng | Tới khoảng 2 880 file mỗi ngày (suy ra từ trigger) |
| Checkpoint | Kẹp dấu trang và ghi "đang đọc trang mấy" trước khi đọc | `/live/_checkpoints/co_stream`; khởi động nguội 43 file, 43 commit, lag 0 |
| Đúng một lần / ít nhất một lần | Gửi thư bảo đảm có sổ ký nhận / gửi lại khi không chắc | Parquet `/live` đúng một lần; topic làm giàu ít nhất một lần |

---

## Cảnh 1 — Hai cách nói chuyện với Spark (0:00–0:30)

**Lời thoại:**
"Ở video trước ta ra lệnh cho Spark bằng các hàm như lọc, nhóm, đếm. Video này có hai món mới. Một: hỏi Spark bằng SQL, ngôn ngữ truy vấn mà ai học cơ sở dữ liệu cũng biết. Hai: cho Spark chạy mãi trên dữ liệu không có điểm kết thúc, đó là Structured Streaming."

**Hoạt hình:**
- Logo Spark ở giữa. Bên trái một tờ giấy có chữ `SELECT`; bên phải một băng chuyền chạy vô tận ra ngoài khung hình.
- Hai thứ cùng trượt vào logo Spark, logo tách làm đôi: "Spark SQL" và "Structured Streaming".

**Số trên màn hình:** không có số.

---

## Cảnh 2 — Spark SQL (0:30–1:35)

**Lời thoại:**
"Spark SQL cho chạy câu SQL chuẩn trên DataFrame: đăng ký DataFrame thành một bảng tạm rồi viết SELECT như với cơ sở dữ liệu. Câu SQL và code DataFrame đều đi qua bộ tối ưu Catalyst, biên dịch ra cùng một kế hoạch.
Giống viết phiếu yêu cầu theo mẫu chuẩn gửi kho: bạn chỉ ghi cần gì, thủ kho tự chọn đường đi lấy hàng nhanh nhất.
Bước 5 của đồ án tính toàn bộ bằng Spark SQL. Một kết quả: CO trung bình theo giờ trong ngày, giờ Việt Nam. Trước 01/07/2025, CO cao về đêm, khoảng 1 938 lúc 23 giờ, và thấp quanh trưa, khoảng 707 lúc 13 giờ. Từ 01/07/2025 đường gần như phẳng, chỉ 209 đến 361. Nguyên nhân của điểm gãy này chưa xác định; đồ án chỉ báo số liệu trước và sau riêng."

**Hoạt hình:**
- Một DataFrame (bảng có cột) nhận nhãn `createOrReplaceTempView("aqi_clean")`, tấm nhãn dán lên như biển tên.
- Câu SQL gõ từng dòng:
  `SELECT hour, AVG(co) FROM aqi_clean WHERE after_2025_07_01 = 1 GROUP BY hour ORDER BY hour`
- Câu SQL và một đoạn mã DataFrame tương đương cùng trượt vào một cái phễu nhãn "Catalyst"; phễu có bốn vạch: phân tích → tối ưu logic → kế hoạch vật lý → sinh mã. Cả hai ra cùng một sơ đồ kế hoạch.
- Ví dụ đời thường ở khung nhỏ: tờ phiếu "Cần: 3 thùng A" đưa vào kho; thủ kho vạch đường ngắn nhất trên bản đồ kho.
- Biểu đồ đường: trục hoành 0–23 giờ (giờ VN). Đường "Trước 01/07/2025" (28 481 giờ) vẽ từ trái sang phải, có chấm đánh dấu đỉnh 23h và đáy 13h. Rồi đường "Từ 01/07/2025" (6 583 giờ) vẽ bên dưới, gần như phẳng. Hai đường khác màu, có chú giải; ghi chú góc: "Điểm gãy, nguyên nhân chưa xác định".

**Số trên màn hình:** `Trước 01/07/2025: 28 481 giờ · 23h ≈ 1 938 · 13h ≈ 707` · `Từ 01/07/2025: 6 583 giờ · 209 → 361` · đơn vị `µg/m³`.

---

## Cảnh 3 — SQL trên web phải được canh cửa (1:35–2:05)

**Lời thoại:**
"Trang SparkSQL của dashboard cho bạn gõ SQL thật. Vì thế backend phải chặn mọi câu có thể ghi, xóa hay chạy mã tùy ý. Như quầy tra cứu của thư viện: cho xem, không cho mang sách đi, không cho sửa thẻ mục lục.
Đồ án chặn hai lớp. Lớp một, sql_guard.py chỉ nhận đúng một câu SELECT, chặn từ khóa như insert, drop, create, giới hạn 5 000 ký tự. Lớp hai, phiên Spark chỉ có hai bảng: aqi là bộ Kaggle, live là Parquet do Spark Streaming ghi."

**Hoạt hình:**
- Ô nhập SQL trên dashboard. Gõ `SELECT …`: đi qua cổng bảo vệ, cổng sáng xanh.
- Gõ `DROP TABLE aqi`: chữ `DROP` đỏ lên, cổng đóng sập, thông báo bị chặn.
- Phía sau cổng chỉ có hai cánh cửa nhãn `aqi` và `live`; các cánh cửa khác bị xây kín.

**Số trên màn hình:** `1 câu SELECT` · `≤ 5 000 ký tự` · `bảng: aqi · live`.

---

## Cảnh 4 — Structured Streaming: bảng dài thêm mãi (2:05–3:00)

**Lời thoại:**
"Structured Streaming coi luồng dữ liệu như một bảng không ngừng dài thêm. Ta viết truy vấn y như với DataFrame thường, Spark tự chạy truy vấn đó lặp lại trên phần dữ liệu mới đến.
Như sổ điểm danh lớp: mỗi buổi thêm dòng mới, còn công thức 'đếm số buổi vắng' thì không đổi.
Job streaming của đồ án làm năm việc. Đọc topic hcmc_co_stream từ Kafka. Giải JSON theo một lược đồ 22 trường khai báo sẵn, tin thiếu CO hoặc tốc độ gió thì bỏ, không điền số giả. Tính chỉ số tích tụ CO và mức cảnh báo: từ 4 400 là WARNING, từ 12 400 là DANGER, theo thang của OpenWeather. Nạp mô hình model_best để dự đoán CO. Rồi ghi ra hai đích: Parquet trên HDFS và topic hcmc_co_enriched."

**Hoạt hình:**
- Một bảng có tiêu đề cột; dòng mới mọc dần từ đáy bảng, không có dòng cuối. Một khung truy vấn cố định nằm phía trên, mỗi khi có dòng mới thì khung đó chớp sáng một lần.
- Khung nhỏ đời thường: sổ điểm danh, mỗi buổi một dòng mới; ô "Tổng vắng" tự cập nhật.
- Chuyển sang đồ án: dây chuyền 5 trạm nối nhau, mỗi trạm là một hộp có biểu tượng:
  1. Đọc Kafka (cuốn sổ `hcmc_co_stream`).
  2. `from_json` (khuôn đúc 22 ô): một tin thiếu ô CO rơi khỏi băng, chuyển xám "bỏ, không điền".
  3. Tính `co ÷ (gió + 1)` và đèn cảnh báo: thang màu xanh / vàng / đỏ với vạch 4 400 và 12 400; chấm CO hiện tại nằm sâu trong vùng xanh.
  4. Nạp mô hình (hộp tím `model_best`), chấm dữ liệu nhận thêm một nhãn tím "dự đoán".
  5. Đường tách đôi: một nhánh vào kệ HDFS `/live/co_stream`, một nhánh vào cuốn sổ `hcmc_co_enriched`.

**Số trên màn hình:** `22 trường` · `co_accumulation_index = co ÷ (wind_speed_kmh + 1)` · `WARNING ≥ 4 400 · DANGER ≥ 12 400 µg/m³ (thang OpenWeather)`.

---

## Cảnh 5 — Micro-batch và trigger: xe buýt theo giờ (3:00–4:00)

**Lời thoại:**
"Bên trong, Structured Streaming chạy theo micro-batch. Đến lượt, nó lấy mọi tin mới kể từ offset đã xử lý, gom thành một lô nhỏ và chạy như một job lô bình thường. Mỗi lô có một số thứ tự tăng dần.
Trigger quy định khi nào chạy lô kế tiếp. Giống xe buýt đưa đón cứ 30 phút một chuyến, chở tất cả ai đang chờ ở trạm. Chờ đông thì chuyến đông, không ai thì Spark bỏ qua chuyến đó.
Đồ án đặt hai trigger cho cùng một luồng: 30 giây cho Parquet trên HDFS, để đỡ sinh file nhỏ; 5 giây cho topic làm giàu, để dashboard thấy dự đoán sớm. Ở tab Sơ đồ luồng, mỗi lô thành công thắp 4 bước, ví dụ lô spark-batch-26 lúc 06:01 ngày 13/10/2026. Khi phát lại ở tốc độ 60 lần, 1 259 giờ dữ liệu đi qua trong khoảng 4 phút và cả 1 259 đều được dự đoán.
Cái giá của trigger: mỗi lô có dữ liệu sinh ra một file Parquet. Phát lại liên tục thì có thể tới khoảng 2 880 file nhỏ mỗi ngày. Con số này suy ra từ trigger, chưa đếm đủ một ngày."

**Hoạt hình:**
- Một trạm xe buýt; người (chấm xanh dương nhãn "MÔ PHỎNG" và chấm xanh lá) tới trạm theo nhịp không đều. Đồng hồ trên trạm đếm ngược "30 s". Hết giờ, xe buýt nhãn `batch N` tới, đón tất cả, chạy đi; số trên xe tăng N → N+1. Một lần trạm vắng: xe không chạy, chữ "bỏ qua batch rỗng".
- Tách làm hai làn: làn trên xe buýt lớn chạy mỗi "30 s" về kệ HDFS `/live`; làn dưới xe nhỏ chạy mỗi "5 s" về cuốn sổ `hcmc_co_enriched` → dashboard.
- Ảnh tab "Sơ đồ luồng · Spark Structured Streaming": 4 ô bước sáng lần lượt, viền đứt nhãn "suy ra từ job thành công", tiêu đề `spark-batch-26 · 06:01 · 13/10/2026`.
- Đồng hồ tốc độ vặn tới "60×", bộ đếm chạy 0 → 1 259 trong một đồng hồ cát "≈ 4 phút"; dưới cùng bộ đếm thứ hai "đã dự đoán: 1 259 / 1 259".
- Mỗi xe buýt làn trên khi tới kệ HDFS thả xuống một tờ giấy nhỏ (file Parquet). Các tờ chồng lên thành đống cao dần; nhãn "≈ 2 880 file/ngày (suy ra)". Ba cách giảm hiện thành ba thẻ: tắt phát lại `WITH_REPLAY=0`, tăng trigger, gộp file bằng `coalesce`.

**Số trên màn hình:** `trigger 30 s → /live/co_stream` · `trigger 5 s → hcmc_co_enriched` · `spark-batch-26 · 06:01 13/10/2026` · `60× · 1 259 giờ · ≈ 4 phút · 1 259/1 259 dự đoán` · `≈ 2 880 file/ngày (suy ra)`.

---

## Cảnh 6 — Checkpoint: ghi chỗ đang đọc trước khi đọc (4:00–5:05)

**Lời thoại:**
"Nếu Spark chết giữa chừng thì sao? Trước khi chạy lô N, Spark ghi vào checkpoint: lô N sẽ xử lý offset từ a đến b. Chạy xong, ghi: lô N đã xong. Khởi động lại, nó đọc checkpoint và chạy lại đúng lô N với đúng khoảng offset đó.
Như đọc sách: trước khi đọc chương mới, bạn ghi lên giấy kẹp 'đang đọc trang 40 đến 60'; đọc xong ghi 'xong trang 60'. Ngủ quên giữa chừng thì sáng ra biết đọc lại từ đâu.
Đồ án có hai checkpoint trên HDFS, một cho Parquet và một cho topic làm giàu. Bên trong có thư mục offsets, ghi trước khi chạy, và commits, ghi khi xong.
Đồ án đã thử khởi động nguội: tắt hẳn máy ảo, bật lại bằng start_all.sh mất khoảng 1 phút 20 giây. HDFS khỏe, Kafka còn nguyên tin, Spark đọc tiếp từ checkpoint: 43 file Parquet tương ứng 43 commit, lag bằng 0."

**Hoạt hình:**
- Một cuốn sổ tay nhãn `checkpoint` mở hai trang: trái `offsets/`, phải `commits/`.
- Lô `N` sắp chạy: bút ghi vào trang trái `N: offset a → b`. Lô chạy (vòng quay vàng). Bút ghi vào trang phải `N ✓`.
- Lô `N+1`: ghi trang trái, lô bắt đầu chạy, rồi tia sét đỏ đánh xuống, màn hình nhiễu, Spark tắt. Trang phải không có `N+1 ✓`.
- Spark bật lại, mở sổ tay: thấy trang trái có `N+1` mà trang phải chưa có, nên chạy lại lô `N+1` đúng khoảng offset đã ghi. Xong, ghi `N+1 ✓`.
- Khung đời thường: giấy kẹp sách ghi "đang đọc 40 → 60", nhân vật ngủ gục, sáng dậy mở đúng trang 40.
- Đồ án: hai cuốn sổ tay đặt trên kệ HDFS, nhãn `/live/_checkpoints/co_stream` và `/live/_checkpoints/co_enriched`.
- Khởi động nguội: nút nguồn laptop "wsl --shutdown" tắt mọi biểu tượng; đồng hồ đếm lên tới "≈ 1 phút 20 giây" trong khi các biểu tượng sáng lại theo thứ tự HDFS → Kafka → Spark → backend. Kết quả ba ô: `fsck HEALTHY` · `Kafka còn nguyên tin` · `43 file = 43 commit · lag 0`.

**Số trên màn hình:** `offsets/ · commits/` · `/live/_checkpoints/co_stream` · `/live/_checkpoints/co_enriched` · `≈ 1 phút 20 giây` · `43 file Parquet · 43 commit · lag 0`.

---

## Cảnh 7 — Đúng một lần hay ít nhất một lần (5:05–5:45)

**Lời thoại:**
"Khi có sự cố, mỗi tin được xử lý bao nhiêu lần? Có ba mức: tối đa một lần, có thể mất; ít nhất một lần, có thể trùng; đúng một lần, không mất không trùng.
Như gửi thư: gửi thường rồi thôi thì có thể thất lạc; gửi lại khi không chắc thì người nhận có thể nhận hai lá; gửi bảo đảm có sổ ký nhận thì mỗi lá đến đúng một lần.
Job của đồ án ghi hai đích. Parquet trên HDFS dùng file sink của Spark, có nhật ký _spark_metadata, đạt đúng một lần. Topic hcmc_co_enriched dùng Kafka sink, chỉ đảm bảo ít nhất một lần, nên sau sự cố có thể thấy một bản ghi làm giàu lặp lại."

**Hoạt hình:**
- Ba hàng ngang, mỗi hàng một lá thư và một hộp thư.
  - "Tối đa một lần": lá thư rơi giữa đường, chuyển đỏ.
  - "Ít nhất một lần": hai lá thư giống hệt nhau vào hộp, lá thứ hai cam.
  - "Đúng một lần": một lá thư vào hộp kèm sổ ký nhận, có dấu ✓.
- Sơ đồ đồ án: từ hộp Spark, hai mũi tên. Mũi tên tới HDFS `/live/co_stream` có tấm sổ nhỏ `_spark_metadata`, nhãn xanh "đúng một lần". Mũi tên tới cuốn sổ `hcmc_co_enriched` thỉnh thoảng thả hai chấm giống nhau, nhãn cam "ít nhất một lần".

**Số trên màn hình:** `at-most-once · at-least-once · exactly-once` · `_spark_metadata`.

---

## Cảnh 8 — Tổng kết (5:45–6:00)

**Lời thoại:**
"Spark SQL cho hỏi bằng câu chuẩn; Structured Streaming cho chạy câu đó mãi trên dữ liệu mới. Micro-batch là chuyến xe, trigger là giờ xuất bến, checkpoint là sổ ghi chỗ đang đọc. Video sau: mô hình dự đoán mà job streaming đang nạp được huấn luyện ra sao."

**Hoạt hình:**
- Xe buýt dừng trước hộp tím `model_best`; camera lướt vào trong hộp, chuyển sang video 6.

**Số trên màn hình:** không có số mới.

---

## Điểm cần duyệt

- Cảnh 2: số CO theo giờ (1 938 lúc 23h, 707 lúc 13h, 209 đến 361) lấy từ [TĐ] và khớp `mock/analysis-sql.json`. Mục A3 trong [HO] ghi số khác (1 868 lúc 0h, 748 lúc 12h). Tôi chưa rõ vì sao hai nơi lệch nhau; kịch bản dùng bộ số khớp với file kết quả thật hiện có. Nên xác nhận lại bằng `GET /api/analysis/sql` trên backend thật trước khi dựng.
- Cảnh 6 dùng `N`, `N+1` thay vì số lô thật, tránh nhầm với offset 44, 45 (tin trùng) của video 3.
