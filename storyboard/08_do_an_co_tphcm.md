# Video 8 — Tổng hợp: đồ án CO TP.HCM chạy như thế nào từ đầu đến cuối

- **Thời lượng mục tiêu:** khoảng 6 phút
- **Nguồn:** [TĐ] chương 7 (Dữ liệu của đồ án, Điểm gãy, Hai luồng, coIsForecast), chương 8 (Sơ đồ toàn cảnh, Pipeline 9 bước, modelInputs, Dự đoán trên luồng, Hành trình một bản ghi, Khởi động, Ngân sách RAM), chương 9 (Ôn phản biện); [HO] mục chạy thử cả hệ thống 14/10/2026; [INFRA]

## Bảng phủ khái niệm

| Khái niệm | Ví dụ đời thường | Ví dụ trong đồ án |
|---|---|---|
| Sơ đồ toàn cảnh | Tòa báo: tòa soạn in, đội tin nóng, trang chủ | Đường lô và đường luồng trên một máy WSL, gặp nhau ở `model_best` |
| Pipeline 9 bước | Nhà máy nước: hút, lọc, bơm, mỗi khâu ghi sổ | 9 bước, mỗi bước ghi trạng thái vào `step_summary.json` |
| Điểm gãy | Cân nặng 4 năm, Tết năm nào cũng tăng 2 kg | CO giảm 77,4% quanh 01/07/2025, nguyên nhân chưa xác định |
| Hai luồng | Xem lại trận bóng đã quay / xem trực tiếp | Phát lại (mô phỏng, xanh dương) và OpenWeather (thật, xanh lá) |
| modelInputs | Đến khám lần đầu nhưng mang theo sổ khám cũ | Producer tự dựng 15 đặc trưng, gọi `air_pollution/history` |
| Hành trình một bản ghi | Một lá thư qua bưu cục, phân loại, giao tận nhà | CO 719,17 → dự đoán 709,9, qua 7 chặng |
| Khởi động theo thứ tự | Mở quán: mở cửa, bật bếp, rồi mới nhận khách | `start_all.sh`: HDFS → Kafka → Airflow → producer, Spark → backend, khoảng 1 phút 20 giây |
| Ngân sách RAM | Một căn phòng nhỏ, kê thêm đồ thì phải dọn bớt | 7,7 GB; bật YARN chỉ còn khoảng 0,9 GB, nên tắt YARN |

---

## Cảnh 1 — Toàn cảnh (0:00–0:55)

**Lời thoại:**
"Bảy video trước, ta xem từng công cụ. Giờ ghép lại. Đồ án phân tích và dự đoán khí CO ở TP.HCM, dữ liệu từ tháng 4/2022 tới tháng 3/2026. Toàn bộ chạy trên một máy WSL2 Ubuntu, có HDFS 4 DataNode, Kafka, Spark, Airflow, một backend và một dashboard. Cụm Docker 5 DataNode mà bạn có thể đã thấy là môi trường thử nghiệm của thành viên 1 và 3; cụm chạy chính là cụm 4 DataNode trên WSL2.
Hệ thống có hai đường chạy song song, đúng kiến trúc Lambda như video 1. Giống một tòa báo: tòa soạn in báo mỗi sáng, đội tin nóng đưa tin từng phút, trang chủ ghép cả hai.
Đường lô biến file CSV thành mô hình đã huấn luyện. Đường luồng dùng mô hình đó dự đoán từng bản ghi mới. Backend gom cả hai lên dashboard."

**Hoạt hình:**
- Một khung laptop lớn nhãn "WSL2 Ubuntu · ≈ 7,7 GB RAM". Bên trong, hai làn ngang vẽ dần từ trái sang phải.
- Làn trên "Đường lô (Airflow chạy 9 bước)": `CSV Kaggle 35 064 giờ` → `HDFS /co/raw · 4 DataNode` → `Spark: sạch, SQL, MLlib` → hộp tím `/co/output/model_best · random-forest-20261014`.
- Làn dưới "Đường luồng (thường trực)": `OpenWeather` (xanh lá) và `Phát lại Kaggle · MÔ PHỎNG` (xanh dương) → `Kafka :9092 hcmc_co_stream` → `Spark Structured Streaming` → hai nhánh `HDFS /live/co_stream` và `Kafka hcmc_co_enriched`.
- Mũi tên tím nét đứt từ `model_best` xuống `Spark Structured Streaming`, nhãn "nạp lúc khởi động".
- Bên phải: `Backend FastAPI :8000` → `Dashboard React :5173 · 11 tab`, mũi tên nét đứt "đọc JMX, WebHDFS, SQL" từ backend vào HDFS.
- Ngoài khung laptop, ở góc dưới, một khung nhỏ mờ nhãn "Cụm Docker 5 DataNode · thử nghiệm của thành viên 1 và 3"; không có mũi tên nào nối vào sơ đồ chính.
- Khung đời thường thoáng qua ở góc trên: tờ báo, điện thoại "TIN NÓNG", trang chủ.

**Số trên màn hình:** `04/2022 → 03/2026` · `4 DataNode` · `:9092 · :8000 · :5173` · `random-forest-20261014`.

---

## Cảnh 2 — Đường lô: 9 bước (0:55–1:55)

**Lời thoại:**
"Đường lô giữ khung 9 bước của giảng viên, đổi nội dung cho bài toán CO. Như nhà máy nước: hút nước sông, lọc, khử trùng, bơm vào bể, mỗi khâu ghi sổ của riêng mình. Ở đây mỗi bước là một script tự ghi trạng thái và số liệu thật vào step_summary.json, và trang Pipeline của dashboard chỉ đọc file này.
Bước 1 và 2 chuẩn bị thư mục và đưa CSV lên HDFS, kiểm md5 khớp file gốc. Bước 3 đọc bằng Spark: 35 064 giờ liền mạch, không trùng, không thiếu, và xác nhận giờ là UTC. Bước 4 làm sạch: đổi sang giờ Việt Nam, ghi Parquet; 35 064 dòng vào, 35 064 dòng ra, không bịa thêm giá trị nào. Bước 5 phân tích Spark SQL. Bước 6 phân cụm ngày và huấn luyện mô hình. Bước 7 lưu mô hình, nạp lại từ HDFS và dự đoán lại 200 giờ để chắc kết quả trùng. Bước 8 kiểm luồng sống. Bước 9 là DAG Airflow chạy bước 1 đến 7."

**Hoạt hình:**
- Khung đời thường mở đầu: ống nước sông chảy qua bốn bể nối tiếp, mỗi bể có một cuốn sổ nhỏ tự ghi.
- Chuyển sang đồ án: 9 ô bước xếp thành hai hàng (5 + 4), mỗi ô sáng lên khi lời thoại nhắc tới, kèm một con số:
  1–2: `/co/raw/Dataset.csv` + biểu tượng md5 ✓ · 3: `35 064 giờ · 0 trùng · 0 thiếu · UTC` · 4: `35 064 → 35 064 · Parquet 23 cột` · 5: `CO −77,4%` · 6: `k = 2 · RF được chọn` · 7: `nạp lại · 200 giờ trùng` · 8: `luồng sống` · 9: `Airflow DAG`.
- Mỗi ô khi sáng thả một dòng chữ nhỏ vào một tệp chung `step_summary.json` ở cuối hàng; tệp này có mũi tên tới ảnh trang Pipeline, bộ đếm `9/9`.

**Số trên màn hình:** `9 bước` · `35 064 giờ` · `35 064 → 35 064` · `200 giờ` · `step_summary.json` · `9/9`.

---

## Cảnh 3 — Điểm gãy và cách đồ án xử lý (1:55–2:55)

**Lời thoại:**
"Phát hiện quan trọng nhất của đường lô: giữa năm 2025, CO trong dữ liệu giảm khoảng bốn lần và mất hẳn chu kỳ mùa. Trung bình từ 1 343,8 xuống 303,3 microgam trên mét khối, giảm 77,4%.
Đồ án đã kiểm bốn điều. Không phải do mùa: so cùng tháng lịch vẫn giảm 79%. Không phải một ngày: CO giảm dần qua nhiều tuần. Không phải lỗi của Kaggle: bản raw tự tải từ OpenWeather có cùng hình dạng. Và luồng thật hôm nay nằm đúng mức mới. Còn nguyên nhân thì chưa xác định. Đồ án không kết luận CO ngoài đời giảm thật.
Tìm ranh giới cũng cần cẩn thận. Như cân nặng ghi mỗi ngày trong 4 năm, Tết năm nào cũng tăng 2 kg: muốn biết lúc nào bạn thật sự giảm cân, phải trừ phần 'Tết nào cũng tăng' trước. Đồ án trừ mùa vụ trước rồi mới tìm, ra một đoạn chuyển tiếp từ 13/05 đến 11/07/2025. Mô hình chỉ huấn luyện trên giai đoạn sau. Đã thử trộn cả bốn năm: không mô hình nào thắng nổi mốc 'CO giờ trước'."

**Hoạt hình:**
- Biểu đồ CO trung bình theo tháng 04/2022 → 03/2026. Đoạn trước vẽ dần, lên xuống theo mùa; tới vạch đỏ 01/07/2025 thì dừng. Đoạn sau bắt đầu lại ở mức thấp, gần phẳng. **Không có nét nối** qua vạch. Hai nhãn mức: `1 343,8` và `303,3`, mũi tên `−77,4%`.
- Bốn thẻ kiểm tra lật lên lần lượt, mỗi thẻ có dấu ✓: "Không phải mùa vụ · −79%", "Không phải một ngày", "Có cả ở bản raw OpenWeather", "Luồng thật khớp mức mới".
- Thẻ thứ năm lật lên với dấu "?" vàng lớn: "Nguyên nhân: CHƯA XÁC ĐỊNH".
- Khung đời thường: đồ thị cân nặng có các gò nhỏ mỗi dịp Tết; một tấm lọc "trừ mùa vụ" quét qua làm các gò phẳng đi, lộ ra chỗ đường đi xuống thật.
- Trên biểu đồ đồ án, một dải sọc vàng phủ từ 13/05 tới 11/07/2025 nhãn "chuyển tiếp, loại khỏi huấn luyện"; vùng sau dải được tô xanh nhạt "dữ liệu huấn luyện của mô hình".
- Ba cột nhỏ phương án A, B, C so với vạch mốc: chỉ A thấp hơn vạch.

**Số trên màn hình:** `1 343,8 → 303,3 µg/m³ · −77,4%` · `cùng tháng lịch −79%` · `chuyển tiếp 13/05 → 11/07/2025` · `Nguyên nhân: chưa xác định` · `A: RF 38,91 < mốc 43,86 · B, C: thua mốc`.

---

## Cảnh 4 — Hai luồng: phát lại và thật (2:55–3:45)

**Lời thoại:**
"Đường luồng có hai nguồn cùng đi qua Kafka. Như xem lại một trận bóng đã quay, có thể tua nhanh, so với xem trực tiếp, chỉ thấy khi trận đang đá.
Luồng phát lại đẩy lại từng giờ của bộ lịch sử như thể đang diễn ra, bắt đầu từ đầu tập kiểm tra của mô hình, 07/02/2026 lúc 20 giờ, để mọi dự đoán trên đó là ngoài mẫu. Nó là mô phỏng, tăng tốc được 10 lần, 60 lần, và nhãn trạm như Quận 1, Bình Thạnh chỉ là nhãn gán xoay vòng, không phải trạm đo thật.
Luồng thật lấy số của OpenWeather, theo nhịp của nguồn, tối đa khoảng một giá trị mỗi giờ. Có lúc nguồn trả trước giá trị dự báo của giờ sau; bản ghi khi đó mang cờ coIsForecast bằng true.
Hai nguồn không bao giờ vẽ chung một đường. Tab Dữ liệu sống để hai cột cạnh nhau."

**Hoạt hình:**
- Khung đời thường: trái một tivi có thanh tua và nút "×10, ×60"; phải một tivi có chấm đỏ "TRỰC TIẾP".
- Chuyển sang đồ án: hai làn chấm chạy vào cuốn sổ Kafka. Làn xanh dương dày, có biển "MÔ PHỎNG" và nhãn trạm xoay vòng `Quận 1 → Bình Thạnh → …` với chú thích "nhãn gán, không phải trạm đo"; một núm tốc độ `1× · 10× · 60×`. Mốc xuất phát ghi `07/02/2026 20:00 · đầu tập kiểm tra`.
- Làn xanh lá thưa, nhãn `TP.HCM (OpenWeather)`. Một chấm xanh lá có viền sáng đặc biệt mang nhãn `coIsForecast: true · giá trị dự báo của nguồn`.
- Cuối cảnh: ảnh tab Dữ liệu sống chia hai cột, cột trái xanh dương "Phát lại", cột phải xanh lá "OpenWeather thật". Một đường cố nối hai cột thành một biểu đồ bị gạch đỏ.

**Số trên màn hình:** `phát lại từ 07/02/2026 20:00` · `1× · 10× · 60×` · `OpenWeather: ≈ 1 giá trị/giờ` · `coIsForecast: true / false / null`.

---

## Cảnh 5 — Hành trình của một bản ghi (3:45–5:00)

**Lời thoại:**
"Giờ ta theo đúng con số ở đầu loạt video: một bản ghi OpenWeather thật ngày 14/10/2026, CO 719,17. Như theo một lá thư từ bưu cục tới tận nhà.
Chặng 1: API OpenWeather trả về CO và 7 chất khí khác.
Chặng 2: producer thêm nhiệt độ, gió, và gói modelInputs gồm 15 đặc trưng. Luồng sống chỉ có tối đa một quan sát mỗi giờ, tự nó phải chờ 24 giờ mới có CO của 24 giờ trước. Nên producer gọi thêm API lịch sử của OpenWeather để lấy các giờ trước. Như lần đầu đi khám ở phòng khám mới nhưng mang theo sổ khám cũ.
Chặng 3: Kafka cất tin vào partition 0 ở một offset mới.
Chặng 4: Spark giải JSON, đối chiếu ngưỡng: 719 nhỏ hơn 4 400 nên an toàn. Random Forest dự đoán 709,9.
Chặng 5: ghi một dòng Parquet vào /live/co_stream trên HDFS, và ghi JSON sang topic hcmc_co_enriched.
Chặng 6: backend đọc topic làm giàu, đổi sang dạng hợp đồng, giờ có đuôi cộng 7, đẩy qua WebSocket.
Chặng 7: dashboard hiện số ở cột 'OpenWeather thật', kèm dự đoán, và ghi chú 'giá trị dự báo của nguồn' vì cờ coIsForecast là true."

**Hoạt hình:**
- Một chấm xanh lá lớn mang nhãn `719,17` đi dọc một con đường có 7 cột mốc đánh số. Mỗi khi tới một cột mốc, chấm dừng lại, "thay áo" (hình dạng dữ liệu đổi), một khung JSON rút gọn bật ra bên cạnh rồi thu lại.
  1. Đám mây OpenWeather: khung `{"components":{"co":719.17, …}}`.
  2. Producer: chấm được gắn thêm ba lá cờ nhỏ (thời tiết, `modelInputs`, `coIsForecast`). Một nhánh phụ bay về đám mây nhãn `air_pollution/history`, mang về 24 viên bi nhỏ xếp thành hàng "t-1 … t-24". Khung đời thường: một cuốn sổ khám cũ được đặt lên bàn bác sĩ.
  3. Cuốn sổ Kafka: chấm nằm vào một dòng mới ở lề `partition 0 · offset mới`, dấu ACK bay ngược về producer.
  4. Bếp Spark: chấm đi qua thang ngưỡng (vạch 4 400 ở rất xa phía trên), đèn xanh "AN TOÀN (SAFE)"; rừng Random Forest thả xuống số tím `709,9`.
  5. Đường tách đôi: một bản xuống kệ HDFS `/live/co_stream` thành một dòng Parquet có các ô `co_actual · co_predicted · model_version · kafka_offset · provider`; một bản vào cuốn sổ `hcmc_co_enriched`.
  6. Backend: chấm đi qua một "máy dịch" đổi tên trường `co_predicted → coPredicted`, giờ có đuôi `+07:00`; rồi chạy vào ống WebSocket hai chiều.
  7. Màn hình dashboard: cột "OpenWeather thật" hiện `CO 719,17`, `Dự đoán 709,9`, `random-forest-20261014`, ghi chú vàng "giá trị dự báo của nguồn".
- Một thanh tiến trình 7 chặng chạy ở mép trên suốt cảnh.

**Số trên màn hình:** `14/10/2026` · `CO 719,17 µg/m³` · `15 đặc trưng` · `partition 0` · `719 < 4 400 → SAFE` · `dự đoán 709,9` · `random-forest-20261014` · `coIsForecast: true`.

---

## Cảnh 6 — Bật cả hệ thống, và chuyện RAM (5:00–5:35)

**Lời thoại:**
"Cả hệ thống bật bằng một lệnh, bash infra/start_all.sh, đúng thứ tự phụ thuộc. Như mở quán: mở cửa, bật bếp, rồi mới nhận khách. HDFS trước, chờ đủ 4 DataNode và ra khỏi chế độ an toàn; rồi Kafka; Airflow; hai producer và job Spark Streaming; cuối cùng backend. Khởi động nguội mất khoảng 1 phút 20 giây.
Mọi dịch vụ chia nhau khoảng 7,7 GB RAM của máy ảo, như kê đồ trong một căn phòng nhỏ. Bật cả YARN thì chỉ còn khoảng 0,9 GB trống; tắt YARN còn khoảng 2,2 GB. Spark chạy local nên không cần YARN, và đồ án quyết định tắt nó. Trên dashboard, YARN hiện nhãn 'Tắt chủ động', không tính là lỗi."

**Hoạt hình:**
- Khung đời thường: một quán nhỏ, ba công tắc bật lần lượt "Cửa → Bếp → Mở bán".
- Đồ án: một danh sách dọc các dịch vụ, mỗi dòng có đèn; đèn bật theo thứ tự HDFS (kèm `4/4 DataNode`, `safemode wait`) → Kafka → Airflow → Producer + Spark Streaming → Backend. Đồng hồ góc đếm tới `≈ 1:20`.
- Một thanh RAM 7,7 GB chia khối màu theo dịch vụ. Khi khối "YARN" được thêm vào, phần trống còn lại co về "≈ 0,9 GB", chuyển đỏ. Gỡ khối YARN, phần trống nở ra "≈ 2,2 GB", chuyển xanh.
- Thẻ YARN trên tab Tổng quan có viền đứt và nhãn "Tắt chủ động".

**Số trên màn hình:** `bash infra/start_all.sh` · `≈ 1 phút 20 giây` · `RAM ≈ 7,7 GB` · `có YARN: ≈ 0,9 GB trống` · `không YARN: ≈ 2,2 GB trống`.

---

## Cảnh 7 — Nói thật về giới hạn (5:35–5:55)

**Lời thoại:**
"Cuối cùng, những điều phải nói thật. Dữ liệu nhỏ, khoảng 3 megabyte. Cụm là giả lập phân tán trên một máy, replication bằng 1. Mô hình chỉ hơn mốc 'CO giờ trước' khoảng 11% về RMSE, đo trên một cửa sổ 52 ngày. Trên dữ liệu thật, mới có 3 điểm được dự đoán, quá ít để kết luận. Và nguyên nhân điểm gãy 01/07/2025 vẫn chưa xác định."

**Hoạt hình:**
- Năm thẻ trắng lật lên lần lượt, mỗi thẻ một dòng, viền vàng: "≈ 3 MB" · "1 máy, replication 1" · "−11,3% RMSE · 52 ngày" · "dữ liệu thật: 3 điểm" · "điểm gãy: chưa xác định".
- Không dùng màu đỏ (đây là giới hạn, không phải lỗi).

**Số trên màn hình:** `3 179 004 byte` · `dfs.replication = 1` · `RMSE 38,91 vs 43,86 · 1 259 giờ ≈ 52 ngày` · `3 điểm thật: RMSE 6,0 vs mốc 28,8 (quá ít)`.

---

## Cảnh 8 — Kết (5:55–6:00)

**Lời thoại:**
"Từ một file CSV và một API thời tiết, qua HDFS, Kafka, Spark và Airflow, tới một con số trên màn hình. Cảm ơn bạn đã theo dõi loạt video."

**Hoạt hình:**
- Camera lùi ra khỏi sơ đồ toàn cảnh; chấm `719,17` nhỏ dần thành một điểm sáng trên bản đồ TP.HCM rồi tắt.

**Số trên màn hình:** `CO 719,17 µg/m³ · dự đoán 709,9`.

---

## Điểm cần duyệt

- Cảnh 1 có câu về cụm Docker 5 DataNode theo ghi chú của người dùng; trong repo, `AGENTS.md` và `infra/README.md` chỉ ghi "Không có Docker" trên máy chạy chính. Nên thêm một dòng vào tài liệu dự án nếu muốn video và tài liệu khớp nhau.
- Cảnh 4: [INFRA] mục "Hai luồng" còn một câu cũ nói phát lại "từ 01/07/2025"; [TĐ] và [INFRA] mục A5 nói mặc định bắt đầu từ đầu tập kiểm tra (`--start auto`). Kịch bản theo bản mới (07/02/2026 20:00). Câu cũ trong `infra/README.md` nên sửa.
- Cảnh 5: các trường không có số trong nguồn (`dt`, nhiệt độ, gió, offset cụ thể của bản ghi 719,17) để trống hoặc "…", đúng như [TĐ].
- Cảnh 7: "3 điểm" theo [HO] mục 14/10/2026 21:10 (RMSE 6,0 so với 28,8). Mục A5 cũ hơn ghi "2 dòng"; dùng số mới hơn.
