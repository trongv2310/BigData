# Video 6 — Spark MLlib: K-Means, silhouette, Random Forest, RMSE/MAE/R², chia theo thời gian

- **Thời lượng mục tiêu:** khoảng 6 phút
- **Nguồn:** [TĐ] chương 5 (toàn bộ), chương 4 (Hàm cửa sổ và lag, Pivot); [HO] mục A3, A3b, A4

## Bảng phủ khái niệm

| Khái niệm | Ví dụ đời thường | Ví dụ trong đồ án |
|---|---|---|
| Có giám sát / không giám sát | Học với đáp án / tự xếp đồ thành nhóm khi không có nhãn | 6b dự đoán CO (hồi quy) / 6a phân cụm ngày |
| Chuẩn hóa hình dạng | Hai người khác chiều cao nhưng cùng dáng | Ngày A trung bình 1 600, ngày B 300, cùng hệ số 2,0 lúc 22h |
| K-Means | Đặt điểm tập trung, mỗi người về điểm gần nhất, dời điểm vào giữa nhóm | 1 460 ngày × 24 chiều, k = 2 |
| Silhouette | Mỗi người tự hỏi: mình gần nhóm mình hay nhóm bên cạnh hơn? | k = 2 đạt 0,4452, k = 3 đạt 0,4426: cấu trúc yếu |
| Seed / độ ổn định | Bốc thăm vị trí xuất phát khác nhau ra kết quả khác | Seed 21 kẹt nghiệm kém (ARI 0,0569); k = 3 ổn định |
| Đặc trưng độ trễ | Đoán nhiệt độ mai từ nhiệt độ mấy hôm trước | 15 đặc trưng, `coLag1h` … `coLag24h` |
| Chia theo thời gian | Ôn đề cũ, thi thử, rồi thi thật | Huấn luyện 5 036 giờ, kiểm tra 1 259 giờ, không xáo |
| Rò rỉ dữ liệu | Biết trước đề thi thật | Đưa pm2_5 của chính giờ t vào đoán CO giờ t |
| Mốc so sánh | Dự báo "mai giống hôm nay" | Mốc CO giờ trước RMSE 43,86 |
| Random Forest | Hỏi 100 người rồi lấy trung bình | 100 cây, sâu 8, RMSE 38,91 |
| RMSE / MAE / R² | Một lần sai rất xa bị RMSE phạt nặng hơn MAE | 4 giờ ngày 03/11/2025: MAE 10,53, RMSE 13,60, R² −3,79 |
| Chọn theo kiểm định | Chọn cách ôn theo đề thi thử, không theo đề thi thật | Chọn Random Forest (23,5) dù GBT thấp hơn trên tập kiểm tra |

---

## Cảnh 1 — Hai bài toán của bước 6 (0:00–0:35)

**Lời thoại:**
"Học máy có hai kiểu chính. Có giám sát: học từ ví dụ đã có đáp án để đoán đáp án mới, như ôn bài có lời giải. Không giám sát: không có đáp án, chỉ tìm các nhóm giống nhau, như dọn tủ quần áo mà không ai dặn trước phải chia mấy ngăn.
Đồ án làm cả hai bằng Spark MLlib. Bước 6a, không giám sát: phân các ngày thành cụm theo hình dạng CO 24 giờ. Bước 6b, có giám sát: dự đoán CO của giờ kế tiếp."

**Hoạt hình:**
- Trái: chồng bài tập có đáp án đỏ ở lề; một mũi tên "học" chỉ sang bài mới có dấu "?".
- Phải: đống quần áo lộn xộn tự bay vào ba ngăn tủ theo hình dáng, không có nhãn trên ngăn.
- Hai nửa đổi nhãn: trái thành "6b · Dự đoán CO (hồi quy)", phải thành "6a · Phân cụm ngày (K-Means)".

**Số trên màn hình:** `pyspark.ml`.

---

## Cảnh 2 — Biến một ngày thành một điểm (0:35–1:20)

**Lời thoại:**
"K-Means cần mỗi đối tượng là một điểm. Đồ án xoay bảng 35 064 giờ thành bảng ngày, mỗi ngày một dòng có 24 cột cho 24 giờ. Bỏ 2 ngày đầu và cuối thiếu giờ do lệch múi giờ, còn 1 460 ngày.
Nhưng nếu so CO thô, cụm chỉ tách được 'trước và sau điểm gãy' vì hai giai đoạn chênh nhau khoảng bốn lần, không nói lên điều gì mới. Nên mỗi giờ được chia cho CO trung bình của chính ngày đó, để so dáng lên xuống chứ không so mức.
Như so hai người khác chiều cao nhưng cùng dáng người. Ngày A trung bình 1 600, lúc 22 giờ là 3 200; ngày B trung bình 300, lúc 22 giờ là 600. Cả hai đều được hệ số 2,0 lúc 22 giờ: cùng dáng 'đêm cao gấp đôi trung bình', nên rơi vào cùng cụm."

**Hoạt hình:**
- Một dải dài 35 064 ô giờ cuộn lại thành lưới: mỗi hàng một ngày, 24 ô. Hai hàng đầu và cuối thiếu ô, chuyển xám và rơi ra. Bộ đếm hàng dừng ở `1 460`.
- Mỗi hàng ngày biến thành một đường cong 24 điểm.
- Hai đường cong A (cao) và B (thấp) đặt chồng: khác hẳn về độ cao. Một "cái kẹp chuẩn hóa" ép mỗi đường chia cho trung bình của nó: hai đường co về cùng một thang và trùng khít; chấm ở 22h của cả hai sáng lên với nhãn `2,0`.
- Khung đời thường: hai bóng người cao thấp khác nhau, sau khi co về cùng chiều cao thì hai bóng trùng khít.

**Số trên màn hình:** `35 064 giờ → 1 462 ngày → 1 460 ngày × 24 cột` · `A: 3 200 ÷ 1 600 = 2,0` · `B: 600 ÷ 300 = 2,0`.

---

## Cảnh 3 — K-Means chạy thế nào (1:20–2:10)

**Lời thoại:**
"K-Means chia dữ liệu thành k nhóm bằng cách lặp hai bước. Gán mỗi điểm về tâm gần nhất. Rồi dời mỗi tâm về trung bình các điểm trong nhóm. Lặp tới khi tâm gần như đứng yên.
Hãy tưởng tượng buổi cắm trại: ban tổ chức cắm vài lá cờ tập trung, mỗi người đi về lá cờ gần mình nhất; rồi mỗi lá cờ được dời vào giữa đám người của nó; mọi người lại xem lá cờ nào gần nhất. Vài vòng là ổn định.
Đồ án chạy K-Means trên 1 460 điểm 24 chiều, thử k từ 2 đến 8, seed 42, chọn k có silhouette lớn nhất. Kết quả k bằng 2. Cụm 0 có 387 ngày, đỉnh lúc 22 giờ gấp 4,19 lần đáy lúc 13 giờ, CO ngày trung bình 1 642. Cụm 1 có 1 073 ngày, phẳng hơn, gấp 1,94 lần, CO trung bình 971. Chỉ 4,1% ngày của cụm 0 là sau 01/07/2025, so với 24% ở cụm 1. Tỷ lệ cuối tuần gần bằng nhau, 26 và 29%, nên cụm không phải 'ngày thường và cuối tuần'."

**Hoạt hình:**
- Mặt phẳng 2 chiều có các chấm (nhãn "Minh họa 2 chiều; đồ án dùng 24 chiều"). Ba lá cờ cắm ngẫu nhiên.
- Bước gán: mỗi chấm nối một sợi chỉ mảnh tới lá cờ gần nhất, rồi đổi sang màu của lá cờ đó.
- Bước dời tâm: mỗi lá cờ trượt vào giữa đám chấm cùng màu; vài chấm ở ranh giới đổi màu.
- Lặp 3 vòng, lá cờ dịch chuyển ít dần, cuối cùng đứng yên; chữ "hội tụ".
- Đồ án: hai đường cong tâm cụm 24 giờ vẽ trên cùng trục 0–23 giờ. Đường cụm 0 nhọn, có mũi tên từ đáy 13h lên đỉnh 22h với nhãn "× 4,19". Đường cụm 1 thoải hơn, "× 1,94".
- Hai thanh ngang bên dưới: cụm 0 `387 ngày` và cụm 1 `1 073 ngày`; mỗi thanh có phần tô "sau 01/07/2025" (4,1% và 24%) và phần sọc "cuối tuần" (26% và 29%).

**Số trên màn hình:** `k = 2 … 8 · seed 42` · `Cụm 0: 387 ngày · đỉnh 22h × 4,19 đáy 13h · TB 1 642` · `Cụm 1: 1 073 ngày · × 1,94 · TB 971` · `sau mốc: 4,1% / 24%` · `cuối tuần: 26% / 29%`.

---

## Cảnh 4 — Silhouette và độ ổn định (2:10–3:00)

**Lời thoại:**
"Làm sao biết chia mấy cụm là hợp? Silhouette đo mỗi điểm nằm gọn trong cụm của nó đến đâu so với cụm gần nhất bên cạnh, từ âm 1 đến 1. Như mỗi người trong buổi cắm trại tự hỏi: mình gần nhóm mình hơn hay gần nhóm bên cạnh hơn?
Quy ước đọc thường dùng: trên 0,5 là cấu trúc cụm rõ, 0,25 đến 0,5 là yếu.
Đồ án: k bằng 2 đạt 0,4452, k bằng 3 đạt 0,4426, gần như hòa. Tiêu chí ghi rõ 'lớn nhất, hòa thì lấy k nhỏ', nên chọn k bằng 2, và kết luận trung thực là cấu trúc cụm yếu.
K-Means còn phụ thuộc điểm xuất phát do seed quyết định. Chạy lại với 6 seed: k bằng 2 có 5 seed gần như giống hệt, nhưng seed 21 kẹt ở nghiệm kém, silhouette chỉ 0,3626 và độ giống so với seed 42 chỉ 0,0569. Còn k bằng 3 rất ổn định, độ giống giữa mọi cặp seed từ 0,9874 trở lên."

**Hoạt hình:**
- Một chấm phóng to, hai thước đo: thước ngắn "a" tới các chấm cùng màu, thước dài "b" tới cụm bên cạnh. Công thức `s = (b − a) ÷ max(a, b)` hiện ra. Chấm sáng xanh khi b ≫ a; chấm ở ranh giới có a ≈ b thì vàng.
- Một thước đo ngang −1 … 1 với vùng "yếu 0,25–0,5" tô vàng nhạt và "rõ > 0,5" tô xanh.
- Biểu đồ cột silhouette theo k = 2…8; hai cột k = 2 (0,4452) và k = 3 (0,4426) cao gần bằng nhau, cùng nằm trong vùng vàng; các cột k = 4…8 thấp hơn.
- Sáu ô nhỏ, mỗi ô là kết quả của một seed (42, 1, 7, 13, 21, 99) với k = 2: năm ô gần giống nhau, ô seed 21 đổi màu đỏ, hai cụm 893/567 ngày thay vì khoảng 1 070/390.
- Hàng thứ hai, k = 3: sáu ô gần như trùng khít, dấu ✓ xanh.
- Khi nói "độ giống", hiện chú thích nhỏ: "ARI, chỉ số Rand điều chỉnh: 1 = giống hệt, quanh 0 = như ngẫu nhiên".

**Số trên màn hình:** `k = 2: 0,4452` · `k = 3: 0,4426` · `seed 21: silhouette 0,3626 · ARI 0,0569 · 893/567 ngày` · `k = 3: ARI ≥ 0,9874`.

---

## Cảnh 5 — Dự đoán CO: đặc trưng, độ trễ và rò rỉ (3:00–3:55)

**Lời thoại:**
"Sang bài toán dự đoán. Nhãn là CO của giờ t. Đặc trưng là các con số đưa vào để đoán. Đồ án dùng 15 đặc trưng: giờ và tháng của t; nhiệt độ, gió và 7 chất khí khác của giờ t trừ 1; và CO của 1, 2, 3 và 24 giờ trước. Những cột 'giờ trước' được tạo bằng hàm lag của Spark: giá trị CO của một giờ trở thành coLag1h của giờ sau.
Vì sao mọi thứ phải lấy từ trước t? Vì lúc cần đoán CO giờ t thì chưa có số đo nào của giờ t. Nếu đưa bụi mịn pm2_5 của chính giờ t vào, mà hai chất này tương quan 0,79 đến 0,90, điểm số sẽ rất đẹp trên giấy nhưng vô dụng ngoài đời. Đó là rò rỉ dữ liệu. Như biết trước đề thi thật.
Bộ test của đồ án cố tình gài một đặc trưng rò rỉ giờ t để chắc chắn test bắt được."

**Hoạt hình:**
- Trục thời gian ngang với các ô giờ ... t-24 ... t-3, t-2, t-1, t. Ô t có dấu "?" tím.
- Bốn mũi tên từ ô t-1, t-2, t-3, t-24 bay tới ô t, mỗi mũi tên mang nhãn `coLag1h`, `coLag2h`, `coLag3h`, `coLag24h`.
- Bảng nhỏ 4 dòng ngày 03/11/2025 (12:00 đến 15:00): giá trị `co` của dòng 12:00 (176,41) trượt chéo xuống thành `coLag1h` của dòng 13:00, rồi `coLag2h` của dòng 14:00, rồi `coLag3h` của dòng 15:00. Mũi tên chéo sáng theo.
- Bảng 15 đặc trưng hiện thành bốn nhóm màu: Thời gian, Thời tiết, Chất khí khác, CO quá khứ.
- Một mũi tên lén từ ô `pm2_5 (t)` bay vào ô t, bị một tấm khiên chặn lại, khiên sáng đỏ "RÒ RỈ"; nhãn "tương quan 0,79–0,90". Khung đời thường: một tờ đề thi thật có dấu "MẬT" bị giật khỏi tay học sinh.

**Số trên màn hình:** `15 đặc trưng` · `03/11/2025: 12:00 co = 176,41 → 13:00 coLag1h = 176,41` · `pm2_5 · CO: 0,79–0,90`.

---

## Cảnh 6 — Chia theo thời gian và mốc so sánh (3:55–4:40)

**Lời thoại:**
"Dữ liệu chia ba phần. Tập huấn luyện để học, tập kiểm định để chọn mô hình, tập kiểm tra để chấm điểm cuối cùng một lần. Như ôn bằng đề các năm trước, làm đề thi thử để chọn cách ôn, rồi mới thi thật. Với chuỗi thời gian, ba phần phải nối tiếp: cũ học, mới thi, không xáo trộn.
Phương án chính của đồ án chỉ dùng giai đoạn sau chuyển tiếp. Huấn luyện từ 13/07/2025 đến tối 07/02/2026, 5 036 giờ; 20% cuối của đoạn này làm kiểm định. Kiểm tra từ 07/02/2026 đến 01/04/2026, 1 259 giờ, khoảng 52 ngày.
Và phải có mốc so sánh. Dự báo thời tiết 'mai giống hôm nay' đúng khá thường xuyên; đài khí tượng chỉ đáng tin khi đúng hơn câu đó. Mốc của đồ án là 'CO giờ t bằng CO giờ t trừ 1', đạt RMSE 43,86 và R² 0,902. Vì CO thay đổi chậm, mốc này rất khó thắng."

**Hoạt hình:**
- Thanh thời gian dài 04/2022 → 03/2026, vạch đỏ 01/07/2025 và vùng sọc chuyển tiếp 13/05–11/07/2025 nhãn "loại khỏi huấn luyện". Đoạn trước vùng sọc mờ đi.
- Đoạn sau chuyển tiếp tách thành ba khối màu nối tiếp: xanh "Huấn luyện 5 036 giờ", trong đó 20% cuối có viền vàng "Kiểm định"; đỏ cam "Kiểm tra 1 259 giờ ≈ 52 ngày".
- Một bàn tay thử xáo trộn các ô giờ: bị chặn, chữ "không xáo".
- Khung đời thường: ba tờ đề "Đề cũ", "Thi thử", "Thi thật" xếp theo lịch.
- Một đường thẳng nét đứt đen nằm ngang trên biểu đồ cột (chuẩn bị cho cảnh sau), nhãn "Mốc CO giờ trước · RMSE 43,86 · R² 0,902".

**Số trên màn hình:** `Huấn luyện 13/07/2025 00:00 → 07/02/2026 19:00 · 5 036 giờ` · `Kiểm tra 07/02/2026 20:00 → 01/04/2026 06:00 · 1 259 giờ` · `Mốc: RMSE 43,86 · R² 0,902`.

---

## Cảnh 7 — Random Forest và các chỉ số RMSE, MAE, R² (4:40–5:35)

**Lời thoại:**
"Random Forest trồng nhiều cây quyết định. Mỗi cây hỏi một chuỗi câu có hoặc không, ví dụ 'CO giờ trước có dưới 260 không?', và mỗi cây học trên một mẫu ngẫu nhiên. Dự đoán là trung bình của cả rừng. Như hỏi ý kiến một người dễ bị lệch, hỏi 100 người rồi lấy trung bình thì thường sát hơn.
Đo sai số bằng ba chỉ số. MAE là sai số trung bình. RMSE bình phương sai số trước khi lấy trung bình, nên phạt nặng các lần sai lớn. R² cho biết mô hình giải thích được bao nhiêu phần dao động so với đoán bằng trung bình. Lấy 4 giờ thật ngày 03/11/2025 với mốc 'CO giờ trước': MAE 10,53, RMSE 13,60, lớn hơn vì có một lần sai 25,07 lúc 12 giờ. R² ra âm 3,79 dù sai số nhỏ, vì 4 giá trị quá sát nhau. Bài học: không đánh giá R² trên vài điểm.
Trên 1 259 giờ kiểm tra, Random Forest với 100 cây, sâu 8: RMSE 38,91, MAE 15,79, R² 0,923. Thắng mốc, nhưng chỉ khoảng 11% về RMSE, còn MAE gần như không đổi."

**Hoạt hình:**
- Một cây quyết định mọc từ gốc: nút đầu `coLag1h < 260?`, hai nhánh có/không, các lá chứa số. Cây nhân bản thành một khu rừng 100 cây (đếm nhanh), mỗi cây hơi khác hình. Một chấm dữ liệu đi qua mọi cây, mỗi cây thả xuống một số; các số dồn vào một cái phễu "trung bình" ra số tím.
- Khung đời thường: 100 hình người giơ bảng số, một người cầm loa cộng lại.
- Bảng 4 giờ 03/11/2025: cột "thật" và cột "mốc CO giờ trước", cột "sai số" hiện ra `25,07 · 4,96 · 3,40 · 8,68`. Thanh sai số 25,07 dài hẳn và chuyển đỏ. Hai cái cân: cân "MAE" nặng 10,53; cân "RMSE" nghiêng hơn, 13,60 (vì ô đỏ bị bình phương phóng to). Ô `R² = −3,79` hiện kèm biểu tượng cảnh báo "chỉ 4 điểm".
- Biểu đồ cột RMSE trên 1 259 giờ: ba cột tuyến tính 41,30, Random Forest 38,91, GBT 37,07; vạch đen nét đứt ở 43,86. Cột Random Forest có viền vàng "được chọn". Ghi chú dưới: "MAE: 14,7–15,8 so với mốc 16,2".

**Số trên màn hình:** `numTrees = 100 · maxDepth = 8 · seed = 42` · `03/11/2025: MAE 10,53 · RMSE 13,60 · R² −3,79` · `RF: RMSE 38,91 · MAE 15,79 · R² 0,923` · `Tuyến tính 41,30 · GBT 37,07 · Mốc 43,86` · `−11,3% RMSE`.

---

## Cảnh 8 — Vì sao chọn Random Forest chứ không phải GBT (5:35–6:00)

**Lời thoại:**
"GBT có RMSE kiểm tra thấp hơn, 37,07. Vậy sao không chọn GBT? Vì mô hình được chọn theo đoạn kiểm định, trước khi nhìn tập kiểm tra: Random Forest 23,5, GBT 23,8. Đổi sang GBT vì nó đẹp hơn trên tập kiểm tra chính là chọn cách ôn theo đề thi thật, làm con số báo cáo lạc quan giả.
Và một hạn chế phải nói thật: tập kiểm tra chỉ phủ tháng 2 và 3 năm 2026, nên chưa biết mô hình chạy ra sao ở mùa khác."

**Hoạt hình:**
- Bảng hai cột "RMSE kiểm định" và "RMSE kiểm tra". Che cột kiểm tra bằng một tấm rèm; ở cột kiểm định, Random Forest 23,5 sáng xanh "thấp nhất", GBT 23,8, tuyến tính 27,1.
- Dấu ✓ đặt cạnh Random Forest. Rèm mở: GBT 37,07 thấp hơn RF 38,91 ở cột kiểm tra. Một bàn tay định dời dấu ✓ sang GBT thì bị chặn, chữ "chọn theo đề thi = gian lận".
- Cuối: lịch chỉ khoanh tháng 2 và 3/2026, các tháng khác mờ với dấu "?".

**Số trên màn hình:** `Kiểm định: RF 23,5 · GBT 23,8 · Tuyến tính 27,1` · `Kiểm tra: RF 38,91 · GBT 37,07 · Tuyến tính 41,30` · `tập kiểm tra: 02–03/2026`.

---

## Điểm cần duyệt

- Video dài sát 6 phút. Nếu cần cắt, đề xuất bỏ phần độ ổn định theo seed ở cảnh 4 (giữ silhouette), hoặc chuyển sang một video phụ.
- Ba phương án chia dữ liệu A, B, C (B và C thua mốc) chưa đưa vào video này để giữ thời lượng; đưa vào video 8 dưới dạng một câu. Nếu muốn có cảnh riêng thì video 6 sẽ vượt 6 phút.
- Cảnh 7: "khoảng 11%" là −11,3% của [HO] và [TĐ].
