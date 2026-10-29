# Video 2 — HDFS: block, NameNode, DataNode, replication, heartbeat, luồng ghi và đọc

- **Thời lượng mục tiêu:** khoảng 6 phút
- **Nguồn:** [TĐ] chương 2 (HDFS, Block, NameNode, DataNode, Hệ số nhân bản, Nhịp tim, Báo cáo block, Luồng ghi, Luồng đọc, Safe mode), chương 8 (Bản đồ thư mục HDFS); [HO] mục dọn file HDFS và khởi động nguội

## Bảng phủ khái niệm

| Khái niệm | Ví dụ đời thường | Ví dụ trong đồ án |
|---|---|---|
| HDFS | Thư viện: quầy thủ thư + nhiều kệ sách | `hdfs://localhost:9000`, các vùng `/co`, `/live` |
| Block | Cuốn sách dày tách thành nhiều tập | `/raw/Dataset.csv` ghi với block 1 MiB thành 4 block |
| NameNode | Thủ thư giữ thẻ mục lục; sổ hộ khẩu + tờ ghi thay đổi (fsimage + edit log) | NameNode cổng 9000 (RPC) và 9870 (web) |
| DataNode | Kệ sách | 4 DataNode cổng 9864, 9874, 9884, 9894 trên một máy |
| Replication | Photo hồ sơ cất ở nhiều tủ | `dfs.replication` = 1, có lúc 24 block nhưng 38 bản sao |
| Heartbeat | Lớp học trực tuyến bấm "có mặt" | Tắt một DataNode, xem "Last contact" trên trang 9870 |
| Luồng ghi | Chuyền tay bộ hồ sơ, chữ ký truyền ngược | Tab "Sơ đồ luồng" mục "HDFS ghi file" |
| Luồng đọc | Hỏi thủ thư vị trí rồi tự đi lấy sách | Spark đọc `/co/processed/aqi_clean` |
| Safe mode | Thư viện mở cửa nhưng chưa cho mượn khi chưa kiểm kê xong | `start_all.sh` chạy `hdfs dfsadmin -safemode wait` |

---

## Cảnh 1 — HDFS là một thư viện (0:00–0:45)

**Lời thoại:**
"HDFS là hệ thống file trải trên nhiều máy nhưng trông như một cây thư mục duy nhất. Nó sinh ra cho file lớn, ghi một lần và đọc nhiều lần.
Hãy hình dung một thư viện. Có một quầy thủ thư giữ thẻ mục lục: cuốn X tập 1 ở kệ A, tập 2 ở kệ C. Và có nhiều kệ sách. Bạn hỏi thủ thư vị trí, rồi tự đi lấy sách; thủ thư không bê sách giúp bạn. Quầy thủ thư là NameNode. Kệ sách là DataNode. Bạn là client.
Trong đồ án, địa chỉ cụm là hdfs://localhost:9000, giao diện NameNode ở cổng 9870."

**Hoạt hình:**
- Một thư viện vẽ phẳng: quầy thủ thư ở giữa trên, bốn kệ sách xếp hàng phía dưới.
- Một nhân vật sinh viên (client) bước vào, mũi tên lời thoại "Cuốn X ở đâu?" bay tới quầy; thủ thư lật thẻ, mũi tên trả lời "Tập 1: kệ A, tập 2: kệ C" bay ngược lại.
- Sinh viên đi thẳng tới kệ A rồi kệ C, lấy sách. Một đường nét đứt từ quầy tới kệ có dấu ✗: thủ thư không chạm vào sách.
- Nhãn đổi dần: quầy → "NameNode", kệ → "DataNode", sinh viên → "Client (hdfs dfs, Spark)".
- Cuối cảnh, khung thư viện thu lại vào trong một laptop "WSL2 Ubuntu"; bốn kệ có nhãn cổng.

**Số trên màn hình:** `hdfs://localhost:9000` · `NameNode UI :9870`.

---

## Cảnh 2 — Block: cắt file thành viên gạch (0:45–1:50)

**Lời thoại:**
"HDFS không cất nguyên một file trên một máy. Nó cắt file thành những mẩu cố định gọi là block. Mặc định mỗi block tối đa 128 MiB; block cuối chỉ chiếm đúng phần còn lại.
Như một cuốn từ điển quá dày được tách thành nhiều tập để cất ở nhiều kệ.
Ví dụ: file 300 MiB, block 128 MiB, thành 3 block: 128, 128 và 44. Block cuối chỉ chiếm 44 MiB chứ không chiếm đủ 128.
Vì sao block to như vậy? Vì NameNode nhớ mọi block trong RAM, ít block thì đỡ tốn bộ nhớ.
File của đồ án chỉ khoảng 3 MB, với block 128 MiB thì chỉ có 1 block. Để thấy được việc chia, file /raw/Dataset.csv được ghi với block 1 MiB. Kết quả: 3 179 004 byte thành 4 block, ba block đầy 1 048 576 byte và một block cuối 33 276 byte. Lệnh hdfs fsck trả đúng 4 mã block, khớp với tab HDFS trên dashboard."

**Hoạt hình:**
- Một cuốn sách dày bị dao cắt thành 3 tập, mỗi tập in số trang khác nhau, tập cuối mỏng hơn.
- Chuyển sang một thanh dài nhãn "300 MiB". Một thước đo 128 MiB trượt từ trái sang phải, cắt hai lần: hai viên gạch đầy cùng cỡ và một viên gạch ngắn "44 MiB". Viên gạch ngắn có khung nét đứt cỡ 128 bao quanh rồi khung đó biến mất, nhấn ý "không chiếm đủ 128".
- Công thức hiện ra: `số block = ⌈kích thước file ÷ kích thước block⌉`.
- Chuyển sang đồ án: thanh dài nhãn `/raw/Dataset.csv · 3 179 004 byte`. Thước đo "128 MiB" thử đặt lên: thanh nằm gọn trong 1 viên gạch, nhãn "1 block". Thước đổi thành "1 MiB" (thước co lại), cắt ba lần: ba viên gạch đầy và một viên rất mỏng.
- Mỗi viên gạch được dán mã kiểu `blk_1073741827`. Một cửa sổ terminal nhỏ trượt lên gõ `hdfs fsck /raw/Dataset.csv -files -blocks -locations`, in ra 4 dòng, mỗi dòng sáng lên đúng lúc viên gạch tương ứng nhấp nháy.

**Số trên màn hình:** `128 MiB (mặc định)` · `300 MiB → 128 + 128 + 44` · `3 179 004 byte` · `block 1 MiB` · `3 × 1 048 576 + 33 276` · `4 block` · `blk_1073741827`.

---

## Cảnh 3 — NameNode: người giữ sổ (1:50–2:40)

**Lời thoại:**
"NameNode giữ siêu dữ liệu: cây thư mục, quyền, danh sách block của từng file và nơi đặt từng block. Nó không giữ dữ liệu thật. Cả cây thư mục nằm trong RAM để trả lời nhanh.
Để không mất khi tắt máy, NameNode ghi xuống đĩa hai thứ: fsimage, ảnh chụp toàn bộ cây; và edit log, nhật ký mọi thay đổi sau ảnh chụp. Giống cuốn sổ hộ khẩu chép sạch đầu năm, cộng một tờ giấy ghi 'ngày 3 nhập thêm A, ngày 5 chuyển B đi'. Muốn biết hộ khẩu hôm nay: đọc sổ, rồi đọc hết tờ ghi chép.
Riêng vị trí block thì không ghi xuống đĩa; NameNode dựng lại từ báo cáo của các DataNode.
Trong đồ án, NameNode nghe ở cổng 9000. Backend chỉ đọc số liệu cụm qua JMX và WebHDFS, không ghi."

**Hoạt hình:**
- Phóng to quầy thủ thư: một cái đầu máy chủ có hình bộ não "RAM", bên trong là cây thư mục nhỏ `/co`, `/live` mọc nhánh.
- Một nhánh `/raw/Dataset.csv` có 4 viên gạch nhỏ treo lủng lẳng, mỗi viên nối dây tới một kệ.
- Bên dưới, hai vật rơi xuống ổ đĩa: một cuốn sổ bìa cứng "fsimage" và một tờ giấy dài "edit log" với các dòng chữ xuất hiện từng dòng.
- Mô phỏng khởi động lại: màn hình tắt đen, sáng lại; cuốn sổ mở ra, tờ giấy được đọc từng dòng (dòng nào đọc xong thì gạch xám), cây thư mục mọc lại.
- Các sợi dây nối gạch với kệ lúc đầu chưa có; bốn kệ gửi lên các phong bì nhỏ "báo cáo block", mỗi phong bì tới nơi thì một sợi dây hiện ra.
- Góc phải: biểu tượng backend với mũi tên một chiều đọc từ NameNode, nhãn "chỉ đọc · JMX · WebHDFS".

**Số trên màn hình:** `RPC :9000` · `Web :9870` · `fsimage + edit log`.

---

## Cảnh 4 — DataNode và replication (2:40–3:40)

**Lời thoại:**
"DataNode là tiến trình cất block thành file trên đĩa, kèm file checksum để phát hiện dữ liệu hỏng.
Để một máy hỏng không làm mất dữ liệu, HDFS giữ nhiều bản sao cho mỗi block, mặc định 3 bản, mỗi bản trên một DataNode khác nhau. Giống photo một hồ sơ quan trọng cất ở ba tủ, cháy một tủ vẫn còn hai bản. Cái giá: file 1 GB với replication 3 chiếm 3 GB trên cụm, nhưng chịu được 2 DataNode hỏng cùng lúc.
Trong đồ án, replication mặc định là 1. Lý do: 4 DataNode cùng nằm trên một ổ đĩa, tăng lên 3 chỉ tốn gấp ba dung lượng mà không chống được hỏng ổ. Một số file từng được ghi với replication lớn hơn 1, nên có lúc cụm có 24 block nhưng 38 bản sao. Khi bảo vệ, nói rõ: trên cụm thật nhiều máy sẽ để 3.
Một chi tiết vui: mỗi DataNode báo dung lượng của cả ổ đĩa máy, nên tổng dung lượng cụm hiện khoảng 4 TB dù chỉ có một đĩa vật lý."

**Hoạt hình:**
- Phóng to một kệ: mỗi viên gạch là một file `blk_…` kèm một tờ nhãn nhỏ `blk_….meta` dính bên cạnh.
- Phần minh họa (góc nhãn "Minh họa: replication 3, cụm đồ án dùng 1"): một viên gạch xanh nhân bản thành 3 bản bay tới 3 kệ khác nhau. Một kệ chuyển đỏ và đổ xuống; hai bản còn lại vẫn sáng; NameNode phát lệnh, một bản sao mới bay sang kệ thứ tư cho đủ lại 3.
- Ví dụ đời thường đặt cạnh: ba tủ hồ sơ, một tủ bốc khói đỏ, hai tủ còn lại có dấu tích.
- Chuyển sang đồ án: bỏ nhãn minh họa. 4 kệ nằm chung trên **một** hình ổ đĩa lớn. Mỗi viên gạch chỉ một bản. Ổ đĩa nứt (minh họa giả định): cả 4 kệ cùng đỏ, nhấn ý "replication 3 trên một ổ cũng không cứu".
- Bộ đếm hiện `24 block · 38 bản sao`.
- Cuối cảnh: bốn kệ cùng giơ biển "dung lượng cả ổ", bốn biển gộp lại thành một biển "tổng ≈ 4 TB", rồi một mũi tên chỉ xuống "1 ổ đĩa vật lý".

**Số trên màn hình:** `replication 3 (mặc định)` · `1 GB × 3 = 3 GB` · `dfs.replication = 1` · `24 block / 38 bản sao` · `≈ 4 TB`.

---

## Cảnh 5 — Heartbeat và báo cáo block (3:40–4:25)

**Lời thoại:**
"Làm sao NameNode biết DataNode nào còn sống? Mỗi DataNode gửi nhịp tim, mặc định 3 giây một lần, báo 'tôi còn sống' kèm dung lượng còn trống. NameNode không gọi DataNode; nó chỉ trả lời nhịp tim và gài lệnh vào câu trả lời. Nếu quá khoảng 10 phút 30 giây không nghe thấy, NameNode đánh dấu DataNode đó là chết và sao chép bù các block của nó.
Như một lớp học trực tuyến, mỗi bạn phải bấm 'có mặt' theo nhịp. Lâu không bấm, giáo viên coi là vắng và giao phần việc cho người khác.
Ngoài nhịp tim, DataNode còn gửi báo cáo block, danh sách đầy đủ các block nó giữ, lúc khởi động và định kỳ 6 giờ một lần.
Trong đồ án bạn có thể tự thấy: tắt một DataNode rồi mở trang 9870, cột 'Last contact' tăng dần đến khi node chuyển sang Dead."

**Hoạt hình:**
- Bốn kệ, mỗi kệ có trái tim nhỏ đập; mỗi nhịp, một chấm nhỏ bay lên NameNode. Đồng hồ nhỏ đếm "3 s".
- NameNode trả về một mũi tên mỏng; thỉnh thoảng mũi tên kèm phong bì lệnh "chép block X sang kệ Y".
- Màn hình lớp học trực tuyến hiện song song: bốn ô video, mỗi ô có nút "Có mặt" sáng theo nhịp.
- Kệ số 3 (cổng 9884) ngừng đập, trái tim chuyển xám. Bên NameNode, một bảng "Last contact" có cột số của kệ 3 tăng: 3 s, 30 s, 5 phút, 10 phút 30 giây; khi chạm mốc, ô chuyển đỏ "Dead". Ô video tương ứng bên lớp học cũng xám với chữ "Vắng".
- Phía dưới: một phong bì dày hơn nhãn "Báo cáo block (6 giờ)" bay lên chậm, đối lập với chấm nhịp tim nhỏ và nhanh.

**Số trên màn hình:** `heartbeat 3 s (mặc định)` · `≈ 10 phút 30 giây → Dead` · `block report 6 giờ (mặc định)` · `:9870 · Last contact`.

---

## Cảnh 6 — Luồng ghi file (4:25–5:20)

**Lời thoại:**
"Bây giờ ghi một file. Client gọi NameNode tạo file. Xin block đầu tiên, NameNode cấp mã block và danh sách DataNode sẽ giữ nó. Client gửi dữ liệu thành các gói khoảng 64 KB tới DataNode thứ nhất; DataNode này vừa ghi vừa chuyển tiếp cho DataNode thứ hai. Xác nhận đi ngược về client. Mỗi DataNode ghi xong thì báo NameNode. Cuối cùng client báo đóng file. Lệnh hdfs dfs -put ghi vào tên tạm có đuôi _COPYING_, xong mới đổi sang tên thật.
Giống chuyền tay một bộ hồ sơ dày: bạn đưa từng xấp cho người thứ nhất, người đó photo cất một bản rồi chuyền xấp cho người thứ hai; người cuối ký nhận, chữ ký truyền ngược về bạn.
Trong đồ án, tab Sơ đồ luồng của dashboard thắp sáng đúng các bước này khi có lượt ghi thật. Có bước quan sát thật trong log NameNode, có bước chỉ suy ra; giao diện ghi rõ. Với replication 1, đường ống chỉ có một DataNode."

**Hoạt hình:**
- Thanh tiến trình 8 bước ngang đầu màn hình: create, addBlock, chọn vị trí, đẩy dữ liệu, ACK, blockReceived, complete, rename. Bước nào đang diễn ra thì ô đó vàng, xong thì xanh.
- Client gửi mũi tên "create" tới NameNode; tên file `Dataset.csv._COPYING_` hiện trong cây thư mục, chữ mờ.
- NameNode trả về mã block và danh sách 3 kệ (góc nhãn "Minh họa: 3 DataNode; cụm đồ án dùng 1").
- Các gói nhỏ hình hộp "64 KB" chạy từ client tới kệ 1; mỗi hộp vừa chạm kệ 1 thì để lại một bản sao và tiếp tục chạy tới kệ 2, rồi kệ 3.
- Dấu ✓ chạy ngược từ kệ 3 → kệ 2 → kệ 1 → client.
- Mỗi kệ phát một phong bì nhỏ "blockReceived" lên NameNode (viền đứt, nhãn "suy ra" khi chuyển sang phiên bản đồ án).
- "complete", rồi chữ `._COPYING_` rụng khỏi tên file, tên chuyển từ mờ sang đậm.
- Ví dụ đời thường hiện ở một khung nhỏ góc trái: ba người đứng hàng, xấp giấy chuyền, chữ ký chạy ngược.
- Phiên bản đồ án: chụp lại tab "Sơ đồ luồng · HDFS ghi file": chỉ còn một kệ trong đường ống; các ô bước 2, 3, 7 viền liền (quan sát từ log `allocate`, `completeFile`), ô 4–6 viền đứt "suy ra", ô 1 và 8 viền chấm "audit log hoặc suy ra".

**Số trên màn hình:** `8 bước` · `gói ≈ 64 KB` · `Dataset.csv._COPYING_` · `log: allocate · completeFile`.

---

## Cảnh 7 — Luồng đọc và safe mode (5:20–5:50)

**Lời thoại:**
"Đọc thì đơn giản hơn: client hỏi NameNode danh sách block và vị trí, rồi đọc thẳng từng block từ DataNode, kiểm checksum. Sai thì chuyển sang bản sao khác. Đúng như hỏi thủ thư rồi tự ra kệ.
Mỗi lần Spark đọc file Parquet aqi_clean trên HDFS, nó đi đúng đường này.
Một chi tiết vận hành: ngay sau khởi động, NameNode ở chế độ an toàn, chỉ đọc, chờ đủ DataNode báo cáo block. Vì vậy script start_all.sh của đồ án chờ safe mode tắt rồi mới bật Spark Streaming, nếu không job sẽ lỗi vì HDFS chưa cho ghi."

**Hoạt hình:**
- Client gửi "getBlockLocations" lên NameNode, nhận về danh sách 4 gạch kèm kệ.
- Client lần lượt kéo từng viên gạch từ kệ về, mỗi viên đi qua một chiếc kính lúp "checksum" và chuyển xanh.
- Một viên gạch bị gắn dấu đỏ "checksum sai" (minh họa), client quay sang kệ khác lấy bản sao.
- Chuyển cảnh: đồng hồ khởi động, NameNode đeo biển "SAFE MODE · chỉ đọc", bốn kệ gửi báo cáo, biển rơi xuống, rồi biểu tượng Spark Streaming mới được bật lên.

**Số trên màn hình:** `getBlockLocations` · `/co/processed/aqi_clean` · `hdfs dfsadmin -safemode wait`.

---

## Cảnh 8 — Tổng kết: bản đồ HDFS của đồ án (5:50–6:00)

**Lời thoại:**
"Tóm lại: NameNode giữ sổ, DataNode giữ gạch, file cắt thành block, block có bản sao, nhịp tim báo sống chết. Video sau: Kafka, cuốn sổ nhật ký chung của dữ liệu luồng."

**Hoạt hình:**
- Cây thư mục của đồ án hiện nhanh: `/co/raw/Dataset.csv` · `/co/processed/aqi_clean/` · `/co/output/model_best/` · `/live/co_stream/` · `/live/_checkpoints/`.
- Thư mục `/live` nhấp nháy, một chấm xanh lá bay ra theo hướng biểu tượng Kafka, chuyển sang video 3.

**Số trên màn hình:** `/co/raw/Dataset.csv 3 179 004 byte` · `/co/processed/aqi_clean 1 251 944 byte`.

---

## Điểm cần duyệt

- Cảnh 5: [TĐ] chỉ nói "muốn thấy tận mắt: tắt một DataNode…", không nói đã làm thử trên cụm. Lời thoại vì vậy dùng "bạn có thể tự thấy", không nói "đồ án đã thử".
- Cảnh 4: chỉ hiện tổng "≈ 4 TB" vì [TĐ] không ghi dung lượng từng DataNode.
- Cảnh 1–8 không dùng số nào của cụm Docker 5 DataNode.
