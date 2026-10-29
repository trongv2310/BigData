# Video 3 — Kafka: topic, partition, offset, producer, consumer, lag

- **Thời lượng mục tiêu:** khoảng 5 phút 30 giây
- **Nguồn:** [TĐ] chương 3 (Kafka, Topic, Partition, Offset, Producer, Consumer, Broker và KRaft, Lưu giữ tin, Lag, Ngữ nghĩa giao nhận); [INFRA] mục "Hai luồng song song qua Kafka"; [HO] mục A5 và khởi động nguội

## Bảng phủ khái niệm

| Khái niệm | Ví dụ đời thường | Ví dụ trong đồ án |
|---|---|---|
| Kafka | Cuốn sổ nhật ký chung ở bàn trực, không ai được tẩy dòng cũ | Kafka 4.1.2, broker `localhost:9092`, chế độ KRaft |
| Topic | Mỗi loại tin một cuốn sổ riêng | `hcmc_co_stream` (thô) và `hcmc_co_enriched` (có dự đoán) |
| Partition | Siêu thị mở 4 quầy thu ngân | Cả hai topic có 1 partition, để giữ thứ tự theo thời gian |
| Offset | Số dòng in ở lề sổ | Offset 52: bản ghi OpenWeather thật đầu tiên đi hết chuỗi |
| Producer | Người ghi tin vào sổ | `live_producer_owm.py` (mỗi 300 giây) và `replay_kafka_producer.py` (mỗi 10 giây) |
| Consumer, group | Mỗi bộ phận kẹp mẩu giấy "đã đọc tới dòng mấy" | Spark không dùng consumer group, nhớ offset trong checkpoint HDFS |
| Lag | Số dòng mới mà bộ phận chưa đọc tới | `spark:co_stream`, lag = 1 rồi về 0 |
| Tin bất biến | Viết bút mực, không tẩy được | Offset 44 và 45 trùng nhau, không xóa được, lọc lúc đọc |

---

## Cảnh 1 — Vì sao cần một tầng ở giữa (0:00–0:40)

**Lời thoại:**
"Ở đồ án có hai nơi sinh dữ liệu: OpenWeather và chương trình phát lại. Và có một nơi xử lý: Spark. Nếu nối thẳng, nơi gửi phải chờ nơi nhận; nơi nhận chậm hay tạm dừng là mất tin. Kafka đứng ở giữa như một băng chuyền: ai gửi cứ gửi, ai đọc cứ đọc theo tốc độ của mình, không ai phải chờ ai."

**Hoạt hình:**
- Trái: hai nguồn, chấm xanh lá "OpenWeather" và chấm xanh dương "Phát lại · MÔ PHỎNG". Phải: biểu tượng Spark.
- Lần thứ nhất: dây nối thẳng. Spark đeo biển "tạm dừng", các chấm đâm vào Spark rồi vỡ, chuyển đỏ "mất tin".
- Lần thứ hai: một băng chuyền dài đặt vào giữa, nhãn "Kafka". Spark lại tạm dừng; các chấm xếp hàng trên băng chuyền, không vỡ. Spark bật lại và lấy dần các chấm.

**Số trên màn hình:** `Kafka 4.1.2` · `localhost:9092`.

---

## Cảnh 2 — Kafka là cuốn sổ nhật ký, topic là từng cuốn (0:40–1:30)

**Lời thoại:**
"Hãy nghĩ tới một cuốn sổ nhật ký chung đặt ở bàn trực. Ai có tin thì ghi tiếp vào dòng cuối; không ai được tẩy dòng cũ. Mỗi bộ phận đọc sổ kẹp một mẩu giấy đánh dấu đã đọc tới dòng số mấy. Bộ phận nghỉ một tuần quay lại vẫn đọc tiếp được từ chỗ kẹp. Đó chính là Kafka. Khác hàng đợi thông thường: tin không bị xóa khi đã đọc.
Topic là một cuốn sổ có tên, gom các tin cùng loại. Đồ án có hai topic. hcmc_co_stream nhận bản ghi thô từ hai producer; Spark đọc topic này. hcmc_co_enriched nhận bản ghi đã có CO dự đoán do Spark ghi; backend đọc topic này để đẩy lên dashboard. Vì hai nguồn đi chung một topic, mọi nơi đọc đều phải lọc theo trường provider."

**Hoạt hình:**
- Một cuốn sổ mở trên bàn trực, các dòng kẻ ngang. Một bàn tay viết thêm dòng ở cuối; một cục tẩy tiến tới dòng cũ, bị chặn bởi biểu tượng khóa.
- Ba mẩu giấy kẹp ở ba vị trí khác nhau, mỗi mẩu ghi tên một bộ phận. Một mẩu đứng yên lâu (bộ phận nghỉ), các dòng mới mọc thêm phía dưới; mẩu đó sau đó trượt xuống dần.
- Cuốn sổ nhân thành hai cuốn, gáy ghi `hcmc_co_stream` và `hcmc_co_enriched`.
- Mũi tên: hai producer → cuốn `hcmc_co_stream` → Spark → cuốn `hcmc_co_enriched` → backend.
- Trong cuốn `hcmc_co_stream`, các dòng xen kẽ chấm xanh lá và xanh dương; một cái phễu "lọc theo provider" tách chúng thành hai cột.

**Số trên màn hình:** `hcmc_co_stream` · `hcmc_co_enriched` · `provider: openweather | replay-dataset`.

---

## Cảnh 3 — Partition (1:30–2:10)

**Lời thoại:**
"Mỗi topic được chia thành một hay nhiều partition. Partition là một nhật ký có thứ tự, chỉ ghi nối thêm, và là đơn vị song song của Kafka.
Như siêu thị mở 4 quầy thu ngân: khách ở quầy nào thì được tính tiền đúng thứ tự trong quầy đó, nhưng không ai đảm bảo người ở quầy 1 xong trước người ở quầy 3. Nhiều partition thì nhiều người đọc song song, nhưng thứ tự chỉ đúng trong từng partition.
Đồ án chọn 1 partition cho cả hai topic. Lưu lượng chỉ vài tin mỗi giây, và ta muốn thứ tự toàn cục theo thời gian. Lưu ý: partition của Kafka khác partition của Spark mà ta gặp ở video sau."

**Hoạt hình:**
- Siêu thị có 4 quầy, khách xếp 4 hàng; mỗi hàng có số thứ tự riêng 1, 2, 3. Khách quầy 3 ra cửa trước khách quầy 1, một dấu "?" bật lên trên đồng hồ chung.
- Phần minh họa (nhãn "Minh họa: 4 partition"): cuốn sổ tách thành 4 dải song song, tin rơi vào các dải.
- Gộp về 1 dải duy nhất (nhãn "Đồ án: 1 partition"): mọi tin xếp đúng một hàng theo thời gian.
- Góc dưới: hai từ "partition Kafka" và "partition Spark" đặt cạnh nhau với dấu ≠.

**Số trên màn hình:** `4 quầy` · `1 partition` · `replication 1`.

---

## Cảnh 4 — Offset: số dòng ở lề sổ (2:10–2:55)

**Lời thoại:**
"Offset là số thứ tự của một tin trong partition, bắt đầu từ 0, tăng dần và không bao giờ dùng lại. Nó như số dòng in sẵn ở lề cuốn sổ.
Offset là cách đồ án chỉ tay vào một bản ghi cụ thể. Offset 52 là bản ghi OpenWeather thật đầu tiên đi hết chuỗi: CO 251,31 lúc 13 giờ 03 phút 22 giây ngày 14/10/2026. Offset 1647 có CO 298,52, Spark dự đoán 296,0.
Vì tin không sửa được, lỗi cũng nằm lại mãi. Offset 44 và 45 là hai tin trùng, do producer cũ gửi lại khi khởi động. Kafka không xóa được, nên khi truy vấn phải dùng DISTINCT hoặc lọc theo offset. Producer mới đọc lại tin cuối của topic để không gửi trùng nữa. Kafka chỉ xóa tin theo thời gian hoặc dung lượng cấu hình, mặc định 7 ngày, không xóa vì đã đọc."

**Hoạt hình:**
- Phóng to lề cuốn sổ: số 0, 1, 2… chạy dọc. Một tin mới viết vào, số mới hiện ra ở lề, số cũ không bao giờ đổi.
- Cuộn nhanh xuống dòng 52: dòng đó sáng xanh lá, bóng chữ hiện `CO 251,31 · 13:03:22 · 14/10/2026`.
- Cuộn tiếp tới 1647: dòng xanh lá với hai số đặt cạnh nhau, `298,52` (thật) và `296,0` (tím, dự đoán).
- Cuộn ngược về 44 và 45: hai dòng giống hệt nhau, nhấp nháy cam. Cục tẩy lại bị chặn bởi khóa. Một chiếc phễu ghi `DISTINCT` hiện ra ở phía người đọc, hai dòng đi vào phễu thành một.
- Góc dưới: lịch có vòng tròn "7 ngày (mặc định)" trên đoạn đầu cuốn sổ.

**Số trên màn hình:** `offset 52 · CO 251,31 µg/m³ · 13:03:22 14/10/2026` · `offset 1647 · CO 298,52 · dự đoán 296,0` · `offset 44 = 45` · `retention 7 ngày (mặc định)`.

---

## Cảnh 5 — Producer: hai người ghi sổ (2:55–3:45)

**Lời thoại:**
"Producer là chương trình gửi tin vào topic. Nó đóng gói tin, chọn partition và chờ broker xác nhận. Đồ án có hai producer, ghi vào cùng topic hcmc_co_stream.
Producer thứ nhất hỏi OpenWeather mỗi 300 giây tại một tọa độ ở TP.HCM. Nó chỉ gửi khi nguồn có số mới, vì thời điểm dt mà API trả về không đáng tin, có lúc còn lùi về quá khứ.
Producer thứ hai phát lại bộ Kaggle: mỗi dòng giờ cách nhau 10 giây ở tốc độ 1×, 1 giây ở 10×. Ở 60× danh nghĩa khoảng 6 tin mỗi giây, nhưng đo thật chỉ khoảng 4,4, vì producer chờ xác nhận của broker trước khi gửi tin tiếp. Đây là mô phỏng, không phải thời gian thực. Mỗi tin phát lại mang hai giờ: datetime là lúc phát, originalDatetime là giờ gốc trong dữ liệu."

**Hoạt hình:**
- Hai nhân vật cầm bút đứng trước cuốn sổ `hcmc_co_stream`.
- Nhân vật xanh lá đeo tai nghe vệ tinh, đồng hồ cát lật mỗi "300 s". Mỗi lần lật, nó hỏi một biểu tượng đám mây "OpenWeather"; đám mây trả về một tấm thẻ có `dt`. Lần thứ hai tấm thẻ có `dt` lùi về trước: thẻ chuyển xám, bị bỏ vào thùng "bỏ qua, không gửi".
- Nhân vật xanh dương đeo biển "MÔ PHỎNG", cầm một cuộn băng CSV. Núm vặn tốc độ 1× → 10× → 60×: tốc độ viết tăng. Bên cạnh có hai đồng hồ: "danh nghĩa ≈ 6 tin/s" và "đo thật ≈ 4,4 tin/s"; mỗi tin viết xong phải chờ một dấu ✓ từ broker bay về mới viết tin tiếp.
- Một tin phát lại phóng to: hai nhãn giờ `datetime = lúc phát` và `originalDatetime = giờ gốc`.

**Số trên màn hình:** `mỗi 300 s` · `(10.8231, 106.6297)` · `1× = 10 s · 10× = 1 s · 60× ≈ 6 tin/s danh nghĩa` · `đo thật ≈ 4,4 tin/s`.

---

## Cảnh 6 — Consumer và một điều hay bị hỏi (3:45–4:30)

**Lời thoại:**
"Consumer là chương trình đọc tin. Các consumer cùng tên nhóm chia nhau partition, và nhóm lưu offset đã đọc vào một topic nội bộ của Kafka. Như các bộ phận kẹp mẩu giấy đánh dấu.
Điều hay bị hỏi: Spark Structured Streaming đọc Kafka nhưng không dùng consumer group để nhớ offset. Nó tự ghi offset của từng lô vào checkpoint trên HDFS, thư mục /live/_checkpoints/co_stream. Vì vậy công cụ kafka-consumer-groups không thấy Spark. Backend phải đọc checkpoint đó để tính độ trễ, và dashboard hiện nó với tên spark:co_stream, có ghi chú đây không phải consumer group thật."

**Hoạt hình:**
- Hai mẩu giấy kẹp vào cuốn sổ; một ngăn kéo nhỏ nhãn `__consumer_offsets` ở cạnh bàn, các mẩu giấy được cất vào đó.
- Spark tiến lại, nhưng không lấy mẩu giấy từ ngăn kéo. Nó cầm một cuốn sổ tay riêng, đi về một kệ sách HDFS và cất sổ tay tại thư mục `/live/_checkpoints/co_stream`.
- Một kính lúp nhãn `kafka-consumer-groups.sh` quét ngăn kéo: không thấy Spark, hiện chữ "không có".
- Backend đi tới kệ HDFS, mở sổ tay của Spark (mũi tên một chiều, nhãn "chỉ đọc"), rồi hiện thẻ `spark:co_stream` trên dashboard kèm huy hiệu "Spark checkpoint".

**Số trên màn hình:** `__consumer_offsets` · `/live/_checkpoints/co_stream` · `spark:co_stream`.

---

## Cảnh 7 — Lag (4:30–5:15)

**Lời thoại:**
"Lag là số tin đã có trong partition mà người đọc chưa xử lý: lấy offset của tin sẽ ghi tiếp theo trừ đi offset đã xử lý xong. Lag tăng mãi nghĩa là người đọc chậm hơn người ghi.
Như hộp thư: thư đã tới mà bạn chưa đọc. Hôm nào thư tới nhiều hơn bạn đọc kịp, chồng thư chưa đọc cứ cao dần.
Trong đồ án: partition có tin tới offset 1647, tức offset kế tiếp là 1648. Spark đã xử lý xong tới 1647, chưa gồm tin vừa tới. Lag bằng 1648 trừ 1647, bằng 1. Ở lô kế tiếp, lag về 0. Dashboard đã quan sát đúng như vậy: lag lên 1 ngay khi producer gửi tin mới, rồi về 0 khi Spark ghi nhận."

**Hoạt hình:**
- Một hộp thư; thư rơi vào với nhịp nhanh hơn nhịp một bàn tay lấy thư ra. Chồng thư cao dần, cột chỉ số "chưa đọc" chuyển từ xanh sang cam.
- Chuyển sang cuốn sổ: hai con trỏ trên lề. Con trỏ đỏ "log end offset" ở 1648; con trỏ xám "đã xử lý" ở 1647. Khoảng giữa hai con trỏ tô cam, ghi `lag = 1`.
- Biểu tượng Spark chạy một lô (vòng quay nhỏ), con trỏ xám trượt xuống bằng con trỏ đỏ, ô cam biến mất, `lag = 0`, dòng chữ chuyển xanh.
- Cuối cảnh: ảnh chụp thẻ `spark:co_stream` trên tab Kafka, con số lag nhảy 1 → 0.

**Số trên màn hình:** `lag = log end offset − committed offset` · `1648 − 1647 = 1` · `→ 0`.

---

## Cảnh 8 — Tổng kết (5:15–5:30)

**Lời thoại:**
"Tóm lại: topic là cuốn sổ, partition là các trang song song, offset là số dòng, producer viết, consumer đọc, lag là phần chưa đọc. Kafka giữ tin; còn ai tính toán trên những tin này? Video sau: Spark."

**Hoạt hình:**
- Sáu từ khóa xếp quanh cuốn sổ, nối vào bằng dây mảnh.
- Một chấm xanh lá rời trang sổ, bay về chiếc mũ bếp trưởng của Spark.

**Số trên màn hình:** không có số mới.

---

## Điểm cần duyệt

- Cảnh 5: nguồn chỉ ghi tọa độ (10.8231, 106.6297), không ghi tên địa điểm, nên lời thoại chỉ nói "một tọa độ ở TP.HCM".
- Cảnh 7: ví dụ 1648 − 1647 là ví dụ có số của [TĐ] mục Lag, dựa trên offset 1647 có thật; không phải ảnh chụp một lần đo cụ thể.
