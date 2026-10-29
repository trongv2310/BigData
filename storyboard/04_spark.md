# Video 4 — Spark: driver và executor, transformation và action, DAG, shuffle, partition

- **Thời lượng mục tiêu:** khoảng 6 phút
- **Nguồn:** [TĐ] chương 4 (Apache Spark, local[*] và spark-submit, SparkSession, DataFrame, Transformation và action, DAG, Shuffle, Partition, Cache, Lineage, Spark UI), chương 2 (MapReduce), chương 1 (Tính cục bộ dữ liệu)

## Bảng phủ khái niệm

| Khái niệm | Ví dụ đời thường | Ví dụ trong đồ án |
|---|---|---|
| Driver / executor | Bếp trưởng đọc thực đơn, chia việc cho phụ bếp | PySpark 3.5.1 chạy `local[*]`: driver và executor trong một tiến trình |
| Transformation / action | Ghi danh sách đi chợ / ra quầy tính tiền | `cleaning.py`: mỗi `.count()` là một action, Spark tính lại từ đầu |
| Cache | Chuẩn bị sẵn nguyên liệu dùng nhiều lần | `day_table()` cache bảng 1 460 ngày vì K-Means chạy 7 lần |
| Job / stage / task, DAG | Món ăn chia công đoạn; công đoạn nào cần gom nguyên liệu thì phải chờ | "CO trung bình theo giờ": stage 1 có 1 task, stage 2 có 4 task |
| Shuffle | Kiểm kê: đem mọi phiếu cùng loại về một bàn | `dropDuplicates`, `GROUP BY` bước 5, `Window.orderBy` |
| Partition | Chia 10 000 bài thi thành các xấp | `coalesce(1)`: `aqi_clean` đúng một file 1 251 944 byte |
| Lineage | Giữ công thức làm lại trang nháp thay vì photo | Mất partition thì Spark tính lại đúng phần đó |

---

## Cảnh 1 — Spark là gì (0:00–0:35)

**Lời thoại:**
"Spark là bộ máy tính toán phân tán. Nó chia một phép tính lớn thành nhiều việc nhỏ chạy song song trên từng phần dữ liệu, và giữ dữ liệu trung gian trong RAM. Thế hệ trước, MapReduce, ghi kết quả trung gian xuống đĩa sau mỗi pha, nên chậm với bài toán lặp nhiều vòng. Spark ra đời để giữ chúng trong bộ nhớ.
Trong đồ án, Spark làm gần hết phần tính: đọc HDFS và Kafka, làm sạch, chạy SQL, huấn luyện mô hình, dự đoán trên luồng."

**Hoạt hình:**
- Hai làn đua. Làn trên "MapReduce": sau mỗi vòng, xe dừng ở trạm "ghi đĩa" (biểu tượng ổ cứng) rồi mới chạy tiếp. Làn dưới "Spark": xe giữ hành lý trên nóc (biểu tượng RAM), chạy liền mạch qua các vòng.
- Sau 3 vòng, xe Spark về trước; dòng chữ "giữ dữ liệu trung gian trong RAM".
- Bốn biểu tượng nhỏ HDFS, Kafka, SQL, MLlib xoay quanh logo Spark.

**Số trên màn hình:** `PySpark 3.5.1`.

---

## Cảnh 2 — Driver và executor: bếp trưởng và phụ bếp (0:35–1:30)

**Lời thoại:**
"Một chương trình Spark có hai vai. Driver chạy chương trình chính, dựng kế hoạch, chia thành các việc nhỏ và phát cho executor. Executor là tiến trình làm việc, chạy các việc đó trên phần dữ liệu của mình.
Hình dung một nhà bếp: bếp trưởng đọc thực đơn, chia việc 'thái hành, rửa rau, ướp thịt' cho các phụ bếp. Phụ bếp làm song song trên phần nguyên liệu của mình, bếp trưởng gom lại thành món.
Trên cụm thật, executor nằm ở nhiều máy, do YARN hay Kubernetes cấp. Đồ án chạy Spark ở chế độ local sao, nghĩa là một tiến trình duy nhất, số luồng bằng số lõi CPU. Bếp trưởng và các phụ bếp đứng chung một căn bếp. Vì thế YARN không cần, và đã tắt từ 13/10/2026 để lấy RAM. Một lệnh thật của đồ án: spark-submit, driver-memory 2g, chạy file huấn luyện 06b_train.py."

**Hoạt hình:**
- Căn bếp: bếp trưởng đội mũ cao (nhãn "Driver") cầm tờ thực đơn. Ba phụ bếp (nhãn "Executor") đứng ở ba thớt.
- Thực đơn tách thành ba thẻ việc bay tới ba phụ bếp. Mỗi phụ bếp có một rổ nguyên liệu riêng (nhãn "partition"). Họ làm song song; mỗi người xong thì đĩa của họ bay về bếp trưởng, ghép thành món.
- Minh họa cụm thật (nhãn "Minh họa: nhiều máy"): ba phụ bếp tách ra ba căn bếp khác nhau, một người quản lý nhãn "YARN" phân phòng.
- Chuyển sang đồ án: ba căn bếp gộp lại thành một căn bếp lớn trong khung laptop "WSL2"; các phụ bếp hóa thành các luồng của một tiến trình, nhãn `local[*]`. Biểu tượng YARN có công tắc gạt sang "Tắt chủ động · 13/10/2026".
- Một terminal hiện dòng lệnh, chữ gõ từng ký tự.

**Số trên màn hình:** `--master local[*]` · `spark-submit --driver-memory 2g pipeline/06b_train.py` · `YARN tắt từ 13/10/2026`.

---

## Cảnh 3 — Transformation và action: danh sách đi chợ (1:30–2:30)

**Lời thoại:**
"Dữ liệu trong Spark thường là DataFrame, một bảng có tên cột và kiểu dữ liệu, chia trên nhiều phần. Có hai loại lệnh trên DataFrame.
Transformation chỉ mô tả việc cần làm, chưa tính gì: chọn cột, lọc dòng, thêm cột, nhóm. Action mới thật sự chạy: đếm, hiện kết quả, ghi ra file.
Như ghi danh sách đi chợ: viết thêm bao nhiêu món cũng chưa tốn đồng nào. Chỉ khi ra quầy tính tiền mới mất tiền, và lúc đó bạn đã kịp gạch bớt món trùng. Spark cũng vậy: nhờ chờ tới action, nó nhìn được cả chuỗi và tối ưu chung.
Trong đồ án, bước làm sạch gọi count để đếm số dòng bị bỏ theo từng quy tắc. Mỗi lần count là một action, tức một lần Spark đọc và tính lại từ đầu. Với 35 064 dòng thì không đáng kể; với dữ liệu lớn thì nên cache trước. Ví dụ, bước phân cụm cache bảng 1 460 ngày vì K-Means chạy 7 lần trên cùng bảng đó."

**Hoạt hình:**
- Một tờ danh sách đi chợ; bàn tay viết thêm các dòng `select`, `where`, `withColumn`, `groupBy`. Chiếc ví bên cạnh vẫn nguyên, đồng hồ đứng yên.
- Một dòng trùng được gạch đi (minh họa tối ưu).
- Chữ `count()` xuất hiện; tờ danh sách trượt tới quầy thu ngân, ví mở ra, đồng hồ bắt đầu quay. Cả chuỗi bên trên sáng lên từ trên xuống.
- Hai cột: "Transformation (trễ)" màu xám nhạt, "Action (chạy ngay)" màu vàng. Các từ khóa rơi vào đúng cột.
- Chuyển sang đồ án: đoạn mã `pipeline/cleaning.py` cuộn qua, 5 dòng `.count()` (dòng 15, 17, 21, 23, 25) lần lượt sáng vàng; mỗi lần sáng, một mũi tên quay ngược về `/co/raw/Dataset.csv` và chạy lại cả chuỗi. Bộ đếm góc màn hình: "5 action = 5 lần đọc lại".
- Cuối: một chiếc tủ lạnh nhãn `cache()` chứa bảng "1 460 ngày"; 7 mũi tên `k = 2 … 8` lấy từ tủ lạnh thay vì quay về nguồn.

**Số trên màn hình:** `35 064 dòng` · `5 lần count() trong cleaning.py` · `cache() · 1 460 ngày · k = 2 → 8 (7 lần)`.

---

## Cảnh 4 — Job, stage, task và DAG (2:30–3:35)

**Lời thoại:**
"Mỗi action sinh ra một job. Spark vẽ job thành một đồ thị có hướng không chu trình, gọi là DAG. Job được cắt thành các stage tại những chỗ phải xáo dữ liệu. Mỗi stage gồm nhiều task, mỗi task xử lý một partition trên một lõi CPU.
Trong bếp: thái và ướp thì mỗi phụ bếp tự làm trên rổ của mình, liền một mạch. Nhưng đến bước 'gom tất cả thịt bò về một chảo' thì phải dừng, đem nguyên liệu từ mọi rổ về một chỗ. Chỗ dừng đó là ranh giới stage.
Ví dụ trong đồ án: tính CO trung bình theo giờ trên dữ liệu đang nằm trong 1 partition. Stage 1 có 1 task, đọc và cộng dồn một phần. Rồi xáo theo giờ ra 4 partition, vì phiên Spark của đồ án đặt số partition sau xáo là 4 thay cho mặc định 200. Stage 2 có 4 task, gộp và chia trung bình. Muốn xem DAG thật, mở Spark UI ở cổng 4040 khi ứng dụng đang chạy; tab Spark của dashboard cũng đọc từ đó."

**Hoạt hình:**
- Một hộp "Job" xuất hiện khi chữ `write`/`show` chớp lên. Bên trong, các hình tròn nối mũi tên một chiều từ trái sang phải (DAG). Một mũi tên thử vòng ngược lại bị gạch đỏ: "không chu trình".
- Đường cắt dọc nét đứt đỏ chia DAG thành hai khung "Stage 1" và "Stage 2". Tại đường cắt có biểu tượng hai mũi tên đan chéo "shuffle".
- Khung Stage 1: một ô task màu vàng chạy. Khung Stage 2: bốn ô task vàng chạy song song, bốn lõi CPU nhỏ sáng theo.
- Ví dụ bếp chạy song song ở dải dưới: các phụ bếp làm việc liền mạch → biển "Dừng: gom về một chảo" → tiếp tục.
- Một thanh trượt `spark.sql.shuffle.partitions` từ 200 kéo về 4: số ô task ở stage 2 giảm từ một lưới dày xuống còn 4.
- Cuối: ảnh chụp tab Spark UI `:4040`, thẻ "Jobs" và "Stages".

**Số trên màn hình:** `Stage 1: 1 task` · `Stage 2: 4 task` · `shuffle.partitions = 4 (mặc định 200)` · `Spark UI :4040–4043`.

---

## Cảnh 5 — Shuffle: phần đắt nhất (3:35–4:35)

**Lời thoại:**
"Shuffle là bước xáo dữ liệu giữa các partition qua mạng hoặc đĩa, để các dòng cùng khóa về chung một chỗ. Đây thường là phần đắt nhất của một job.
Có hai loại biến đổi. Biến đổi hẹp như lọc hay thêm cột: mỗi partition tự làm, không cần ai. Biến đổi rộng như nhóm, nối bảng, sắp xếp, bỏ trùng: cần dữ liệu từ nhiều partition, nên phải xáo.
Ý tưởng này có từ MapReduce. Lấy một dòng thật của bộ Kaggle: 3 giờ UTC ngày 01/07/2025, CO 328,48. Pha map phát ra cặp 'giờ 10, 328,48', vì 3 giờ UTC là 10 giờ sáng Việt Nam. Shuffle dồn mọi cặp 'giờ 10' về cùng một chỗ. Pha reduce cộng lại và chia trung bình. Đó chính là câu GROUP BY hour ở bước 5.
Trong đồ án, bỏ trùng theo thời gian, các GROUP BY của bước 5, và hàm cửa sổ sắp theo thời gian khi tạo đặc trưng độ trễ đều gây shuffle. Hàm cửa sổ không chia nhóm còn dồn toàn bộ dữ liệu về một partition. Với 35 064 dòng thì chấp nhận được, nhưng là điểm cần sửa nếu dữ liệu lớn hàng tỷ dòng."

**Hoạt hình:**
- Bốn rổ partition, mỗi rổ chứa các viên bi màu trộn lẫn (màu = giờ trong ngày).
- Biến đổi hẹp: mỗi rổ có một chiếc rây riêng lọc bớt bi, không bi nào rời rổ. Nhãn xanh "hẹp".
- Biến đổi rộng: các viên bi bay chéo nhau giữa các rổ (đường bay đan xen như mạng nhện), cuối cùng mỗi rổ chỉ còn một màu. Nhãn cam "rộng · shuffle". Một đồng hồ chi phí cạnh đó chạy nhanh.
- Ví dụ MapReduce: một dòng CSV `01-07-25 3:00, co=328.48` trượt vào. Đồng hồ `03:00 UTC` quay +7 thành `10:00`. Dòng biến thành thẻ `(giờ 10, 328,48)`; thẻ bay vào một hộp nhãn "giờ 10" cùng nhiều thẻ khác; hộp cộng lại thành "trung bình".
- Đồ án: ba nhãn rơi xuống lần lượt `dropDuplicates(["datetime_utc"])`, `GROUP BY hour`, `Window.orderBy("datetime_utc")`. Với nhãn thứ ba, mọi viên bi từ bốn rổ dồn vào **một** rổ duy nhất, rổ đó phình to, đèn cảnh báo vàng của Spark nhấp nháy.

**Số trên màn hình:** `03:00 UTC = 10:00 giờ VN` · `(giờ 10, 328,48)` · `35 064 dòng`.

---

## Cảnh 6 — Partition của Spark (4:35–5:25)

**Lời thoại:**
"Partition là một mẩu của DataFrame mà một task xử lý. Số partition quyết định bao nhiêu việc chạy song song, và khi ghi ra thì mỗi partition thành một file.
Như chia 10 000 bài thi thành các xấp: chia càng nhiều xấp thì càng nhiều người chấm cùng lúc, nhưng xấp quá mỏng thì công phát và thu bài lại tốn hơn công chấm.
Có hai cách đổi số partition: repartition chia lại, có xáo; coalesce gộp bớt, không xáo, chỉ giảm được.
Trong đồ án, bước làm sạch ghi bằng coalesce 1, để bảng aqi_clean là đúng một file Parquet 1 251 944 byte, dễ kiểm và tránh nhiều file nhỏ. Job streaming dùng 2 partition sau xáo.
Và nhớ: partition của Spark không phải partition của Kafka, càng không phải block của HDFS, dù khi đọc file Spark thường tạo một partition cho mỗi block."

**Hoạt hình:**
- Chồng "10 000 bài" chia thành 4 xấp, rồi 50 xấp; số người chấm tăng theo. Chia tiếp thành rất nhiều xấp mỏng: hình người tổ trưởng chạy tới chạy lui, biểu tượng "chi phí lập lịch" phình lên.
- Hai nút: `repartition(n)` làm các viên bi bay chéo (có shuffle); `coalesce(n)` làm các rổ cạnh nhau gộp vào nhau, không có đường bay chéo.
- Đồ án: nhiều rổ gộp thành một rổ bằng `coalesce(1)`, rổ đổ ra đúng một file Parquet có nhãn kích thước.
- Bảng ba cột cuối cảnh, mỗi cột một biểu tượng: "Block HDFS (viên gạch)" · "Partition Kafka (dải sổ)" · "Partition Spark (rổ)", giữa các cột có dấu ≠. Một mũi tên mảnh từ viên gạch tới rổ ghi "khi đọc file: thường 1 block → 1 partition".

**Số trên màn hình:** `coalesce(1) → aqi_clean 1 251 944 byte` · `streaming: shuffle.partitions = 2`.

---

## Cảnh 7 — Lineage: vì sao Spark chịu lỗi (5:25–5:50)

**Lời thoại:**
"Nếu một partition bị mất giữa chừng thì sao? Spark nhớ chuỗi phép biến đổi đã tạo ra mỗi partition, gọi là dòng dõi. Mất phần nào thì tính lại đúng phần đó từ dữ liệu gốc.
Giống như thay vì photo dự phòng từng trang nháp, bạn giữ công thức 'trang 3 bằng dữ liệu thô dòng 200 đến 300, lọc bỏ số âm, cộng 7 giờ'. Mất trang 3 thì làm lại theo công thức."

**Hoạt hình:**
- Bốn rổ đã tính xong. Rổ số 3 vỡ, chuyển đỏ.
- Một sợi chỉ vàng mọc ra từ chỗ rổ 3, chạy ngược qua các bước `+7 giờ` ← `lọc` ← `dữ liệu gốc dòng 200–300`, rồi chạy xuôi lại; rổ 3 được dựng lại, chuyển xanh. Ba rổ kia không bị đụng tới.
- Song song: tờ nháp "trang 3" bị xé; một mẩu giấy công thức hiện ra, bàn tay viết lại trang 3.

**Số trên màn hình:** `dòng 200 → 300` · `+7 giờ` (đúng ví dụ của [TĐ]).

---

## Cảnh 8 — Tổng kết (5:50–6:00)

**Lời thoại:**
"Driver lập kế hoạch, executor làm. Transformation chỉ ghi danh sách, action mới chạy. Shuffle cắt job thành stage. Partition quyết định độ song song. Video sau: dùng Spark bằng SQL, và chạy Spark liên tục trên dữ liệu luồng."

**Hoạt hình:**
- Căn bếp thu nhỏ; trên tường bếp hiện một bảng SQL, ngoài cửa sổ là băng chuyền Kafka chạy. Chuyển sang video 5.

**Số trên màn hình:** không có số mới.

---

## Điểm cần duyệt

- Cảnh 3: số 5 lần `.count()` đếm trực tiếp trong `pipeline/cleaning.py` (dòng 15, 17, 21, 23, 25), [TĐ] không ghi số này.
- Cảnh 5: mục MapReduce của [TĐ] viết `(01-07-25 3:00, co=280)`, nhưng dòng thật trong `Data/hcmc_aqi_dataset_after_preprocess_final.csv` (dòng 28 493) có CO = 328,48. Kịch bản dùng số thật 328,48; [TĐ] có lẽ dùng 280 làm số minh họa. Nên sửa [TĐ] cho khớp.
