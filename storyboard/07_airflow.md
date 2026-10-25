# Video 7 — Airflow: DAG, task, trigger rule

- **Thời lượng mục tiêu:** khoảng 4 phút 30 giây
- **Nguồn:** [TĐ] chương 6 (Apache Airflow, DAG hcmc_co_pipeline, Trigger rule), chương 1 (Chạy lại an toàn); [INFRA] mục Airflow; [HO] mục A6 và lần chạy DAG trên `/co`

## Bảng phủ khái niệm

| Khái niệm | Ví dụ đời thường | Ví dụ trong đồ án |
|---|---|---|
| Airflow, điều phối | Quản lý dây chuyền nấu ăn gọi từng người đúng lượt | Airflow 3 trong WSL, `bash infra/airflow.sh start` |
| DAG | Danh sách "rửa rau xong mới thái, thái xong mới xào" | `hcmc_co_pipeline`: 7 task nối tiếp + 2 task ghi |
| Task, operator | Mỗi người một việc, mỗi việc một loại dụng cụ | `BashOperator` gọi `spark-submit` từng script pipeline |
| Params | Tờ ghi chú kèm đơn hàng: "làm ít cay" | `co_root` (mặc định `/co`), `overwrite` (mặc định false) |
| Trigger rule | Tiệc chỉ khai mạc khi mọi món đã xong; có món hỏng thì báo quản lý | `record_dag_success` dùng `all_success`, `record_dag_failure` dùng `one_failed` |
| Chạy lại an toàn | Nút "đặt 26 độ" bấm mấy lần vẫn 26 độ | Bước đã có đầu ra thì dừng mã 2, trừ khi `overwrite=true` |

---

## Cảnh 1 — Vấn đề: chín bước, ai bấm? (0:00–0:30)

**Lời thoại:**
"Phần lô của đồ án có nhiều bước: đưa dữ liệu lên HDFS, kiểm tra, làm sạch, phân tích SQL, phân cụm, huấn luyện. Mỗi bước là một script, bước sau đọc kết quả bước trước. Chạy tay từng lệnh thì dễ quên, dễ chạy sai thứ tự, và khi một bước hỏng thì không ai biết. Airflow sinh ra để làm việc đó thay ta."

**Hoạt hình:**
- Một người đứng trước bảy nút bấm, mỗi nút một tên bước. Họ bấm nhầm thứ tự: nút "clean" sáng trước nút "check_raw", một dấu ✗ đỏ bật lên.
- Một nút sáng đỏ (hỏng) nhưng các nút sau vẫn bị bấm tiếp, đỏ lan.
- Bảy nút trượt vào một bảng phân công có mũi tên; logo Airflow hiện lên.

**Số trên màn hình:** không có số.

---

## Cảnh 2 — DAG và task (0:30–1:30)

**Lời thoại:**
"Airflow mô tả quy trình bằng Python dưới dạng một DAG: đồ thị có hướng, không chu trình. Mỗi nút là một task. Mũi tên a sang b nghĩa là b chạy sau khi a xong. Loại task gọi là operator: BashOperator chạy một lệnh shell, PythonOperator chạy một hàm Python.
Giống một người quản lý dây chuyền nấu ăn có danh sách 'rửa rau xong mới thái, thái xong mới xào'. Người đó tự gọi từng người đúng lượt, ai làm hỏng thì dừng các bước sau và ghi lại ai hỏng ở đâu.
DAG của đồ án tên hcmc_co_pipeline. Bảy task nối tiếp: prepare_upload cho bước 1 và 2, check_raw bước 3, clean bước 4, sql_analysis bước 5, cluster_days và cluster_stability cho bước 6a, train_model cho bước 6b và 7. Mỗi task là một BashOperator gọi spark-submit chạy đúng script cũ của pipeline. Chuỗi chạy nối tiếp, không song song, vì mỗi task Spark tốn 1 đến 2 GB RAM, mà máy ảo chỉ có khoảng 7,7 GB."

**Hoạt hình:**
- Ba hình tròn nối mũi tên `a → b → c`. Một mũi tên thử nối từ c ngược về a, bị gạch đỏ: "không chu trình".
- Khung đời thường: ba đầu bếp xếp hàng "rửa → thái → xào"; quản lý cầm bảng gọi từng người, người được gọi thì sáng lên.
- Chuyển sang đồ án: bảy hình chữ nhật xếp từ trái sang phải, tên hiện lần lượt kèm số bước nhỏ phía dưới:
  `prepare_upload (1–2)` → `check_raw (3)` → `clean (4)` → `sql_analysis (5)` → `cluster_days (6a)` → `cluster_stability (6a)` → `train_model (6b, 7)`.
- Mỗi hình chữ nhật có biểu tượng terminal nhỏ "BashOperator · spark-submit".
- Một thanh RAM nhỏ góc phải: mỗi task khi chạy chiếm "1–2 GB" của "≈ 7,7 GB"; hai task không bao giờ cùng chiếm thanh.

**Số trên màn hình:** `hcmc_co_pipeline` · `7 task nối tiếp` · `1–2 GB / task` · `≈ 7,7 GB RAM`.

---

## Cảnh 3 — Cấu hình an toàn và tham số (1:30–2:15)

**Lời thoại:**
"DAG này được cấu hình thận trọng. Tạm dừng khi tạo. Không có lịch, chỉ chạy khi kích hoạt tay. Mỗi lúc chỉ một lượt chạy. Không tự thử lại. Mỗi task tối đa 45 phút.
Mỗi lượt chạy nhận tham số, như tờ ghi chú kèm đơn hàng. co_root mặc định là /co; đổi thành /co_airflow_test để thử cả quy trình mà không đụng kết quả đã giao. overwrite mặc định là false.
overwrite liên quan tới tính chạy lại an toàn. Nút 'đặt điều hòa 26 độ' bấm năm lần vẫn 26 độ; nút 'tăng 1 độ' bấm năm lần thành 31 độ. Pipeline của đồ án theo kiểu thứ nhất: từ bước 4 trở đi, nếu đầu ra đã có thì dừng với mã 2, trừ khi chạy với overwrite."

**Hoạt hình:**
- Một bảng cấu hình với các công tắc lật lần lượt: `paused khi tạo: bật` · `schedule = None` · `max_active_runs = 1` · `retries = 0` · `timeout 45 phút/task`.
- Một tờ ghi chú dán vào lượt chạy: `co_root: /co`, gạch đi, viết `/co_airflow_test`; `overwrite: false`.
- Khung đời thường: hai điều khiển điều hòa. Điều khiển "Đặt 26°" bấm năm lần, màn hình vẫn 26. Điều khiển "+1°" bấm năm lần, màn hình 26 → 31, chuyển đỏ.
- Đồ án: task `clean` thấy thư mục `aqi_clean` đã có trên HDFS, giơ biển "Đã có, dừng · mã 2". Khi tờ ghi chú đổi sang `overwrite: true`, biển hạ xuống và task chạy.

**Số trên màn hình:** `schedule=None` · `max_active_runs=1` · `retries=0` · `45 phút/task` · `co_root=/co` · `overwrite=false` · `mã thoát 2`.

---

## Cảnh 4 — Trigger rule (2:15–3:15)

**Lời thoại:**
"Một task được chạy khi nào? Trigger rule quyết định, dựa trên trạng thái các task đứng trước. Mặc định là all_success: mọi task trước đều thành công. one_failed: có ít nhất một task trước thất bại. all_done: mọi task trước đã kết thúc, bất kể kết quả.
Như một buổi tiệc: chỉ khai mạc khi mọi món đã xong, đó là all_success. Có một món bị hỏng thì gọi quản lý ngay, đó là one_failed.
Cuối DAG của đồ án có hai task ghi kết quả. record_dag_success dùng all_success, ghi bước 9 là done. record_dag_failure dùng one_failed, ghi bước 9 là failed. Cả hai gọi cùng script 09_record_dag.py. Chỉ task cuối nối vào hai task này, nên task nào trước đó lỗi thì các task sau thành upstream_failed, lan tới tận cuối."

**Hoạt hình:**
- Ba thẻ quy tắc lật lên: `all_success` (ba dấu ✓ xanh → cửa mở), `one_failed` (một dấu ✗ đỏ trong ba → chuông báo), `all_done` (✓ ✗ ✓ → cửa mở với biểu tượng "kết thúc").
- Khung đời thường: bàn tiệc có ba đĩa; khi cả ba có dấu ✓ thì rèm sân khấu kéo ra. Lần khác một đĩa cháy đen, một nhân vật quản lý chạy vào.
- Đồ án: chuỗi bảy task từ cảnh 2, phía cuối tách hai nhánh tới `record_dag_success · all_success` (viền xanh) và `record_dag_failure · one_failed` (viền đỏ). Cả hai trỏ tới một hộp `09_record_dag.py`, hộp này viết vào ô "Bước 9" của `step_summary.json`.

**Số trên màn hình:** `all_success · one_failed · all_done` · `09_record_dag.py --state done | failed`.

---

## Cảnh 5 — Hai lần chạy thật: đường thành công và đường lỗi (3:15–4:10)

**Lời thoại:**
"Đồ án đã chạy DAG thật, bằng lệnh airflow dags test, trên thư mục thử /co_airflow_test. Đường thành công: khoảng 8 phút, cả 8 task thành công, bước 9 chuyển done. Kết quả mô hình giống hệt bản đã có.
Rồi thử đường lỗi: để overwrite là false. prepare_upload và check_raw qua. Tới clean thì dừng mã 2 vì bảng đã làm sạch đã có. Các task sau thành upstream_failed. record_dag_failure chạy, bước 9 thành failed. Dữ liệu đã làm sạch không bị đụng.
Sau đó, khi người dùng đồng ý và đã sao lưu, DAG chạy thật trên /co: 8 task thành công, trang Pipeline của dashboard hiện đủ 9 trên 9 bước. Điều chưa làm: chưa chạy DAG qua giao diện Airflow ở cổng 8080."

**Hoạt hình:**
- Chuỗi DAG từ cảnh 4 nằm ngang. Lần 1, nhãn `co_root=/co_airflow_test · overwrite=true`: các task chuyển vàng (đang chạy) rồi xanh lần lượt từ trái sang phải; đồng hồ góc đếm tới "≈ 8 phút"; `record_dag_success` xanh, `record_dag_failure` xám (bỏ qua). Ô "Bước 9: done".
- Lần 2, nhãn `overwrite=false`: `prepare_upload` xanh, `check_raw` xanh, `clean` đỏ với biển "mã 2: aqi_clean đã có". Các task sau chuyển cam theo hiệu ứng domino, nhãn `upstream_failed`. `record_dag_success` xám, `record_dag_failure` sáng đỏ chạy. Ô "Bước 9: failed". Bên dưới, biểu tượng thư mục `aqi_clean` có ổ khóa, nhãn "không bị đụng".
- Lần 3: biểu tượng sao lưu (`/co/output`, `/co/processed` → bản sao), rồi chuỗi chạy trên `/co`, tất cả xanh. Ảnh chụp trang Pipeline của dashboard, bộ đếm `9/9`.
- Góc cuối: biểu tượng trình duyệt `:8080` có chú thích "chưa chạy qua giao diện".

**Số trên màn hình:** `airflow dags test hcmc_co_pipeline 2026-10-14` · `≈ 8 phút · 8/8 task thành công` · `clean: mã 2 → upstream_failed → Bước 9 failed` · `Pipeline 9/9` · `:8080 chưa dùng`.

---

## Cảnh 6 — Vì sao DAG không có bước 8 (4:10–4:30)

**Lời thoại:**
"Bạn có thể hỏi: sao DAG không có bước 8, phần Kafka? Vì bước 8 là luồng chạy thường trực, không có điểm kết thúc; Airflow hợp với việc có đầu có cuối. Bước 8 được kiểm riêng bằng script 08_check_stream.py. Video cuối sẽ ghép tất cả lại: đồ án CO TP.HCM chạy từ đầu đến cuối."

**Hoạt hình:**
- Chuỗi DAG có một khoảng trống nhãn "bước 8?". Bên cạnh, băng chuyền Kafka chạy vô tận ra khỏi khung, không vừa vào một ô có điểm đầu điểm cuối.
- Một hộp nhỏ `08_check_stream.py` đứng riêng, nối nét đứt tới băng chuyền.
- Camera lùi ra, chuỗi DAG và băng chuyền thu nhỏ vào sơ đồ toàn cảnh, chuyển sang video 8.

**Số trên màn hình:** `08_check_stream.py`.

---

## Điểm cần duyệt

- Cảnh 2 nói "PythonOperator" như một ví dụ khái niệm theo [TĐ]; DAG của đồ án chỉ dùng `BashOperator`.
- Cảnh 5 nói "8 task thành công" vì đường thành công chạy 7 task chính và `record_dag_success`; `record_dag_failure` bị bỏ qua. Con số khớp [TĐ] và [HO].
