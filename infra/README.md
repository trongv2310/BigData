# infra/

Môi trường hiện có của dự án (không do repo này cài):

| Thứ | Vị trí / địa chỉ |
|---|---|
| WSL Ubuntu | đĩa ảo `WSL_Ubuntu/ext4.vhdx` |
| Hadoop 3.3.6 | `/home/trong/hadoop`, HDFS `hdfs://localhost:9000`, NameNode UI `:9870`, DataNode `:9864`, YARN RM `:8088` |
| Kafka | `/home/trong/kafka`, `localhost:9092`, topic `hcmc_co_stream` (`start-kafka.sh`, `stop-kafka.sh`) |
| Spark | PySpark `local[*]` (`spark_streaming_consumer.py`) |
| Airflow 3.3.2 | hai DAG, đều tạm dừng: `hcmc_co_pipeline` (THẬT, bước 9: `infra/airflow_dags/hcmc_co_pipeline.py`, nối pipeline bằng spark-submit) và `hcmc_co_pipeline_sample` (DAG mẫu cũ `test_airflow_dag.py`: số liệu cố định, KHÔNG liên quan tới Kafka/Spark thật), UI `:8080`; bật bằng `bash infra/airflow.sh start` |
| HDFS | `/stream_input` (file JSON của luồng), `/raw/Dataset.csv` |

Quy ước: các script pipeline cũ nằm ở thư mục cha `BigData/`. Repo này **sao chép** khi cần, không di chuyển, để đồ án đang nộp không bị hỏng.

## Hai luồng song song qua Kafka (từ 14/10/2026, quyết định D5, Bước A1)

```
OpenWeather Air Pollution + Current Weather ─▶ infra/live_producer_owm.py ──┐                       ┌─▶ backend (STREAM_SOURCE=kafka) ─▶ /ws/stream, /api/aqi/latest?source=
Bộ Kaggle (lịch sử, MÔ PHỎNG) ─▶ infra/replay_kafka_producer.py ───────────┴─▶ Kafka hcmc_co_stream ─┤
                                                                                                       └─▶ infra/spark_kafka_consumer.py ─▶ HDFS /live/co_stream (Parquet)
```

- Chạy: `export OWM_API_KEY=...` (chỉ trong shell WSL, không ghi vào repo hay log; ví dụ `export OWM_API_KEY=$(tr -d '\r\n' < /mnt/f/HK1_Nam04/BigData/own_key.txt)`), rồi `bash infra/start_all.sh` hoặc `bash infra/live_stack.sh start`. Thiếu `OWM_API_KEY` thì script báo rõ và bỏ qua producer OpenWeather (không có luồng dữ liệu thật), phần còn lại vẫn chạy. `WITH_REPLAY=0` tắt luồng phát lại; `WITH_OPEN_METEO=1` bật thêm producer Open-Meteo cũ.
- **Luồng DỮ LIỆU THẬT** (`source=live-openweather`): `live_producer_owm.py` gọi `air_pollution` (CO và 7 chất khí, µg/m³) và `weather` (`units=metric`) của chính OpenWeather tại (10.8231, 106.6297), đúng tọa độ của `scripts/fetch_owm_raw.py`. Air Pollution API không có nhiệt độ và gió nên hai số đó lấy từ Current Weather (gió đổi từ m/s sang km/h, nhân 3,6); thiếu một trong hai phản hồi thì bỏ qua lần đó, không điền số. Lưu ý nguồn thời tiết: các cột nhiệt độ, độ ẩm, gió trong bộ Kaggle có thể không đến từ Current Weather của OpenWeather (không có tài liệu nói rõ), nên "cùng nguồn gốc" chỉ chắc chắn với CO và các chất khí.
- **Hạn mức gói miễn phí** (openweathermap.org/price, đọc ngày 14/10/2026): 60 lời gọi/phút và 1.000.000 lời gọi/tháng, gồm cả Air Pollution và Current Weather. Mỗi lần hỏi tốn 2 lời gọi; mặc định hỏi mỗi **300 giây** = 576 lời gọi/ngày (khoảng 17 300/tháng, 1,7% hạn mức). Gặp 429 thì chờ ít nhất 120 giây, 401 thì chờ 600 giây, lỗi khác thì chờ lùi dần tới 15 phút; không thoát và không bịa số.
- **Nhịp cập nhật thật của nguồn (đo 14/10/2026, xem HANDOFF)**: tài liệu OpenWeather không nói. `dt` trả về KHÔNG đơn điệu và không phải thời điểm quan sát đáng tin (có lúc bằng đúng "bây giờ", có lúc cũ hơn lần hỏi trước, trong khi CO và các chất khác y hệt). Vì vậy producer chỉ gửi khi `dt` mới hơn `dt` đã gửi VÀ bộ số liệu khí khác bộ đã gửi, và ghi lý do mỗi lần bỏ qua. Khởi động lại thì đọc 200 tin cuối của topic để không gửi lại.
- **Luồng PHÁT LẠI** (`source=replay-dataset`, MÔ PHỎNG): `replay_kafka_producer.py` phát từng dòng giờ của bộ Kaggle từ 01/07/2025 (giai đoạn sau điểm gãy), mặc định cách nhau 10 giây ở tốc độ 1 (10× = 1 giây, 60× = khoảng 6 bản ghi/giây, dưới giới hạn 20 tin/giây của `/ws/stream`), hết dữ liệu thì quay lại đầu. `datetime` là lúc phát, `originalDatetime` là giờ gốc của dòng lịch sử (+07:00). Điều khiển bằng `POST /api/replay/control`: backend ghi `control.json`, producer ghi nhịp tim `status.json` mỗi 2 giây vào `/tmp/hcmc_replay` (`REPLAY_STATE_DIR`); không có nhịp tim mới trong 10 giây thì trạng thái là `stopped`. Luồng thật không đọc các tệp này nên không bao giờ bị ảnh hưởng. Mỗi bản ghi phát lại đi qua Spark và thành một dòng Parquet trong `/live`: tối đa khoảng 2 880 file nhỏ mỗi ngày (một file mỗi lần trigger 30 giây có dữ liệu; suy ra từ trigger, chưa đếm đủ một ngày); tắt bằng `WITH_REPLAY=0` khi không cần.
- **`/live` chứa cả hai luồng**: lọc theo cột `provider` (`openweather` thật, `replay-dataset` phát lại, `open-meteo` tin cũ); không trộn vào một chuỗi. Spark consumer đã thêm các cột `originalDatetime`, `owmDt`, `aqi`, `no`, `no2`, `o3`, `so2`, `pm2_5`, `pm10`, `nh3`; dòng cũ để null, view `live` của `/api/sql` gộp schema (`mergeSchema`) nên đọc được cả file cũ lẫn mới.
- **Open-Meteo** (`infra/live_producer.py`) vẫn còn nhưng KHÔNG bật mặc định (`WITH_OPEN_METEO=1` mới bật). Đây là nguồn KHÁC (mô hình CAMS, CO cao hơn nhiều so với OpenWeather); backend gắn `source=live-open-meteo`, không trộn với `live-openweather` và không dùng để dự đoán. Các phần dưới đây về Open-Meteo là lịch sử của D1 và còn đúng cho nguồn đó.

### Dự đoán CO trên luồng (Bước A5)

```
producer (OpenWeather | phát lại) --[bản ghi + modelInputs]--> Kafka hcmc_co_stream --> Spark: nạp /co/output/model_best, dự đoán --+--> HDFS /live/co_stream (Parquet: co_actual, co_predicted, model_version, model_inputs, feature_note)
                                                                                                                                     +--> Kafka hcmc_co_enriched --> backend (READING_TOPIC) --> /ws/stream, /api/aqi/latest (coPredicted, modelVersion)
```

- **Độ trễ CO các giờ trước (vấn đề của luồng):** mô hình cần CO của giờ t-1, t-2, t-3 và t-24 (cộng thời tiết và chất khí của giờ t-1), mà luồng sống chỉ có tối đa một quan sát mỗi giờ nên tự nó cần 24 giờ mới đủ. Cách xử lý: mỗi producer tự dựng `modelInputs` từ nơi nó có lịch sử (`infra/model_inputs.py`). Phát lại: từ chính bộ dữ liệu theo `originalDatetime`. Dữ liệu thật: mỗi lần gửi (tối đa một lần mỗi giờ) gọi thêm `air_pollution/history` của OpenWeather (1 lời gọi), tìm điểm lịch sử theo giờ có ĐÚNG giá trị của quan sát hiện tại rồi lấy các giờ liền trước; nhờ vậy bản ghi sống đầu tiên đã đủ độ trễ, không cần khởi động nóng. Thiếu lịch sử liên tục thì không có `modelInputs` và bản ghi đi tiếp với `co_predicted` null.
- **Điều đã đo:** giá trị "hiện tại" của Air Pollution API bằng điểm lịch sử của giờ hiện hành (ví dụ cả giờ 13:xx ngày 14/10 đều là 251,31, đúng điểm 13:00 của lịch sử; điểm lịch sử căn đúng :00), còn `dt` của API hiện tại không đáng tin (A1).
- **Xấp xỉ có chủ ý cho dữ liệu thật:** nhiệt độ và gió trong `modelInputs` là của quan sát hiện tại thay vì giờ t-1 (API lịch sử không có thời tiết); ghi ở `feature_note`.
- **Chạy:** `bash infra/start_all.sh` (tạo topic `hcmc_co_enriched`, Spark dùng `--driver-memory 1g`, backend đặt `READING_TOPIC=hcmc_co_enriched`). `ENRICH=0` để backend đọc topic thô (không có `coPredicted`). Spark phải chạy thì `reading` mới đến; đổi phiên bản mô hình thì khởi động lại job Spark. Phát lại mặc định bắt đầu từ đầu tập kiểm tra của mô hình (`--start auto`), nên dự đoán trên đó là ngoài mẫu huấn luyện; bắt đầu từ mốc khác thì phần lớn là trong mẫu.
- **Kiểm:** `spark-submit --driver-memory 1g pipeline/08_check_stream.py` đọc `/live/co_stream`, đối chiếu dự đoán trên luồng với dự đoán offline và ghi bước 8 vào `step_summary.json`.

## Dữ liệu sống Open-Meteo qua Kafka (D1 phương án b, lịch sử; nguồn này nay KHÔNG bật mặc định)

```
Open-Meteo API ──▶ infra/live_producer.py ──▶ Kafka hcmc_co_stream ─┬─▶ backend (STREAM_SOURCE=kafka) ──▶ /ws/stream, /api/aqi/latest ──▶ dashboard
                                                                     └─▶ infra/spark_kafka_consumer.py ──▶ HDFS /live/co_stream (Parquet)
```

- Chạy: `bash infra/live_stack.sh start` (producer + Spark consumer), backend: `STREAM_SOURCE=kafka python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000`. Dừng: `bash infra/live_stack.sh stop`. Xem: `bash infra/live_stack.sh status`.
- **Tần suất thật của nguồn:** Open-Meteo cập nhật CO mỗi 3600 giây và thời tiết mỗi 900 giây (trường `interval` của API), nên một bản ghi mới xuất hiện tối đa 15 phút một lần. Producer hỏi API mỗi 60 giây nhưng chỉ gửi khi thời điểm quan sát đổi. Không có số liệu nào được điền giả khi API thiếu.
- **Nguồn dữ liệu:** CO của Open-Meteo là số của mô hình chất lượng không khí (CAMS), không phải cảm biến ở một trạm; giá trị khác hẳn bộ Kaggle/OpenWeather (Open-Meteo trả 2508 µg/m³ lúc 23:00 ngày 12/10/2026; trung bình bộ Kaggle từ 07/2025 là ~300, bộ này kết thúc ở 03/2026, không có số cùng thời điểm để so), nên `source: live-open-meteo` luôn tách khỏi `replay-dataset` (AGENTS.md mục 5). Ngưỡng cảnh báo (D3, chốt 13/10/2026) theo thang CO của OpenWeather: `WARNING` từ 4400, `DANGER` từ 12400 µg/m³ (`CO_WARNING_UG`, `CO_DANGER_UG`), nên các giá trị Open-Meteo đã thấy (1000 đến 2500) đều là `SAFE`.
- Spark cần connector Kafka: lần đầu `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` tải ~10 MB từ Maven Central vào `~/.ivy2`. Kết quả ghi vào `hdfs://localhost:9000/live/co_stream` (checkpoint `/live/_checkpoints/co_stream`); khởi động lại thì đọc tiếp từ offset đã xử lý.
- Script cũ ở thư mục cha (`sensor_producer_*.py`, `spark_streaming_consumer.py`) không bị sửa và vẫn chạy được với `STREAM_SOURCE=hdfs`.
- **Độ trễ Spark (`lagTotal`, `consumerGroups`)**: Spark đọc Kafka không dùng consumer group, nên backend đọc checkpoint `/live/_checkpoints/co_stream` trên HDFS (chỉ đọc) để lấy offset batch đã commit, rồi trừ cho end offset của Kafka; hiện trong `/api/kafka/topics` với tên `spark:co_stream`. Đã thấy lag = 1 ngay sau khi producer gửi tin mới rồi về 0 khi Spark commit.
- **SQL trên dữ liệu sống**: `/api/sql` có thêm bảng `live` (Parquet `/live/co_stream`), dựng lại mỗi truy vấn; ví dụ `SELECT datetime_vn, co, kafka_offset FROM live ORDER BY kafka_offset`. Phiên SQL của backend cần ~1 GB RAM (xem giới hạn bộ nhớ).
- **Luồng `kafka-produce` và `spark-streaming` trong Sơ đồ luồng**: backend đã có định nghĩa và phát `flow.step`, nhưng mặc định TẮT; chạy backend với `FLOWS_EXTRA=1` để bật (nên bật cùng `STREAM_SOURCE=kafka`). Frontend đã lọc `flow.step` theo `flowId` (PR #11), nên bật không còn làm sáng sai sơ đồ HDFS. Đã xem thật 13/10/2026 06:01: offset 28 sáng `kafka-produce`, `spark-batch-26` sáng `spark-streaming`. Lưu ý: Spark ghi checkpoint và Parquet vào `/live` thì NameNode log cũng sinh `hdfs.op` và vài lần `hdfs-write` thật (bước 1, 2, 3, 7), nên sơ đồ HDFS có thể có run mới dù không chạy `replay_producer.py`; đó là ghi thật của Spark, không phải lỗi.

## Khởi động nguội một lệnh (đã thử 13/10/2026)

- `bash infra/start_all.sh` bật HDFS, Kafka, (YARN nếu `WITH_YARN=1`), Airflow, dữ liệu sống và backend theo đúng thứ tự, kiểm từng bước rồi in bảng dịch vụ; `bash infra/stop_all.sh` dừng ngược lại (`KEEP_INFRA=1` giữ HDFS, Kafka). Frontend chạy riêng trên Windows (lệnh in ở cuối `start_all.sh`, xem `docs/DEMO.md`).
- Đã thử hai vòng: dừng sạch, `wsl --shutdown`, khởi động lại. Kết quả: HDFS `fsck` HEALTHY, 4 DataNode, dữ liệu `/live` nguyên vẹn (43 file Parquet, 43 commit); Kafka giữ topic và toàn bộ tin (offset không mất); Spark tiếp tục từ checkpoint, lag 0; khởi động nguội cả hệ thống bằng `start_all.sh` mất khoảng 1 phút 20 giây.
- Ba lỗi gặp và đã sửa trong lần thử này (chi tiết ở `docs/HANDOFF.md` P18): `airflow.sh stop` để lại 35 tiến trình mồ côi; producer gửi lại quan sát gần nhất mỗi lần khởi động (tạo bản trùng, offset 44 và 45 giống hệt); Vite tự nhảy sang cổng 5174 khiến backend chặn CORS.
- Dữ liệu trùng đã có từ trước khi sửa: offset 0 và 1, 44 và 45 trong topic và Parquet `/live`. Kafka không sửa được tin đã ghi; truy vấn dùng `DISTINCT` hoặc lọc theo `kafka_offset`. Bản producer mới không tạo thêm bản trùng khi khởi động lại.
- Lỗi cú pháp dòng 1 của `~/start-4nodes.sh` và `~/stop-4nodes.sh` ("No such file or directory" trước chữ `#!/bin/bash`) là ký tự BOM đầu file của người dùng; vô hại (script vẫn chạy), không sửa vì là script cũ của bạn.

## Chạy tay các thành phần (đã chạy thật 2026-10-12, trong WSL)

| Thành phần | Lệnh | Ghi chú |
|---|---|---|
| Kafka | `JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PATH=$JAVA_HOME/bin:$PATH /home/trong/kafka/bin/kafka-server-start.sh -daemon /home/trong/kafka/config/server.properties` | Cần Java 17 chỉ cho lệnh Kafka. Dữ liệu Kafka nằm ở `/home/trong/kafka-data/kraft-combined-logs` (`log.dirs`, đã chuyển khỏi `/tmp` ngày 13/10/2026 bằng cách sao chép nguyên thư mục nên giữ cluster id, topic và các tin; bản sao lưu cấu hình cũ: `config/server.properties.bak-20261012`), nên khởi động lại WSL không còn làm mất dữ liệu, nhưng Kafka vẫn phải được khởi động lại bằng lệnh này. Chỉ khi thư mục dữ liệu mất mới phải `kafka-storage.sh format --standalone -t <uuid> -c config/server.properties` rồi tạo lại topic `hcmc_co_stream` (và xóa checkpoint Spark `/live/_checkpoints/co_stream`, vì offset trong đó không còn khớp). |
| YARN | `setsid nohup ~/hadoop/bin/yarn --daemon start resourcemanager` (NodeManager do `start-yarn.sh`) | `start-yarn.sh` trong phiên ngắn làm ResourceManager bị `SIGHUP`. |
| Backend | `ALLOWED_ORIGINS=... nohup python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000 &` | Thêm cổng frontend vào `ALLOWED_ORIGINS` nếu không phải 5173. |
| Producer mô phỏng | `python3 infra/replay_producer.py --count 300 --interval 5` | Ghi `/stream_input/replay_<epoch>_<n>.json`; dọn bằng `hdfs dfs -rm '/stream_input/replay_*.json'`. MÔ PHỎNG, không phải đo thật. |
| Spark Streaming | `cd BigData && spark-submit --driver-memory 640m spark_streaming_consumer.py` | Đọc `/stream_input`, in ra console (không ghi HDFS), UI `:4040`. Mỗi micro-batch một file. |
| Frontend | node của Windows (WSL không có node): `VITE_API_BASE=http://localhost:8000 VITE_STREAM=ws npm run dev` | |

YARN đang TẮT (từ 13/10/2026, theo yêu cầu người dùng để lấy RAM; Spark chạy `local[*]` nên không cần YARN): dashboard báo ResourceManager DOWN và NodeManager UNKNOWN, còn khoảng 2 GB RAM trống. Bật lại bằng hai lệnh `~/hadoop/bin/yarn --daemon start resourcemanager` và `... start nodemanager`.

Bộ nhớ: máy WSL có ~7,7 GB; khi chạy HDFS (5 JVM), YARN, Kafka và Spark Streaming thì chỉ còn ~0,9 GB khả dụng, chưa đủ chỗ cho Airflow. Với YARN tắt thì còn ~2,2 GB; bật thêm Airflow standalone còn ~1,3 GB (đã đo 13/10/2026, backend mới khởi động lại để giải phóng JVM SQL).

### Airflow (bật ngày 13/10/2026)

- `bash infra/airflow.sh start|stop|status` chạy `airflow standalone` (api-server, scheduler, dag-processor, triggerer). Không sửa `~/airflow/airflow.cfg`: tắt ví dụ mẫu bằng biến môi trường `AIRFLOW__CORE__LOAD_EXAMPLES=False`. `airflow.sh start` (hoặc `install`, chỉ cài DAG) liên kết `infra/airflow_dags/hcmc_co_pipeline.py` vào `~/airflow/dags/` và tạo `hcmc_co_pipeline_sample.py` từ `test_airflow_dag.py` của thư mục cha chỉ với dòng `dag_id` đổi (bản gốc không bị đụng). Bản `~/airflow/dags/hcmc_co_pipeline.py` cũ (bản sao của DAG mẫu) chỉ bị thay khi còn giống hệt file gốc; khác thì script từ chối. Đặt `SAMPLE_DAG=<đường dẫn>` nếu file mẫu không nằm ở thư mục cha của repo (worktree Orca).
- Mật khẩu đăng nhập UI do SimpleAuthManager sinh, nằm ở `~/airflow/simple_auth_manager_passwords.json.generated` (không ghi vào repo).
- DAG mẫu cũ (nay `hcmc_co_pipeline_sample`, lịch `0 8 * * *`) đã chạy thử ngày 13/10 bằng `airflow dags test`: 3 task thành công, in "Đã xử lý và nạp thành công 3 bản ghi quan trắc CO"; chỉ chạy theo lịch khi bỏ tạm dừng.
- **DAG thật `hcmc_co_pipeline` (A6, 14/10/2026):** 7 tác vụ nối tiếp `prepare_upload` (bước 1-2) -> `check_raw` (3) -> `clean` (4) -> `sql_analysis` (5) -> `cluster_days` (6a) -> `cluster_stability` (6a bổ sung) -> `train_model` (6b, 7), rồi `record_dag_success` (ghi bước 9 `done`) hoặc `record_dag_failure` (`failed`). Tạm dừng khi tạo, không lịch, `max_active_runs=1`, không thử lại. Tham số: `co_root` (mặc định `/co`), `overwrite` (mặc định false: bước nào đã có đầu ra thì dừng mã 2), `local_csv`. Cần HDFS; mỗi tác vụ Spark `local[*]` 1 đến 2 GB.
  - Chạy thử KHÔNG đụng `/co` (không cần `airflow standalone`, tiết kiệm RAM; khoảng 8 phút):
    `cd ~ && export HCMC_CO_REPO=<đường dẫn repo trong WSL> AIRFLOW__CORE__LOAD_EXAMPLES=False && airflow dags test hcmc_co_pipeline 2026-10-14 --conf '{"co_root":"/co_airflow_test","overwrite":true}'`
  - Chạy qua giao diện `:8080`: bỏ tạm dừng rồi Trigger kèm tham số; với `co_root=/co` phải đặt `overwrite=true` (ghi đè kết quả đã giao, hỏi người dùng trước).
  - Dọn dữ liệu thử (cần người dùng đồng ý): `hdfs dfs -rm -r -skipTrash /co_airflow_test`.
- Backend dò Airflow 3 bằng `/api/v2/monitor/health` (Airflow 3 trả 404 ở `/health`, nên trước đó dashboard báo `down` dù Airflow chạy); nếu không có thì thử `/health` của Airflow 2.
- Lưu ý khi tự viết lệnh khởi động: tiến trình nền khởi động từ một shell WSL thoát ngay có thể bị dọn mất; `airflow.sh start` giữ shell sống tới khi Airflow trả lời.

Docker: chưa thấy trên máy, nên chưa có `docker-compose`. TODO nếu sau này cần.
