"""DAG `hcmc_co_pipeline` (bước 9): chạy cả pipeline CO bằng spark-submit, theo thứ tự các bước 2 đến 6 của đề (và bước 7 lưu kết quả):

  prepare_upload (bước 1-2) -> check_raw (3) -> clean (4) -> sql_analysis (5) -> cluster_days (6a) -> cluster_stability (6a bổ sung) -> train_model (6b, 7)
  -> record_dag_success (ghi bước 9 `done`)  /  record_dag_failure (ghi bước 9 `failed` nếu có tác vụ thất bại)

An toàn theo mặc định:
  - DAG TẠM DỪNG khi tạo (is_paused_upon_creation) và không có lịch (schedule=None): chỉ chạy khi người dùng bật/kích hoạt (tiết kiệm RAM, tránh ghi đè ngoài ý muốn);
  - `overwrite` mặc định false: bước nào đã có đầu ra trong `co_root` thì dừng với mã 2 và ghi lý do ở log (đúng quy tắc "ghi đè Parquet có sẵn phải hỏi người dùng"); đặt true chỉ khi chắc chắn;
  - `co_root` mặc định /co. Để thử cả DAG mà KHÔNG đụng kết quả đã giao, kích hoạt với co_root khác, ví dụ
        airflow dags test hcmc_co_pipeline --conf '{"co_root": "/co_airflow_test", "overwrite": true}'
  - max_active_runs = 1, retries = 0 (không tự chạy lại một bước tốn kém hay ghi đè).
Mỗi tác vụ là một lệnh `spark-submit`/`python3` của pipeline/ (không sửa script); trạng thái và số liệu từng bước do chính script ghi vào CO_ROOT/output/step_summary.json.
Cần HDFS đang chạy; Spark chạy `local[*]` ngay trong tác vụ (khoảng 1 đến 2 GB RAM mỗi lần), nên không bật cùng lúc nhiều thứ nặng. DAG mẫu cũ (số liệu cố định) là `hcmc_co_pipeline_sample`.
"""
import os
from datetime import timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

DAG_ID = "hcmc_co_pipeline"
# Gốc repo: biến HCMC_CO_REPO (airflow.sh đặt), nếu không thì suy từ vị trí thật của tệp này (infra/airflow_dags/ -> gốc repo)
REPO = os.environ.get("HCMC_CO_REPO") or str(Path(os.path.realpath(__file__)).parents[2])
DEFAULT_CSV = "/mnt/f/HK1_Nam04/BigData/Data/hcmc_aqi_dataset_after_preprocess_final.csv"
OVERWRITE = "{% if params.overwrite %}--overwrite{% endif %}"

# (task_id, lệnh sau `cd REPO &&`, bộ nhớ Spark): thứ tự chính là thứ tự chạy
STEPS = [
    ("prepare_upload", "python3 pipeline/01_prepare_upload.py --local '{{ params.local_csv }}'"),
    ("check_raw", "spark-submit --driver-memory 1g pipeline/02_check_raw.py"),
    ("clean", f"spark-submit --driver-memory 1g pipeline/04_clean.py {OVERWRITE}"),
    ("sql_analysis", f"spark-submit --driver-memory 1g pipeline/05_sql_analysis.py {OVERWRITE}"),
    ("cluster_days", f"spark-submit --driver-memory 1g pipeline/06a_cluster_days.py {OVERWRITE}"),
    ("cluster_stability", f"spark-submit --driver-memory 1g pipeline/06a_cluster_stability.py {OVERWRITE}"),
    ("train_model", f"spark-submit --driver-memory 2g pipeline/06b_train.py {OVERWRITE}"),
]

RECORD = ("python3 pipeline/09_record_dag.py --state {state} --dag-id {{{{ dag.dag_id }}}} --run-id '{{{{ run_id }}}}' --run-type '{{{{ dag_run.run_type }}}}' "
          "--started '{{{{ dag_run.start_date }}}}' --tasks " + str(len(STEPS)) + " {{% if params.overwrite %}}--overwrite-flag{{% endif %}}")

with DAG(
    dag_id=DAG_ID,
    description="Pipeline CO TP.HCM: HDFS, làm sạch, Spark SQL, phân cụm ngày, huấn luyện mô hình (spark-submit)",
    schedule=None,
    start_date=None,
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    tags=["hcmc-co", "spark", "hdfs"],
    doc_md=__doc__,
    params={
        "co_root": Param("/co", type="string", description="Thư mục gốc trên HDFS của pipeline (mặc định /co). Đổi để thử mà không đụng kết quả đã giao."),
        "overwrite": Param(False, type="boolean", description="Cho phép ghi đè đầu ra có sẵn (Parquet, model_best...). Mặc định false: bước nào đã có đầu ra thì dừng."),
        "local_csv": Param(DEFAULT_CSV, type="string", description="Đường dẫn CSV cục bộ (WSL) upload vào <co_root>/raw/Dataset.csv"),
    },
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=45)},
) as dag:
    env = {"CO_ROOT": "{{ params.co_root }}"}  # append_env: giữ PATH để tìm được spark-submit
    tasks = [BashOperator(task_id=tid, bash_command=f"cd {REPO} && {cmd}", env=env, append_env=True) for tid, cmd in STEPS]
    for a, b in zip(tasks, tasks[1:]):
        a >> b

    ok = BashOperator(task_id="record_dag_success", bash_command=f"cd {REPO} && " + RECORD.format(state="done"), env=env, append_env=True, trigger_rule="all_success")
    bad = BashOperator(task_id="record_dag_failure", bash_command=f"cd {REPO} && " + RECORD.format(state="failed"), env=env, append_env=True, trigger_rule="one_failed")
    tasks[-1] >> [ok, bad]  # chỉ tác vụ cuối nối vào hai tác vụ ghi; một tác vụ trước đó lỗi thì cả chuỗi sau nó thành upstream_failed, và one_failed vẫn tính upstream_failed
