"""Bước 9 (Airflow DAG): ghi kết quả một lần chạy DAG `hcmc_co_pipeline` vào step_summary.json (bước airflow-dag). Do tác vụ cuối của DAG gọi.

  python3 pipeline/09_record_dag.py --state done|failed --dag-id hcmc_co_pipeline --run-id <run_id> --run-type <manual|scheduled|...> --started <thời điểm> --tasks 7 [--overwrite-flag]

`done` chỉ được ghi khi MỌI tác vụ trước đó thành công (trigger_rule all_success trong DAG); `failed` khi có tác vụ thất bại (trigger_rule one_failed). Trạng thái và số liệu của từng bước 1 đến 7
do chính script của bước đó ghi (kể cả `failed` kèm lý do); script này chỉ ghi bước 9 và không nêu tên tác vụ lỗi vì Airflow 3 không cho tác vụ đọc cơ sở dữ liệu của nó (xem log của run).
Ghi vào CO_ROOT/output/step_summary.json, cùng thư mục các bước kia (mặc định /co, nên backend thấy trên /api/pipeline/steps khi chạy ở /co).
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as c  # noqa: E402


def build_entry(state: str, dag_id: str, run_id: str, run_type: str, started: str, tasks: int, overwrite: bool) -> tuple[list[dict], str]:
    """(summary, detail) của bước airflow-dag; hàm thuần túy để kiểm được."""
    run_type = run_type.removeprefix("DagRunType.").lower()  # Airflow 3 render enum thành "DagRunType.MANUAL"
    summary = [c.item("DAG", dag_id, None), c.item("Run ID", run_id, None), c.item("Loại chạy", run_type, None), c.item("Bắt đầu lúc (theo Airflow)", started, None),
               c.item("Số tác vụ chạy spark-submit/python", tasks, "tác vụ"), c.item("Thư mục gốc HDFS", c.CO_ROOT, None), c.item("Cho phép ghi đè đầu ra có sẵn", "có" if overwrite else "không", None)]
    if state == "done":
        detail = (f"Run {run_id} của DAG {dag_id} đã chạy xong cả {tasks} tác vụ nối tiếp nhau bằng spark-submit (chuẩn bị/upload, kiểm dữ liệu gốc, làm sạch, Spark SQL, phân cụm ngày, độ ổn định, huấn luyện mô hình); "
                  f"trạng thái và số liệu của từng bước do chính script ghi trong {c.SUMMARY_PATH}. "
                  + ("Đã bật ghi đè đầu ra có sẵn trong run này." if overwrite else "Không bật ghi đè nên các bước chỉ chạy được khi đầu ra chưa tồn tại."))
    else:
        detail = (f"Run {run_id} của DAG {dag_id} có tác vụ thất bại (xem log của run trong Airflow). Bước nào lỗi giữa chừng tự ghi `failed` kèm lý do trong {c.SUMMARY_PATH}; "
                  + ("" if overwrite else "run này không bật ghi đè nên nếu đầu ra đã tồn tại thì tác vụ tương ứng dừng với mã 2 và ghi lý do ở log, CHƯA ghi gì vào bước đó (nó giữ trạng thái của lần chạy trước). ")
                  + "Đừng đọc các bước còn `done` là kết quả của run này.")
    return summary, detail


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--state", required=True, choices=["done", "failed"])
    ap.add_argument("--dag-id", required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--run-type", default="không rõ")
    ap.add_argument("--started", default="không rõ")
    ap.add_argument("--tasks", type=int, required=True)
    ap.add_argument("--overwrite-flag", action="store_true")
    a = ap.parse_args()
    summary, detail = build_entry(a.state, a.dag_id, a.run_id, a.run_type, a.started, a.tasks, a.overwrite_flag)
    c.update_step("airflow-dag", a.state, summary, detail)
    print(f"[09_record_dag] bước airflow-dag = {a.state} (run {a.run_id}, {c.SUMMARY_PATH})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
