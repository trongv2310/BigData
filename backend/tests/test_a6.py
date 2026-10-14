"""Bước A6: DAG Airflow `hcmc_co_pipeline` (infra/airflow_dags/), tác vụ ghi bước 9 (pipeline/09_record_dag.py), thư mục gốc HDFS đổi được bằng CO_ROOT.
Test DAG cần Airflow (bản cài trong WSL); thiếu thì bỏ qua. Chạy: cd backend && python3 -m unittest discover tests"""
import importlib
import os
import subprocess
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
DAG_FILE = ROOT / "infra" / "airflow_dags" / "hcmc_co_pipeline.py"
sys.path.insert(0, str(ROOT / "pipeline"))

try:
    import airflow  # noqa: F401
    from airflow.models import DagBag
    HAVE_AIRFLOW = True
except ImportError:
    HAVE_AIRFLOW = False

ORDER = ["prepare_upload", "check_raw", "clean", "sql_analysis", "cluster_days", "cluster_stability", "train_model"]


def load_dag():
    """Nạp riêng tệp DAG (không qua ~/airflow/dags), HCMC_CO_REPO trỏ về repo này."""
    with mock.patch.dict(os.environ, {"HCMC_CO_REPO": str(ROOT)}):
        bag = DagBag(dag_folder=str(DAG_FILE))
    return bag


def render(task, params, **ctx):
    context = {"params": params, "dag": SimpleNamespace(dag_id="hcmc_co_pipeline"), "run_id": ctx.get("run_id", "manual__t"),
               "dag_run": SimpleNamespace(run_type="manual", start_date="2026-10-14T00:00:00+00:00")}
    return task.get_template_env().from_string(task.bash_command).render(**context), \
        {k: task.get_template_env().from_string(v).render(**context) for k, v in (task.env or {}).items()}


@unittest.skipUnless(HAVE_AIRFLOW, "không có Airflow")
class DagTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bag = load_dag()
        cls.dag = cls.bag.dags.get("hcmc_co_pipeline")

    def test_loads_without_errors(self):
        self.assertEqual(self.bag.import_errors, {})
        self.assertIsNotNone(self.dag)

    def test_paused_manual_only_single_run(self):
        self.assertTrue(self.dag.is_paused_upon_creation)
        self.assertEqual(type(self.dag.timetable).__name__, "NullTimetable")  # schedule=None: không tự chạy
        self.assertEqual(self.dag.max_active_runs, 1)
        self.assertFalse(self.dag.catchup)

    def test_task_order_is_a_chain_then_recorders(self):
        self.assertEqual(set(self.dag.task_ids), set(ORDER) | {"record_dag_success", "record_dag_failure"})
        for a, b in zip(ORDER, ORDER[1:]):
            self.assertEqual(self.dag.get_task(a).downstream_task_ids, {b}, a)
        last = self.dag.get_task(ORDER[-1])
        self.assertEqual(last.downstream_task_ids, {"record_dag_success", "record_dag_failure"})
        self.assertEqual(self.dag.get_task(ORDER[0]).upstream_task_ids, set())

    def test_trigger_rules_and_no_retries(self):
        self.assertEqual(self.dag.get_task("record_dag_success").trigger_rule.value, "all_success")
        self.assertEqual(self.dag.get_task("record_dag_failure").trigger_rule.value, "one_failed")
        for t in self.dag.tasks:
            self.assertEqual(t.retries, 0, t.task_id)

    def test_default_params(self):
        p = self.dag.params.dump()
        self.assertEqual(p["co_root"], "/co")
        self.assertIs(p["overwrite"], False)
        self.assertTrue(p["local_csv"].endswith(".csv"))

    def test_overwrite_flag_only_when_param_true(self):
        defaults = self.dag.params.dump()
        for tid in ORDER[2:]:  # clean, sql, cluster, stability, train
            off, _ = render(self.dag.get_task(tid), {**defaults, "overwrite": False})
            on, _ = render(self.dag.get_task(tid), {**defaults, "overwrite": True})
            self.assertNotIn("--overwrite", off, tid)
            self.assertIn("--overwrite", on, tid)
        for tid in ("prepare_upload", "check_raw"):
            on, _ = render(self.dag.get_task(tid), {**defaults, "overwrite": True})
            self.assertNotIn("--overwrite", on, tid)

    def test_co_root_passed_to_every_task(self):
        defaults = self.dag.params.dump()
        for t in self.dag.tasks:
            _, env = render(t, {**defaults, "co_root": "/co_airflow_test"})
            self.assertEqual(env, {"CO_ROOT": "/co_airflow_test"}, t.task_id)
            self.assertTrue(t.append_env, t.task_id)  # giữ PATH để tìm spark-submit

    def test_commands_run_pipeline_scripts_from_repo(self):
        defaults = self.dag.params.dump()
        expect = {"prepare_upload": "pipeline/01_prepare_upload.py", "check_raw": "pipeline/02_check_raw.py", "clean": "pipeline/04_clean.py", "sql_analysis": "pipeline/05_sql_analysis.py",
                  "cluster_days": "pipeline/06a_cluster_days.py", "cluster_stability": "pipeline/06a_cluster_stability.py", "train_model": "pipeline/06b_train.py"}
        for tid, script in expect.items():
            cmd, _ = render(self.dag.get_task(tid), defaults)
            self.assertTrue(cmd.startswith(f"cd {ROOT} && "), tid)
            self.assertIn(script, cmd)
            self.assertTrue((ROOT / script).exists(), script)
            self.assertEqual("spark-submit" in cmd, tid != "prepare_upload", tid)

    def test_recorder_commands_render(self):
        defaults = self.dag.params.dump()
        ok, _ = render(self.dag.get_task("record_dag_success"), {**defaults, "overwrite": True}, run_id="manual__x")
        bad, _ = render(self.dag.get_task("record_dag_failure"), defaults, run_id="manual__x")
        self.assertIn("--state done", ok)
        self.assertIn("--state failed", bad)
        for cmd in (ok, bad):
            self.assertIn("--dag-id hcmc_co_pipeline", cmd)
            self.assertIn("--run-id 'manual__x'", cmd)
            self.assertIn(f"--tasks {len(ORDER)}", cmd)
            self.assertNotIn("{{", cmd)
            self.assertNotIn("{%", cmd)
        self.assertIn("--overwrite-flag", ok)
        self.assertNotIn("--overwrite-flag", bad)

    def test_dag_and_sample_ids_differ(self):
        """DAG mẫu cũ (số liệu cố định) có cùng dag_id gốc nên airflow.sh cài bản đổi id; tệp DAG thật không được dùng id mẫu."""
        self.assertNotIn("hcmc_co_pipeline_sample", DAG_FILE.read_text(encoding="utf-8").replace("`hcmc_co_pipeline_sample`", ""))


class RecordTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rec = importlib.import_module("09_record_dag")

    def test_done_entry(self):
        summary, detail = self.rec.build_entry("done", "hcmc_co_pipeline", "manual__1", "manual", "2026-10-14T01:00:00+00:00", 7, False)
        labels = {s["label"]: s for s in summary}
        self.assertEqual(labels["DAG"]["value"], "hcmc_co_pipeline")
        self.assertEqual(labels["Run ID"]["value"], "manual__1")
        self.assertEqual(labels["Số tác vụ chạy spark-submit/python"]["value"], 7)
        self.assertEqual(labels["Cho phép ghi đè đầu ra có sẵn"]["value"], "không")
        self.assertIn("đã chạy xong cả 7 tác vụ", detail)
        self.assertIn("Không bật ghi đè", detail)

    def test_run_type_enum_text_is_normalised(self):
        summary, _ = self.rec.build_entry("done", "d", "r", "DagRunType.MANUAL", "x", 7, False)
        self.assertEqual({s["label"]: s["value"] for s in summary}["Loại chạy"], "manual")

    def test_failed_entry_does_not_claim_success(self):
        _, detail = self.rec.build_entry("failed", "hcmc_co_pipeline", "manual__2", "manual", "x", 7, True)
        self.assertIn("thất bại", detail)
        self.assertNotIn("đã chạy xong", detail)
        _, d2 = self.rec.build_entry("failed", "hcmc_co_pipeline", "manual__2", "manual", "x", 7, False)
        self.assertIn("giữ trạng thái của lần chạy trước", d2)  # không bật ghi đè: bước bị từ chối không được coi là kết quả của run này

    def test_overwrite_text(self):
        summary, detail = self.rec.build_entry("done", "d", "r", "manual", "x", 7, True)
        self.assertEqual({s["label"]: s["value"] for s in summary}["Cho phép ghi đè đầu ra có sẵn"], "có")
        self.assertIn("Đã bật ghi đè", detail)


class CoRootTests(unittest.TestCase):
    """CO_ROOT quyết định mọi đường dẫn HDFS của pipeline; mặc định /co phải giữ nguyên (kết quả đã giao nằm ở đó)."""

    def run_common(self, env_value):
        env = {k: v for k, v in os.environ.items() if k != "CO_ROOT"}
        if env_value is not None:
            env["CO_ROOT"] = env_value
        code = "import common as c; print(c.CO_ROOT, c.RAW_DIR, c.PROCESSED_DIR, c.OUTPUT_DIR, c.RAW_FILE, c.CLEAN_DIR, c.SUMMARY_PATH)"
        out = subprocess.run([sys.executable, "-c", code], cwd=ROOT / "pipeline", env=env, capture_output=True, text=True, timeout=60)
        self.assertEqual(out.returncode, 0, out.stderr[-500:])
        return out.stdout.split()

    def test_default_is_co(self):
        r = self.run_common(None)
        self.assertEqual(r[:4], ["/co", "/co/raw", "/co/processed", "/co/output"])
        self.assertTrue(all(p.startswith("/co/") for p in r[4:]))

    def test_override_and_normalise(self):
        for value in ("/co_airflow_test", "co_airflow_test", "/co_airflow_test/"):
            r = self.run_common(value)
            self.assertEqual(r[:4], ["/co_airflow_test", "/co_airflow_test/raw", "/co_airflow_test/processed", "/co_airflow_test/output"], value)
            self.assertTrue(all(p.startswith("/co_airflow_test/") for p in r[4:]), value)

    def test_no_hardcoded_co_paths_left_in_runtime_strings(self):
        import re
        for name in ("02_check_raw.py", "06b_train.py"):
            text = (ROOT / "pipeline" / name).read_text(encoding="utf-8")
            code_lines = [l for l in text.splitlines() if re.search(r'["\']/co/', l) and not l.lstrip().startswith("#")]
            self.assertEqual(code_lines, [], name)


if __name__ == "__main__":
    unittest.main()
