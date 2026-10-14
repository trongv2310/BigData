"""Bước A3b: trạng thái partial, /api/analysis/*, gộp summary nhiều phần, độ ổn định K-Means (phần Python thuần).
Chạy: cd backend && python3 -m unittest discover tests"""
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(ROOT / "pipeline"))

from fastapi.testclient import TestClient

from app import config, pipeline_steps
from app.main import create_app
from helpers import FakeNameNode, schema_errors

MOCK = ROOT / "mock"


def load(name):
    return json.loads((MOCK / name).read_text(encoding="utf-8"))


class Ari(unittest.TestCase):
    def test_known_values(self):
        import stability as st
        self.assertEqual(st.adjusted_rand_index([0, 0, 1, 1], [5, 5, 9, 9]), 1.0)          # cùng cách chia, khác cách đánh số
        self.assertAlmostEqual(st.adjusted_rand_index([0, 0, 1, 1], [0, 1, 0, 1]), -0.5)   # giá trị kinh điển
        self.assertAlmostEqual(st.adjusted_rand_index([0, 0, 0, 1, 1, 1], [0, 0, 1, 1, 2, 2]), 0.2424, places=3)
        self.assertEqual(st.adjusted_rand_index([1, 1, 1], [2, 2, 2]), 1.0)                # một cụm duy nhất ở cả hai: không chia được gì, coi là trùng
        with self.assertRaises(ValueError):
            st.adjusted_rand_index([0, 1], [0])

    def test_best_k_follows_the_06a_criterion_including_ties(self):
        import stability as st
        self.assertEqual(st.best_k([{"k": 2, "silhouette": 0.3}, {"k": 3, "silhouette": 0.5}, {"k": 4, "silhouette": 0.4}]), 3)
        self.assertEqual(st.best_k([{"k": 2, "silhouette": 0.4}, {"k": 3, "silhouette": 0.4}]), 2)  # hòa thì k nhỏ

    def test_summarize_reports_the_outlier_seed(self):
        import stability as st
        good, bad = [0] * 5 + [1] * 5, [0, 1] * 5
        per_seed = {42: {2: {"silhouette": 0.44, "sizes": [5, 5], "labels": good}}, 1: {2: {"silhouette": 0.44, "sizes": [5, 5], "labels": [1] * 5 + [0] * 5}},
                    21: {2: {"silhouette": 0.36, "sizes": [5, 5], "labels": bad}}}
        s = st.summarize(per_seed)["2"]
        self.assertEqual((s["ariVsBaseSeed"]["1"], s["silhouetteMin"], s["silhouetteMax"]), (1.0, 0.36, 0.44))
        self.assertLess(s["ariVsBaseSeed"]["21"], 0.1)
        self.assertLess(s["ariPairwiseMin"], 0.1)


class MergeItems(unittest.TestCase):
    def test_new_items_replace_same_label_in_place_and_others_are_kept(self):
        import common as c
        old = [c.item("A", 1), c.item("B", 2), c.item("C", 3)]
        new = [c.item("B", 20), c.item("D", 4)]
        self.assertEqual([(i["label"], i["value"]) for i in c.merge_items(old, new)], [("A", 1), ("B", 20), ("C", 3), ("D", 4)])
        self.assertEqual(c.merge_items(old, []), old)                                       # không có số mới thì giữ nguyên số cũ

    def test_partial_is_a_valid_state_for_scripts(self):
        import common as c
        self.assertIn("partial", c.STATES)


class PartialState(unittest.TestCase):
    def test_partial_is_kept_and_not_turned_into_failed(self):
        doc = {"steps": {"cluster-and-model": {"state": "partial", "updatedAt": "2026-10-14T15:00:00+07:00",
                                               "summary": [{"label": "Số cụm chọn (k)", "value": 2, "unit": "cụm"}], "detail": "Mới xong 6a; 6b chưa chạy."}}}
        b = pipeline_steps.build(doc)
        self.assertEqual(schema_errors("PipelineSteps", b), [])
        step = next(s for s in b["steps"] if s["id"] == "cluster-and-model")
        self.assertEqual((step["state"], step["summary"][0]["value"], step["detail"]), ("partial", 2, "Mới xong 6a; 6b chưa chạy."))


class AnalysisRoutes(unittest.TestCase):
    """Dùng chính kết quả thật trong mock/ (chép từ HDFS) làm nội dung tệp trên NameNode giả."""
    def setUp(self):
        self._old = (config.NAMENODE_HTTP,)
        self.addCleanup(lambda: setattr(config, "NAMENODE_HTTP", self._old[0]))
        self.client = TestClient(create_app())
        self.addCleanup(self.client.close)
        full = load("analysis-day-clusters.json")
        self.stability = full["stability"]
        self.main_file = {k: v for k, v in full.items() if k != "stability"}

    def serve(self, files):
        nn = FakeNameNode(files={k: json.dumps(v) if not isinstance(v, str) else v for k, v in files.items()})
        self.addCleanup(nn.close)
        config.NAMENODE_HTTP = nn.url

    def get(self, url, status=200, schema=None):
        r = self.client.get(url)
        self.assertEqual(r.status_code, status, r.text)
        name = schema or ("Error" if status != 200 else None)
        if name:
            self.assertEqual(schema_errors(name, r.json()), [], r.text)
        return r.json()

    def test_sql_analysis_is_returned_verbatim(self):
        sql = load("analysis-sql.json")
        self.serve({config.ANALYSIS_SQL_PATH: sql})
        self.assertEqual(self.get("/api/analysis/sql", schema="SqlAnalysis"), sql)

    def test_day_clusters_with_and_without_stability(self):
        self.serve({config.ANALYSIS_CLUSTERS_PATH: self.main_file, config.ANALYSIS_CLUSTERS_STABILITY_PATH: self.stability})
        full = self.get("/api/analysis/day-clusters", schema="DayClusters")
        self.assertEqual(full, load("analysis-day-clusters.json"))
        self.serve({config.ANALYSIS_CLUSTERS_PATH: self.main_file})
        none = self.get("/api/analysis/day-clusters", schema="DayClusters")
        self.assertIsNone(none["stability"])                                                  # chưa chạy độ ổn định: null, không bịa
        self.assertEqual({k: v for k, v in none.items() if k != "stability"}, self.main_file)

    def test_missing_results_are_404_not_ready(self):
        self.serve({})
        for url in ("/api/analysis/sql", "/api/analysis/day-clusters"):
            self.assertEqual(self.get(url, 404)["error"]["code"], "ANALYSIS_NOT_READY")
        self.serve({config.ANALYSIS_CLUSTERS_STABILITY_PATH: self.stability})                 # chỉ có độ ổn định, thiếu kết quả chính
        self.assertEqual(self.get("/api/analysis/day-clusters", 404)["error"]["code"], "ANALYSIS_NOT_READY")

    def test_hdfs_down_and_corrupt_files_are_503(self):
        config.NAMENODE_HTTP = "http://127.0.0.1:1"
        for url in ("/api/analysis/sql", "/api/analysis/day-clusters"):
            self.assertEqual(self.get(url, 503)["error"]["code"], "ANALYSIS_UNAVAILABLE")
        self.serve({config.ANALYSIS_SQL_PATH: "{hỏng", config.ANALYSIS_CLUSTERS_PATH: "[]"})
        self.assertEqual(self.get("/api/analysis/sql", 503)["error"]["code"], "ANALYSIS_UNAVAILABLE")
        self.assertEqual(self.get("/api/analysis/day-clusters", 503)["error"]["code"], "ANALYSIS_UNAVAILABLE")  # JSON hợp lệ nhưng không phải đối tượng
        self.serve({config.ANALYSIS_CLUSTERS_PATH: self.main_file, config.ANALYSIS_CLUSTERS_STABILITY_PATH: "{hỏng"})
        self.assertEqual(self.get("/api/analysis/day-clusters", 503)["error"]["code"], "ANALYSIS_UNAVAILABLE")  # tệp phụ hỏng: không giả vờ là "chưa chạy"


if __name__ == "__main__":
    unittest.main()
