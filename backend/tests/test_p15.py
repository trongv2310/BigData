"""P15: dò Airflow đúng đường dẫn (Airflow 3 không có /health ở api-server; dùng /api/v2/monitor/health) và báo trạng thái scheduler."""
import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import config, services  # noqa: E402


def fake_http(responses):
    """responses: {đường dẫn: (ok, body)}; đường dẫn không có trong dict coi như 404 (ok=False)."""
    calls = []

    async def fake(url):
        calls.append(url)
        return responses.get(url.removeprefix(config.AIRFLOW_HTTP), (False, None))

    return fake, calls


class AirflowProbe(unittest.TestCase):
    def setUp(self):
        self.old = services._http_json

    def tearDown(self):
        services._http_json = self.old

    def probe(self, responses):
        services._http_json, calls = fake_http(responses)
        return asyncio.run(services._airflow()), calls

    def test_airflow3_up_reports_scheduler(self):
        s, calls = self.probe({"/api/v2/monitor/health": (True, {"scheduler": {"status": "healthy"}, "metadatabase": {"status": "healthy"}})})
        self.assertEqual((s["state"], s["detail"]), ("up", "DAG hcmc_co_pipeline, scheduler healthy"))
        self.assertEqual(len(calls), 1)  # không cần thử đường dẫn cũ

    def test_airflow2_falls_back_to_health(self):
        s, calls = self.probe({"/health": (True, {"scheduler": {"status": "healthy"}})})
        self.assertEqual(s["state"], "up")
        self.assertEqual(len(calls), 2)

    def test_down_when_neither_answers(self):
        s, _ = self.probe({})
        self.assertEqual((s["state"], s["detail"]), ("down", "DAG hcmc_co_pipeline"))
        self.assertEqual(s["url"], config.AIRFLOW_HTTP)


if __name__ == "__main__":
    unittest.main()
