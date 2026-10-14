"""Hợp đồng 0.6.0: dịch vụ TẮT CHỦ ĐỘNG theo cấu hình (YARN mặc định, Airflow khi WITH_AIRFLOW=0) báo state "disabled", không phải "down"."""
import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import config, services  # noqa: E402


def fake_http(answers: dict):
    """answers: {phần cuối URL: (ok, body)}; URL không khớp coi như không phản hồi."""
    async def fake(url):
        for suffix, res in answers.items():
            if url.endswith(suffix):
                return res
        return False, None
    return fake


class DisabledServices(unittest.TestCase):
    def setUp(self):
        self.old = (services._http_json, config.YARN_ENABLED, config.AIRFLOW_ENABLED)

    def tearDown(self):
        services._http_json, config.YARN_ENABLED, config.AIRFLOW_ENABLED = self.old

    def yarn(self, enabled, answers):
        config.YARN_ENABLED = enabled
        services._http_json = fake_http(answers)
        return asyncio.run(services._yarn())

    def airflow(self, enabled, answers):
        config.AIRFLOW_ENABLED = enabled
        services._http_json = fake_http(answers)
        return asyncio.run(services._airflow())

    def test_yarn_off_by_config_and_silent_is_disabled(self):
        rm, nm = self.yarn(False, {})
        self.assertEqual((rm["state"], nm["state"]), ("disabled", "disabled"))
        self.assertIn("WITH_YARN=1", rm["detail"])
        self.assertEqual(rm["url"], config.YARN_HTTP)

    def test_yarn_on_by_config_and_silent_is_down(self):
        rm, nm = self.yarn(True, {})
        self.assertEqual((rm["state"], nm["state"]), ("down", "unknown"))

    def test_yarn_running_is_up_even_if_config_says_off(self):
        rm, nm = self.yarn(False, {"/ws/v1/cluster/info": (True, {}), "/ws/v1/cluster/nodes": (True, {"nodes": {"node": [{"state": "RUNNING"}]}})})
        self.assertEqual((rm["state"], nm["state"]), ("up", "up"))

    def test_airflow_off_by_config_is_disabled(self):
        s = self.airflow(False, {})
        self.assertEqual(s["state"], "disabled")
        self.assertIn("WITH_AIRFLOW=0", s["detail"])

    def test_airflow_on_by_config_and_silent_is_down(self):
        self.assertEqual(self.airflow(True, {})["state"], "down")

    def test_airflow_running_is_up_even_if_config_says_off(self):
        s = self.airflow(False, {"/api/v2/monitor/health": (True, {"scheduler": {"status": "healthy"}})})
        self.assertEqual(s["state"], "up")

    def test_defaults_follow_start_all(self):
        import importlib, os
        saved = {k: os.environ.pop(k, None) for k in ("YARN_ENABLED", "AIRFLOW_ENABLED")}
        try:
            fresh = importlib.reload(config)
            self.assertFalse(fresh.YARN_ENABLED)
            self.assertTrue(fresh.AIRFLOW_ENABLED)
        finally:
            for k, v in saved.items():
                if v is not None:
                    os.environ[k] = v
            importlib.reload(config)


if __name__ == "__main__":
    unittest.main()
