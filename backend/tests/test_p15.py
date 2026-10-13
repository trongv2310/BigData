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


class FakeMsg:
    def __init__(self, value):
        self.value = value


class FakeConsumer:
    """Giả KafkaConsumer: `log` là {partition: [value, ...]}; chỉ hỗ trợ đúng các lời gọi live_producer.last_published dùng."""
    log = {}

    def __init__(self, **kw):
        self.pos = {}

    def partitions_for_topic(self, topic):
        return set(self.log) or None

    def assign(self, parts):
        self.parts = parts

    def end_offsets(self, parts):
        return {tp: len(self.log.get(tp.partition, [])) for tp in parts}

    def seek(self, tp, off):
        self.pos[tp] = off

    def poll(self, timeout_ms=0):
        return {tp: [FakeMsg(v) for v in self.log[tp.partition][off:]] for tp, off in self.pos.items()}

    def close(self):
        pass


def rec(co_at, weather_at):
    return {"coObservedAt": co_at, "weatherObservedAt": weather_at, "co": 1.0}


class ProducerRestart(unittest.TestCase):
    """Khởi động lại producer không được gửi lại quan sát đã có trong topic (lỗi gặp khi khởi động nguội: offset 44 và 45 trùng nhau)."""

    @classmethod
    def setUpClass(cls):
        import importlib.util
        spec = importlib.util.spec_from_file_location("live_producer", Path(__file__).resolve().parents[2] / "infra" / "live_producer.py")
        cls.lp = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.lp)

    def last(self, log):
        FakeConsumer.log = log
        return self.lp.last_published("x:9092", "t", consumer_cls=FakeConsumer)

    def test_returns_newest_message_key(self):
        self.assertEqual(self.last({0: [rec("2026-10-13T09:00", "2026-10-13T09:45"), rec("2026-10-13T10:00", "2026-10-13T10:00")]}),
                         ("2026-10-13T10:00", "2026-10-13T10:00"))

    def test_empty_topic_is_none(self):
        self.assertIsNone(self.last({0: []}))
        self.assertIsNone(self.last({}))

    def test_message_without_observation_times_is_ignored(self):
        self.assertIsNone(self.last({0: [{"co": 1.0}]}))


if __name__ == "__main__":
    unittest.main()
