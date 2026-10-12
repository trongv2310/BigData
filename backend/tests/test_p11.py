"""P11: nguồn reading từ Kafka (STREAM_SOURCE=kafka). Không cần broker thật: thay các hàm đọc của kafka_stream bằng bản giả,
và kiểm phân tích tin bằng bản ghi đúng dạng producer thật (infra/live_producer.py) đã gửi lên Kafka ngày 2026-10-12."""
import asyncio
import json
import unittest
from unittest import mock

from fastapi.testclient import TestClient

from app import config, hub as hubmod, kafka_stream
from app.clients.kafka import KafkaUnreachable
from helpers import schema_errors

# Tin thật của live_producer.py (offset 0 của hcmc_co_stream)
LIVE = {"datetime": "2026-10-12 23:15:00", "station": "Tram_TPHCM_Live_API", "co": 2508.0, "wind_speed_kmh": 1.6,
        "temperature_C": 25.9, "coObservedAt": "2026-10-12T23:00", "weatherObservedAt": "2026-10-12T23:15", "provider": "open-meteo"}


def raw(rec) -> bytes:
    return json.dumps(rec).encode()


class Parse(unittest.TestCase):
    def test_real_message_becomes_valid_reading(self):
        r = kafka_stream._parse(raw(LIVE))
        self.assertEqual(schema_errors("AqiReading", r), [])
        self.assertEqual((r["source"], r["station"], r["co"], r["status"], r["isSimulatedStation"]),
                         ("live-open-meteo", "TP.HCM (Open-Meteo)", 2508.0, "DANGER", True))
        self.assertEqual(r["datetime"], "2026-10-12T23:15:00+07:00")

    def test_bad_messages_are_skipped(self):
        for bad in (None, b"", b"not json", raw({"station": "x"}), raw({"datetime": "2026-10-12 23:00:00", "station": "x", "co": "abc",
                                                                          "wind_speed_kmh": 1, "temperature_C": 1})):
            self.assertIsNone(kafka_stream._parse(bad), bad)


class Hub(unittest.TestCase):
    def setUp(self):
        self.old = (config.STREAM_SOURCE, hubmod.KAFKA_READ_POLL_SECONDS)
        config.STREAM_SOURCE, hubmod.KAFKA_READ_POLL_SECONDS = "kafka", 0.05

    def tearDown(self):
        config.STREAM_SOURCE, hubmod.KAFKA_READ_POLL_SECONDS = self.old

    def run_hub(self, end, reads, seconds=0.6):
        async def scenario():
            h = hubmod.Hub()
            q = asyncio.Queue()
            h._subs.add(q)
            with mock.patch.object(kafka_stream, "end_offsets", end), mock.patch.object(kafka_stream, "read_new", reads):
                task = asyncio.create_task(h._poll_kafka_readings())
                await asyncio.sleep(seconds)
                task.cancel()
            return [q.get_nowait() for _ in range(q.qsize())]
        return asyncio.run(scenario())

    def test_publishes_only_new_messages_and_advances_position(self):
        r1, r2 = kafka_stream._parse(raw(LIVE)), kafka_stream._parse(raw({**LIVE, "datetime": "2026-10-13 00:00:00", "co": 2100.0}))
        calls = []

        async def end():
            return {0: 5}  # tin offset 0-4 có sẵn lúc bắt đầu nghe: không phát

        async def reads(pos):
            calls.append(dict(pos))
            return ([r1, r2], {0: 7}) if len(calls) == 1 else ([], pos)

        got = self.run_hub(end, reads)
        self.assertEqual([t for t, _ in got], ["reading", "reading"])
        self.assertEqual([d["co"] for _, d in got], [2508.0, 2100.0])
        self.assertEqual(calls[0], {0: 5})
        self.assertEqual(calls[1], {0: 7})  # lần sau đọc tiếp từ vị trí mới, không lặp

    def test_reset_mark_when_kafka_goes_down(self):
        state = {"n": 0}

        async def end():
            state["n"] += 1
            return {0: 10 * state["n"]}

        async def reads(pos):
            raise KafkaUnreachable("cổng không mở")

        got = self.run_hub(end, reads)
        self.assertEqual(got, [])
        self.assertGreater(state["n"], 1)  # mỗi lần Kafka tắt thì đặt lại mốc, không phát lại tin cũ


class Latest(unittest.TestCase):
    def test_latest_uses_kafka_then_falls_back_to_dataset(self):
        from app.main import app
        old = config.STREAM_SOURCE
        config.STREAM_SOURCE = "kafka"
        try:
            live = kafka_stream._parse(raw(LIVE))
            with TestClient(app) as c:
                async def ok():
                    return live

                async def down():
                    raise KafkaUnreachable("cổng không mở")

                async def empty():
                    return None

                with mock.patch.object(kafka_stream, "latest_reading", ok):
                    r = c.get("/api/aqi/latest").json()
                self.assertEqual((r["source"], r["co"]), ("live-open-meteo", 2508.0))
                for fn in (down, empty):
                    with mock.patch.object(kafka_stream, "latest_reading", fn):
                        r = c.get("/api/aqi/latest").json()
                    self.assertEqual(r["source"], "replay-dataset")  # nhãn thật: dòng cuối bộ Kaggle, không giả làm dữ liệu sống
                    self.assertEqual(schema_errors("AqiReading", r), [])
        finally:
            config.STREAM_SOURCE = old


class Snapshot(unittest.TestCase):
    def test_snapshot_remeasures_services_every_time(self):
        """Khi không ai nghe thì vòng thăm dò dừng; client mới nối phải nhận trạng thái vừa đo, không phải bản lưu cũ."""
        def svc(detail, state="up"):
            return [{"name": "Spark Streaming", "kind": "spark", "state": state, "url": None, "checkedAt": "2026-10-12T23:00:00+07:00", "detail": detail}]

        results = [svc("app cũ"), svc("app mới")]

        async def check_all():
            return results.pop(0)

        async def scenario():
            h = hubmod.Hub()
            with mock.patch.object(hubmod.services, "check_all", check_all):
                first = await h.snapshot_services()
                second = await h.snapshot_services()
            return first, second

        first, second = asyncio.run(scenario())
        self.assertEqual(first[0]["detail"], "app cũ")
        self.assertEqual(second[0]["detail"], "app mới")  # trước khi sửa: vẫn là "app cũ"


class Config(unittest.TestCase):
    def test_stream_source_is_one_of_the_known_values(self):
        self.assertIn(config.STREAM_SOURCE, ("hdfs", "kafka", "both"))


if __name__ == "__main__":
    unittest.main()
