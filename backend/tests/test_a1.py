"""Bước A1: producer OpenWeather, producer phát lại, nhận biết nguồn, bản ghi mới nhất theo nguồn, /api/replay/*.
Chạy: cd backend && python3 -m unittest discover tests   (cần requests và kafka-python như các test cũ)"""
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(ROOT / "infra"))

from fastapi.testclient import TestClient

import live_producer_owm as owm
import replay_kafka_producer as rp
from app import config, kafka_stream, reading, replay
from app.main import create_app
from helpers import schema_errors

VN = timezone(timedelta(hours=7))

# Hai phản hồi THẬT của OpenWeather gọi lúc 2026-10-14 12:51 (+07) cho (10.8231, 106.6297); chỉ bỏ phần không dùng
AIR = {"coord": {"lon": 106.6257, "lat": 10.822},
       "list": [{"main": {"aqi": 2}, "components": {"co": 251.31, "no": 0.26, "no2": 2.8, "o3": 65.31, "so2": 2.03, "pm2_5": 7.86, "pm10": 9.9, "nh3": 1.16},
                 "dt": 1791956800}]}
WEATHER = {"dt": 1791956697, "main": {"temp": 32.02, "humidity": 59}, "wind": {"speed": 2.18, "deg": 106}, "cod": 200}
KEY = "abcd1234secretkey5678"


class OwmRecord(unittest.TestCase):
    def test_real_response_becomes_valid_live_reading(self):
        rec = owm.to_record(AIR, WEATHER)
        self.assertEqual((rec["datetime"], rec["co"], rec["wind_speed_kmh"], rec["temperature_C"]), ("2026-10-14 12:46:40", 251.31, 7.85, 32.02))
        self.assertEqual((rec["coObservedAt"], rec["weatherObservedAt"], rec["owmDt"]), ("2026-10-14T12:46:40", "2026-10-14T12:44:57", 1791956800))
        self.assertEqual((rec["provider"], rec["pm2_5"], rec["nh3"], rec["aqi"]), ("openweather", 7.86, 1.16, 2))
        r = reading.from_record(rec)
        self.assertEqual(schema_errors("AqiReading", r), [])
        self.assertEqual((r["source"], r["station"], r["originalDatetime"], r["datetime"]), ("live-openweather", "TP.HCM (OpenWeather)", None, "2026-10-14T12:46:40+07:00"))

    def test_missing_fields_raise_instead_of_inventing_numbers(self):
        for air, weather in (({"list": []}, WEATHER), (AIR, {"dt": 1, "main": {}, "wind": {"speed": 1}}), (AIR, {"dt": 1, "main": {"temp": 30}, "wind": {}}),
                             ({"list": [{"main": {"aqi": 1}, "components": {}, "dt": 1}]}, WEATHER)):
            with self.assertRaises(ValueError):
                owm.to_record(air, weather)

    def test_coordinates_are_those_of_the_raw_fetch_script(self):
        self.assertEqual((owm.LATITUDE, owm.LONGITUDE), (10.8231, 106.6297))


class KeyIsNeverPrinted(unittest.TestCase):
    def test_scrub_removes_key_from_error_text(self):
        e = ConnectionError(f"Max retries exceeded with url: /data/2.5/air_pollution?lat=1&appid={KEY} (Caused by ...)")
        self.assertNotIn(KEY, owm.scrub(e, KEY))
        self.assertIn("***", owm.scrub(e, KEY))

    def test_http_errors_are_described_without_key(self):
        class Resp:
            def __init__(self, code):
                self.status_code = code
        for code, word, wait in ((401, "401", 600.0), (429, "429", 120.0), (500, "500", 0.0)):
            err = owm.requests.HTTPError(f"{code} Client Error for url: https://x/?appid={KEY}", response=Resp(code))
            msg, min_wait = owm.describe_http_error(err, KEY)
            self.assertNotIn(KEY, msg)
            self.assertIn(word, msg)
            self.assertEqual(min_wait, wait)

    def test_main_refuses_to_run_without_env_key(self):
        with mock.patch.dict(os.environ, {}, clear=False), mock.patch.object(sys, "argv", ["x", "--once"]):
            os.environ.pop("OWM_API_KEY", None)
            self.assertEqual(owm.main(), 2)


class FakeConsumer:
    """Giả KafkaConsumer một partition; `messages` là danh sách (value bytes, timestamp ms) theo offset 0..n-1."""
    def __init__(self, messages, **_):
        self.messages, self.pos, self.polls = messages, 0, 0

    def partitions_for_topic(self, _):
        return {0}

    def beginning_offsets(self, parts):
        return {tp: 0 for tp in parts}

    def end_offsets(self, parts):
        return {tp: len(self.messages) for tp in parts}

    def assign(self, parts):
        self.tp = parts[0]  # poll trả theo đúng partition được gán (producer đọc topic thô, backend đọc READING_TOPIC)

    def seek(self, tp, off):
        self.pos = off

    def position(self, tp):
        return self.pos

    def poll(self, timeout_ms=0, max_records=500):
        self.polls += 1
        tp = getattr(self, "tp", None) or next(iter(kafka_stream_tp()))
        end = min(self.pos + max_records, len(self.messages))
        out = [SimpleNamespace(offset=i, value=self.messages[i][0], timestamp=self.messages[i][1]) for i in range(self.pos, end)]
        self.pos = end
        return {tp: out} if out else {}

    def close(self):
        pass


def kafka_stream_tp():
    return [kafka_stream.TopicPartition(config.READING_TOPIC, 0)]  # topic mà backend đọc (kafka_stream._partitions), không phải topic thô


def msg(rec) -> tuple[bytes, int]:
    return json.dumps(rec).encode(), int(time.time() * 1000)


def stamped(msgs):
    """Gán timestamp Kafka tăng dần THEO VỊ TRÍ (offset), như broker thật; timestamp lấy từ lúc tạo tin sẽ sai khi chèn tin vào giữa danh sách."""
    return [(value, 1_700_000_000_000 + i) for i, (value, _) in enumerate(msgs)]


def replay_rec(n):
    return {"datetime": "2026-10-14 13:00:00", "originalDatetime": f"2025-07-01 {n % 24:02d}:00:00", "station": "Tram_Quan_1", "co": 300.0 + n,
            "wind_speed_kmh": 5.0, "temperature_C": 30.0, "provider": "replay-dataset"}


class LastPublished(unittest.TestCase):
    def test_finds_openweather_message_even_when_topic_ends_with_replay_messages(self):
        old = owm.to_record(AIR, WEATHER)
        msgs = [msg(replay_rec(i)) for i in range(5)] + [msg(old)] + [msg(replay_rec(i)) for i in range(5, 30)]
        got = owm.last_published("x", config.KAFKA_TOPIC, consumer_cls=lambda **kw: FakeConsumer(msgs, **kw))
        self.assertEqual((got["dt"], got["values"][0]), (1791956800, 251.31))

    def test_finds_openweather_message_buried_under_thousands_of_replay_messages(self):
        old = owm.to_record(AIR, WEATHER)
        msgs = [msg(replay_rec(i)) for i in range(1)] + [msg(old)] + [msg(replay_rec(i)) for i in range(3000)]  # phát lại 60x chiếm hàng nghìn tin cuối
        got = owm.last_published("x", config.KAFKA_TOPIC, consumer_cls=lambda **kw: FakeConsumer(msgs, **kw))
        self.assertEqual(got["dt"], 1791956800)

    def test_none_when_topic_has_only_replay(self):
        msgs = [msg(replay_rec(i)) for i in range(10)]
        self.assertIsNone(owm.last_published("x", config.KAFKA_TOPIC, consumer_cls=lambda **kw: FakeConsumer(msgs, **kw)))


class SendOnlyNewObservations(unittest.TestCase):
    """Hành vi thật đã đo ngày 14/10/2026: cùng số liệu CO 251,31 đi kèm dt 12:46:40, 13:03:22, 13:14:02 rồi lại 13:08:33 (cũ hơn)."""
    def rec(self, dt, co=251.31):
        air = json.loads(json.dumps(AIR))
        air["list"][0]["dt"], air["list"][0]["components"]["co"] = dt, co
        return owm.to_record(air, WEATHER)

    def test_first_record_is_sent(self):
        self.assertIsNone(owm.skip_reason(self.rec(1000), None))

    def test_newer_dt_with_identical_values_is_skipped(self):
        last = {"dt": 1000, "values": owm.values_of(self.rec(1000))}
        self.assertIn("y hệt", owm.skip_reason(self.rec(1600), last))

    def test_older_or_equal_dt_is_skipped_even_if_values_differ(self):
        last = {"dt": 1000, "values": owm.values_of(self.rec(1000))}
        self.assertIn("không mới hơn", owm.skip_reason(self.rec(900, co=300.0), last))
        self.assertIn("không mới hơn", owm.skip_reason(self.rec(1000, co=300.0), last))

    def test_newer_dt_with_new_values_is_sent(self):
        last = {"dt": 1000, "values": owm.values_of(self.rec(1000))}
        self.assertIsNone(owm.skip_reason(self.rec(4600, co=262.5), last))


class SourceDetection(unittest.TestCase):
    def test_every_source_maps_and_validates(self):
        legacy_replay = {"datetime": "2026-10-13 10:00:00", "station": "Tram_Quan_1", "co": 300.0, "wind_speed_kmh": 9.0, "temperature_C": 30.0}
        open_meteo = {"datetime": "2026-10-12 23:15:00", "station": "Tram_TPHCM_Live_API", "co": 2508.0, "wind_speed_kmh": 1.6, "temperature_C": 25.9}
        for rec, src, orig in ((replay_rec(3), "replay-dataset", "2025-07-01T03:00:00+07:00"), (legacy_replay, "replay-dataset", None),
                               (open_meteo, "live-open-meteo", None), (owm.to_record(AIR, WEATHER), "live-openweather", None)):
            r = reading.from_record(rec)
            self.assertEqual(schema_errors("AqiReading", r), [])
            self.assertEqual((r["source"], r["originalDatetime"]), (src, orig))

    def test_live_record_never_gets_original_datetime_even_if_present(self):
        r = reading.from_record({**owm.to_record(AIR, WEATHER), "originalDatetime": "2020-01-01 00:00:00"})
        self.assertIsNone(r["originalDatetime"])


class LatestPerSource(unittest.TestCase):
    def setUp(self):
        kafka_stream.reset_latest_cache()
        self.addCleanup(kafka_stream.reset_latest_cache)

    def test_finds_old_live_reading_behind_many_replay_messages(self):
        live = owm.to_record(AIR, WEATHER)
        msgs = [msg(replay_rec(i)) for i in range(1200)]
        msgs.insert(100, msg(live))  # dữ liệu thật nằm sau lưng hơn 1000 tin phát lại
        c = FakeConsumer(stamped(msgs))
        r = kafka_stream._latest(c, "live-openweather")
        self.assertEqual((r["source"], r["co"]), ("live-openweather", 251.31))
        self.assertEqual(kafka_stream._latest(c, None)["source"], "replay-dataset")  # tin mới nhất của topic

    def test_unknown_source_returns_none_and_incremental_read_sees_new_message(self):
        msgs = stamped([msg(replay_rec(i)) for i in range(50)])
        c = FakeConsumer(msgs)
        self.assertIsNone(kafka_stream._latest(c, "live-openweather"))
        msgs.append((msg(owm.to_record(AIR, WEATHER))[0], 1_700_000_000_000 + 50))  # tin thật mới đến sau lần hỏi trước
        self.assertEqual(kafka_stream._latest(c, "live-openweather")["source"], "live-openweather")


class ReplayStatusAndControl(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self._old = (config.REPLAY_STATE_DIR, config.REPLAY_CONTROL)
        config.REPLAY_STATE_DIR, config.REPLAY_CONTROL = self.dir, True
        self.addCleanup(lambda: (setattr(config, "REPLAY_STATE_DIR", self._old[0]), setattr(config, "REPLAY_CONTROL", self._old[1])))
        self.client = TestClient(create_app())
        self.addCleanup(self.client.close)

    def heartbeat(self, state="running", speed=10, age=0, emitted=7):
        rp.write_status(Path(self.dir), {"paused": state == "paused", "speed": speed}, 10.0, emitted, datetime(2025, 7, 1, 6, tzinfo=VN))
        if age:  # giả nhịp tim cũ
            f = Path(self.dir) / "status.json"
            d = json.loads(f.read_text(encoding="utf-8"))
            d["updatedAt"] = (datetime.now(VN) - timedelta(seconds=age)).isoformat(timespec="seconds")
            f.write_text(json.dumps(d), encoding="utf-8")

    def status(self):
        r = self.client.get("/api/replay/status")
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(schema_errors("ReplayStatus", r.json()), [], r.text)
        return r.json()

    def test_stopped_when_no_producer(self):
        s = self.status()
        self.assertEqual((s["state"], s["controllable"], s["emittedCount"]), ("stopped", False, 0))
        self.assertIn("live_stack.sh", s["detail"])

    def test_running_status_comes_from_heartbeat(self):
        self.heartbeat("running", 10)
        s = self.status()
        self.assertEqual((s["state"], s["speed"], s["intervalSeconds"], s["emittedCount"], s["controllable"], s["detail"]), ("running", 10, 1.0, 7, True, None))
        self.assertEqual(s["lastOriginalDatetime"], "2025-07-01T06:00:00+07:00")

    def test_stale_heartbeat_is_stopped_not_running(self):
        self.heartbeat("running", age=60)
        self.assertEqual(self.status()["state"], "stopped")

    def test_control_when_stopped_is_409_and_writes_nothing(self):
        r = self.client.post("/api/replay/control", json={"action": "pause"})
        self.assertEqual((r.status_code, r.json()["error"]["code"]), (409, "REPLAY_UNAVAILABLE"))
        self.assertEqual(schema_errors("Error", r.json()), [])
        self.assertFalse((Path(self.dir) / "control.json").exists())

    def test_control_when_disabled_is_409(self):
        self.heartbeat()
        config.REPLAY_CONTROL = False
        s = self.status()
        self.assertEqual((s["controllable"], bool(s["detail"])), (False, True))
        self.assertEqual(self.client.post("/api/replay/control", json={"action": "pause"}).status_code, 409)

    def test_invalid_requests_are_400(self):
        self.heartbeat()
        for bad in ({"action": "stop"}, {"action": "pause", "speed": 5}, {"speed": 10}, {"action": "pause", "extra": 1}):
            r = self.client.post("/api/replay/control", json=bad)
            self.assertEqual((r.status_code, r.json()["error"]["code"]), (400, "BAD_REQUEST"), bad)

    def test_control_roundtrip_with_producer_confirming(self):
        """Producer giả: đọc control.json bằng chính read_control của producer thật rồi ghi lại status (như vòng lặp thật)."""
        self.heartbeat("running", 1)
        stop = threading.Event()

        def fake_producer():
            state = {"paused": False, "speed": 1}
            while not stop.is_set():
                new = rp.read_control(Path(self.dir), state)
                if new != state:
                    state = new
                    rp.write_status(Path(self.dir), state, 10.0, 7, None)
                time.sleep(0.05)
        t = threading.Thread(target=fake_producer, daemon=True)
        t.start()
        self.addCleanup(lambda: (stop.set(), t.join()))
        r = self.client.post("/api/replay/control", json={"action": "pause", "speed": 60})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(schema_errors("ReplayStatus", r.json()), [])
        self.assertEqual((r.json()["state"], r.json()["speed"]), ("paused", 60))
        r = self.client.post("/api/replay/control", json={"action": "resume"})  # bỏ speed: giữ tốc độ hiện tại
        self.assertEqual((r.json()["state"], r.json()["speed"]), ("running", 60))

    def test_command_without_confirmation_is_reported_not_faked(self):
        self.heartbeat("running", 1)  # có nhịp tim nhưng không ai xử lý lệnh
        with mock.patch.object(replay, "CONFIRM_POLLS", 3), mock.patch.object(replay, "CONFIRM_STEP", 0.01):
            r = self.client.post("/api/replay/control", json={"action": "pause"})
        self.assertEqual((r.status_code, r.json()["error"]["code"]), (409, "REPLAY_UNAVAILABLE"))


class ReplayProducerPieces(unittest.TestCase):
    def test_original_datetime_is_utc_plus_7(self):
        self.assertEqual(rp.parse_original("01-07-25 0:00").isoformat(), "2025-07-01T07:00:00+07:00")
        self.assertEqual(rp.parse_original("30-06-25 17:00").isoformat(), "2025-07-01T00:00:00+07:00")

    def test_load_rows_starts_at_requested_time_and_message_keeps_both_times(self):
        d = Path(tempfile.mkdtemp()) / "k.csv"
        d.write_text("datetime,co,no,no2,o3,so2,pm2_5,pm10,nh3,temperature_C,wind_speed_kmh\n"
                     "30-06-25 16:00,900,1,2,3,4,5,6,7,30,5\n30-06-25 17:00,300,1,2,3,4,5,6,7,31,6\n30-06-25 18:00,310,1,2,,4,5,6,7,32,7\n", encoding="utf-8")
        rows, idx = rp.load_rows(str(d), datetime(2025, 7, 1, tzinfo=VN))
        self.assertEqual((len(rows), idx, [r["co"] for r in rows[idx:]]), (3, 1, [300.0, 310.0]))  # nạp cả bộ (cần dòng trước để dựng độ trễ), phát từ chỉ số 1
        now = datetime(2026, 10, 14, 13, 0, 5, tzinfo=VN)
        m = rp.make_message(rows[idx], 1, now)
        self.assertEqual((m["datetime"], m["originalDatetime"], m["provider"]), ("2026-10-14 13:00:05", "2025-07-01 00:00:00", "replay-dataset"))
        self.assertIsNone(rp.make_message(rows[2], 2, now)["o3"])  # ô trống -> null, không điền số
        r = reading.from_record(m)
        self.assertEqual(schema_errors("AqiReading", r), [])
        self.assertEqual((r["source"], r["originalDatetime"], r["datetime"]), ("replay-dataset", "2025-07-01T00:00:00+07:00", "2026-10-14T13:00:05+07:00"))
        with self.assertRaises(ValueError):
            rp.load_rows(str(d), datetime(2030, 1, 1, tzinfo=VN))

    def test_read_control_ignores_garbage(self):
        d = Path(tempfile.mkdtemp())
        cur = {"paused": False, "speed": 10}
        self.assertEqual(rp.read_control(d, cur), cur)  # chưa có tệp
        for bad in ("not json", '{"paused": "yes", "speed": 10}', '{"paused": true, "speed": 7}', '{"paused": true, "speed": true}', "[]"):
            (d / "control.json").write_text(bad, encoding="utf-8")
            self.assertEqual(rp.read_control(d, cur), cur, bad)
        (d / "control.json").write_text('{"paused": true, "speed": 60}', encoding="utf-8")
        self.assertEqual(rp.read_control(d, cur), {"paused": True, "speed": 60})


if __name__ == "__main__":
    unittest.main()
