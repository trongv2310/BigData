"""P1: chuyển bản ghi luồng -> AqiReading, đọc /stream_input, /api/services, /ws/stream.   Chạy: cd backend && python3 -m unittest discover tests"""
import asyncio
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient

from app import config, reading, stream_input
from app.clients import hdfs
from app.main import create_app
from helpers import FakeNameNode, schema_errors

REPLAY = {"datetime": "2026-10-12 15:30:00", "station": "Tram_Quan_5", "co": 600.0, "wind_speed_kmh": 7.0, "temperature_C": 31.5}
LIVE = {"datetime": "2026-10-12 15:30:03", "station": "Tram_TPHCM_Live_API", "co": 210.0, "wind_speed_kmh": 9.0, "temperature_C": 30.0}


def fstat(name, mtime, typ="FILE"):
    return {"pathSuffix": name, "type": typ, "modificationTime": mtime, "length": 100, "blockSize": 1, "replication": 1, "owner": "trong"}


class ReadingTest(unittest.TestCase):
    def test_replay_record(self):
        r = reading.from_record(REPLAY)
        self.assertEqual(schema_errors("AqiReading", r), [])
        self.assertEqual((r["station"], r["source"], r["status"], r["coAccumulationIndex"]), ("Quận 5", "replay-dataset", "WARNING", 75.0))
        self.assertTrue(r["isSimulatedStation"])
        self.assertEqual(r["datetime"], "2026-10-12T15:30:00+07:00")

    def test_live_record_is_separate_source(self):
        r = reading.from_record(LIVE)
        self.assertEqual(schema_errors("AqiReading", r), [])
        self.assertEqual((r["source"], r["status"]), ("live-open-meteo", "SAFE"))
        self.assertTrue(r["isSimulatedStation"])  # D2: luôn true

    def test_thresholds_follow_consumer(self):
        self.assertEqual([reading.status_for(x) for x in (500, 500.1, 1000, 1000.1)], ["SAFE", "WARNING", "WARNING", "DANGER"])

    def test_broken_file_is_none(self):
        self.assertIsNone(reading.parse_file_text(""))
        self.assertIsNone(reading.parse_file_text('{"co": 1'))
        self.assertIsNone(reading.parse_file_text('{"co": 1}'))


class StreamInputTest(unittest.TestCase):
    def setUp(self):
        self.nn = FakeNameNode(files={
            "/stream_input": [fstat("replay_1.json", 1000), fstat("live_1.json", 2000), fstat("x.json._COPYING_", 3000), fstat("sub", 4000, "DIRECTORY")],
            "/stream_input/replay_1.json": json.dumps(REPLAY) + "\n",
            "/stream_input/live_1.json": json.dumps(LIVE) + "\n",
        })
        self._old = config.NAMENODE_HTTP
        config.NAMENODE_HTTP = self.nn.url

    def tearDown(self):
        config.NAMENODE_HTTP = self._old
        self.nn.close()

    def run_async(self, coro):
        async def wrapper():
            try:
                return await coro
            finally:
                await hdfs.aclose()
        return asyncio.run(wrapper())

    def test_list_skips_temp_and_dirs(self):
        files = self.run_async(stream_input.list_files())
        self.assertEqual([f["pathSuffix"] for f in files], ["replay_1.json", "live_1.json"])

    def test_latest_is_newest_file(self):
        r = self.run_async(stream_input.latest_reading())
        self.assertEqual(r["source"], "live-open-meteo")

    def test_latest_none_when_dir_empty(self):
        self.nn.files["/stream_input"] = []
        self.assertIsNone(self.run_async(stream_input.latest_reading()))

    def test_unreachable_raises(self):
        config.NAMENODE_HTTP = "http://127.0.0.1:1"
        with self.assertRaises(hdfs.HdfsUnreachable):
            self.run_async(stream_input.latest_reading())

    def test_write_ops_are_refused(self):
        with self.assertRaises(hdfs.HdfsError):
            self.run_async(hdfs.webhdfs("/x", "DELETE"))


class HubTest(StreamInputTest):
    def test_only_new_files_become_readings(self):
        from app import hub as hubmod

        async def scenario():
            hubmod.STREAM_POLL_SECONDS = 0.05
            h = hubmod.Hub()
            q = asyncio.Queue()
            h._subs.add(q)
            task = asyncio.create_task(h._poll_stream_input())
            await asyncio.sleep(0.3)
            self.assertTrue(q.empty())  # hai file có sẵn không bị phát lại
            self.nn.files["/stream_input"].append(fstat("replay_2.json", 5000))
            self.nn.files["/stream_input/replay_2.json"] = json.dumps(REPLAY) + "\n"
            await asyncio.sleep(0.4)
            task.cancel()
            return [q.get_nowait() for _ in range(q.qsize())]

        got = self.run_async(scenario())
        self.assertEqual([t for t, _ in got], ["reading"])
        self.assertEqual(got[0][1]["station"], "Quận 5")


class AppTest(unittest.TestCase):
    def test_services_match_contract(self):
        old = config.NAMENODE_HTTP
        config.NAMENODE_HTTP = "http://127.0.0.1:1"  # NameNode "tắt" -> phải báo down, không văng lỗi
        try:
            with TestClient(create_app()) as c:
                body = c.get("/api/services").json()
        finally:
            config.NAMENODE_HTTP = old
        self.assertEqual(schema_errors("ServiceList", body), [])
        by = {s["kind"]: s for s in body["services"]}
        self.assertEqual(by["namenode"]["state"], "down")
        self.assertEqual(by["backend"]["state"], "up")

    def test_replay_stream_events_match_contract_and_seq_increases(self):
        old = config.STREAM_MODE
        config.STREAM_MODE = "replay"
        try:
            with TestClient(create_app()) as c, c.websocket_connect("/ws/stream") as ws:
                evs = [json.loads(ws.receive_text()) for _ in range(3)]
        finally:
            config.STREAM_MODE = old
        self.assertEqual([e["seq"] for e in evs], [0, 1, 2])
        for e in evs:
            self.assertEqual(schema_errors("StreamEvent", e), [])

    def test_unknown_route_uses_error_schema(self):
        with TestClient(create_app()) as c:
            r = c.get("/api/nope")
        self.assertEqual(r.status_code, 404)
        self.assertEqual(schema_errors("Error", r.json()), [])


if __name__ == "__main__":
    unittest.main()
