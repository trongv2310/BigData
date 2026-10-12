"""P12: độ trễ của Spark đọc Kafka tính từ checkpoint trên HDFS. Định dạng file lấy từ checkpoint thật của spark_kafka_consumer.py (2026-10-12)."""
import asyncio
import contextlib
import unittest
from unittest import mock

from app import config, hub as hubmod, kafka_lag, kafka_stream
from app.clients import hdfs, kafka
from helpers import FakeNameNode, schema_errors

# offsets/1 thật: dòng 1 "v1", dòng 2 cấu hình batch, dòng 3 offset kế tiếp của từng partition
OFFSETS_1 = ('v1\n{"batchWatermarkMs":0,"batchTimestampMs":1791823560008,"conf":{"spark.sql.shuffle.partitions":"2"}}\n'
             '{"hcmc_co_stream":{"0":4}}')
CKPT = "/live/_checkpoints/co_stream"


def entry(name):
    return {"pathSuffix": name, "type": "FILE", "length": 10, "modificationTime": 1, "replication": 1, "blockSize": 1, "owner": "trong", "group": "g", "permission": "644"}


def topics(end):
    return [{"name": "hcmc_co_stream", "partitions": 1, "replicationFactor": 1, "approxMessages": end, "endOffsetTotal": end,
             "endOffsets": {0: end}, "consumerGroups": []}]


class Lag(unittest.TestCase):
    def setUp(self):
        self.old = (config.NAMENODE_HTTP, config.SPARK_KAFKA_CHECKPOINTS)

    def tearDown(self):
        config.NAMENODE_HTTP, config.SPARK_KAFKA_CHECKPOINTS = self.old

    def run_with(self, files, end, ckpts=(CKPT,)):
        nn = FakeNameNode(files=files)
        config.NAMENODE_HTTP, config.SPARK_KAFKA_CHECKPOINTS = nn.url, list(ckpts)

        async def go():
            try:
                return await kafka_lag.add_spark_groups(topics(end))
            finally:
                await hdfs.aclose()
        try:
            return asyncio.run(go())
        finally:
            nn.close()

    def test_parse_real_offsets_file(self):
        self.assertEqual(kafka_lag.parse_offsets(OFFSETS_1), {"hcmc_co_stream": {0: 4}})
        for bad in ("", "v1", "v1\n{}\nnot json", '{"batchWatermarkMs":0}'):
            self.assertEqual(kafka_lag.parse_offsets(bad), {}, bad)

    def test_lag_is_end_offset_minus_committed_batch(self):
        files = {f"{CKPT}/commits": [entry("0"), entry("1")], f"{CKPT}/offsets/1": OFFSETS_1}
        got = self.run_with(files, end=6)
        self.assertEqual(got[0]["consumerGroups"], [{"group": "spark:co_stream", "lag": 2}])  # Kafka có 6 tin, Spark đã commit tới offset 4
        self.assertEqual(self.run_with(files, end=4)[0]["consumerGroups"][0]["lag"], 0)

    def test_uses_latest_committed_batch_only(self):
        # batch 2 đã ghi offsets nhưng chưa commit: vẫn tính theo batch 1
        files = {f"{CKPT}/commits": [entry("0"), entry("1")], f"{CKPT}/offsets/1": OFFSETS_1,
                 f"{CKPT}/offsets/2": OFFSETS_1.replace('"0":4', '"0":6')}
        self.assertEqual(self.run_with(files, end=6)[0]["consumerGroups"][0]["lag"], 2)

    def test_no_checkpoint_or_no_commit_adds_nothing(self):
        self.assertEqual(self.run_with({}, end=6)[0]["consumerGroups"], [])
        self.assertEqual(self.run_with({f"{CKPT}/commits": []}, end=6)[0]["consumerGroups"], [])

    def test_hdfs_down_adds_nothing(self):
        config.SPARK_KAFKA_CHECKPOINTS = [CKPT]
        config.NAMENODE_HTTP = "http://127.0.0.1:1"

        async def go():
            try:
                return await kafka_lag.add_spark_groups(topics(6))
            finally:
                await hdfs.aclose()
        self.assertEqual(asyncio.run(go())[0]["consumerGroups"], [])


class LiveView(unittest.TestCase):
    """View `live` của /api/sql: Parquet do spark_kafka_consumer.py ghi (cột lấy từ file thật đọc lại ngày 2026-10-12)."""
    DDL = ("topic string, partition int, offset long, kafka_timestamp timestamp, datetime string, station string, co double, "
           "wind_speed_kmh double, temperature_C double, coObservedAt string, weatherObservedAt string, provider string, "
           "co_accumulation_index double, canh_bao string")

    @classmethod
    def setUpClass(cls):
        import tempfile
        from datetime import datetime
        from pathlib import Path
        from fastapi.testclient import TestClient
        from app import sql_engine
        from app.main import create_app
        from test_p3 import write_csv

        cls.d = Path(tempfile.mkdtemp())
        write_csv(cls.d / "s.csv", datetime(2025, 6, 27, 17), 24, lambda t: 100)
        cls._old = (config.SQL_DATASET_URI, config.SQL_LIVE_URI)
        config.SQL_DATASET_URI, config.SQL_LIVE_URI = (cls.d / "s.csv").as_uri(), (cls.d / "live").as_uri()
        cls.client, cls.sql_engine = TestClient(create_app()), sql_engine

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        cls.sql_engine.stop()
        config.SQL_DATASET_URI, config.SQL_LIVE_URI = cls._old

    def sql(self, query, status=200):
        r = self.client.post("/api/sql", json={"query": query})
        self.assertEqual(r.status_code, status, r.text)
        self.assertEqual(schema_errors("SqlResponse" if status == 200 else "Error", r.json()), [], r.text)
        return r.json()

    def write(self, rows, mode):
        from datetime import datetime
        spark = self.sql_engine._session()
        spark.createDataFrame([(("hcmc_co_stream", 0, o, datetime(2026, 10, 12, 16, 45), dt, "Tram_TPHCM_Live_API", co, wind, 25.8,
                                 "2026-10-12T23:00", dt[:10] + "T" + dt[11:16], "open-meteo", round(co / (wind + 1), 2), "NGUY HIEM (DANGER)"))
                               for o, dt, co, wind in rows], self.DDL).write.mode(mode).parquet(str(self.d / "live"))

    def test_live_view_appears_when_data_arrives_and_sees_new_rows(self):
        self.assertEqual(self.sql("SELECT * FROM live", 400)["error"]["code"], "SQL_ERROR")  # chưa có dữ liệu sống: như bảng không tồn tại
        self.assertEqual(self.sql("SELECT count(*) FROM aqi")["rows"], [[24]])  # bảng aqi không bị ảnh hưởng
        self.write([(3, "2026-10-12 23:45:00", 2508.0, 2.2)], "overwrite")
        b = self.sql("SELECT datetime_vn, co, wind_speed_kmh, co_accumulation_index, kafka_offset, provider FROM live")
        self.assertEqual(b["rows"], [["2026-10-12 23:45:00", 2508.0, 2.2, 783.75, 3, "open-meteo"]])
        self.write([(4, "2026-10-13 00:00:00", 2100.0, 1.0)], "append")  # Spark ghi thêm một batch: lần truy vấn sau phải thấy
        self.assertEqual(self.sql("SELECT count(*) AS n, max(kafka_offset) AS last_offset FROM live")["rows"], [[2, 4]])


class ExtraFlows(unittest.TestCase):
    """Luồng kafka-produce và spark-streaming: chỉ có khi FLOWS_EXTRA; mặc định tắt (chỉ có ý nghĩa khi STREAM_SOURCE=kafka)."""

    def setUp(self):
        self.old = config.FLOWS_EXTRA

    def tearDown(self):
        config.FLOWS_EXTRA = self.old

    def client(self):
        from fastapi.testclient import TestClient
        from app.main import app
        return TestClient(app)

    def test_off_by_default_keeps_them_planned_and_404(self):
        config.FLOWS_EXTRA = False
        with self.client() as c:
            st = {f["id"]: f["status"] for f in c.get("/api/flows").json()["flows"]}
            self.assertEqual((st["kafka-produce"], st["spark-streaming"], st["hdfs-write"]), ("planned", "planned", "available"))
            self.assertEqual(c.get("/api/flows/kafka-produce").status_code, 404)

    def test_on_publishes_valid_definitions(self):
        config.FLOWS_EXTRA = True
        with self.client() as c:
            lst = c.get("/api/flows").json()
            self.assertEqual(schema_errors("FlowList", lst), [])
            self.assertEqual({f["id"]: f["status"] for f in lst["flows"]}["kafka-produce"], "available")
            for fid, steps in (("kafka-produce", 2), ("spark-streaming", 4)):
                d = c.get(f"/api/flows/{fid}").json()
                self.assertEqual(schema_errors("FlowDefinition", d), [], fid)
                ids = {x["id"] for x in d["components"]}
                self.assertEqual(len(d["steps"]), steps)
                self.assertTrue(all(s["from"] in ids and s["to"] in ids for s in d["steps"]), fid)  # bước chỉ nối các thành phần có thật
                self.assertEqual([s["order"] for s in d["steps"]], list(range(1, steps + 1)))

    def test_step_events_match_schema_and_definition(self):
        from app import flows
        ev = flows.kafka_produce_steps(0, 7) + flows.spark_batch_steps(12)
        for e in ev:
            self.assertEqual(schema_errors("FlowStepEvent", {"type": "flow.step", "ts": "2026-10-12T23:00:00+07:00", "seq": 0, "data": e}), [])
        self.assertEqual([(e["runId"], e["step"]) for e in ev[:2]], [("kafka-p0-o7", 1), ("kafka-p0-o7", 2)])
        self.assertEqual({e["runId"] for e in ev[2:]}, {"spark-batch-12"})
        self.assertEqual([e["step"] for e in ev[2:]], [1, 2, 3, 4])

    def run_hub(self, coro_name, patches, seconds=0.5):
        async def scenario():
            h = hubmod.Hub()
            q = asyncio.Queue()
            h._subs.add(q)
            with contextlib.ExitStack() as st:
                for p in patches:
                    st.enter_context(p)
                task = asyncio.create_task(getattr(h, coro_name)())
                await asyncio.sleep(seconds)
                task.cancel()
            return [q.get_nowait() for _ in range(q.qsize())]
        return asyncio.run(scenario())

    def test_hub_emits_kafka_steps_per_new_offset_only_when_on(self):
        old = (config.STREAM_SOURCE, hubmod.KAFKA_READ_POLL_SECONDS)
        config.STREAM_SOURCE, hubmod.KAFKA_READ_POLL_SECONDS = "kafka", 0.05
        calls = []

        async def end():
            return {0: 10}

        async def reads(pos):
            calls.append(1)
            return ([], {0: 12}) if len(calls) == 1 else ([], pos)  # tin offset 10 và 11 vừa đến

        try:
            for on, expect in ((True, [("kafka-p0-o10", 1), ("kafka-p0-o10", 2), ("kafka-p0-o11", 1), ("kafka-p0-o11", 2)]), (False, [])):
                config.FLOWS_EXTRA = on
                calls.clear()
                got = self.run_hub("_poll_kafka_readings", [mock.patch.object(kafka_stream, "end_offsets", end),
                                                            mock.patch.object(kafka_stream, "read_new", reads)])
                self.assertEqual([(d["runId"], d["step"]) for t, d in got if t == "flow.step"], expect, on)
        finally:
            config.STREAM_SOURCE, hubmod.KAFKA_READ_POLL_SECONDS = old

    def test_hub_emits_spark_steps_per_batch_only_when_on(self):
        from app import spark_progress
        old = hubmod.SPARK_POLL_SECONDS
        hubmod.SPARK_POLL_SECONDS = 0.05
        prog = {"queryName": "HCMC_CO_Kafka_Streaming", "batchId": 5, "numInputRows": 1, "inputRowsPerSecond": 0.0, "processedRowsPerSecond": 1.0}
        try:
            for on, expect_steps in ((True, 4), (False, 0)):
                config.FLOWS_EXTRA = on
                sent = []

                async def poll(self_):
                    if sent:
                        return []
                    sent.append(1)
                    return [prog]

                got = self.run_hub("_poll_spark", [mock.patch.object(spark_progress.ProgressTracker, "poll", poll)])
                self.assertEqual(sum(1 for t, _ in got if t == "spark.progress"), 1)
                self.assertEqual(sum(1 for t, _ in got if t == "flow.step"), expect_steps, on)
        finally:
            hubmod.SPARK_POLL_SECONDS = old


class Route(unittest.TestCase):
    def test_topics_route_includes_spark_lag_and_matches_schema(self):
        from fastapi.testclient import TestClient
        from app.main import app

        async def fake_snapshot():
            return topics(6)

        async def fake_add(ts):
            ts[0]["consumerGroups"].append({"group": "spark:co_stream", "lag": 2})
            return ts

        with mock.patch.object(kafka, "snapshot", fake_snapshot), mock.patch.object(kafka_lag, "add_spark_groups", fake_add):
            with TestClient(app) as c:
                r = c.get("/api/kafka/topics")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(schema_errors("KafkaTopics", r.json()), [])
        self.assertEqual(r.json()["topics"][0]["consumerGroups"], [{"group": "spark:co_stream", "lag": 2}])
        self.assertNotIn("endOffsets", r.json()["topics"][0])  # trường nội bộ không lộ ra ngoài hợp đồng


if __name__ == "__main__":
    unittest.main()
