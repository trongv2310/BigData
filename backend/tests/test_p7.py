"""P7: spark.progress suy ra từ Spark REST (job + stage) với Spark giả. Dạng dữ liệu lấy từ cụm thật 2026-10-12 (mỗi batch một job một stage)."""
import asyncio
import unittest

from app import config, hub as hubmod, spark_progress
from app.clients import hdfs
from helpers import FakeNameNode, schema_errors

APP = "local-1"
Q = "2e936c6f-1f18-4612-b022-d87cc8f06580"


def job(jid, batch, start, end, status="SUCCEEDED"):
    j = {"jobId": jid, "stageIds": [jid], "status": status, "submissionTime": f"2026-10-12T14:04:{start}GMT",
         "description": f"\nid = {Q}\nrunId = r1\nbatch = {batch}"}
    if end:
        j["completionTime"] = f"2026-10-12T14:04:{end}GMT"
    return j


def stage(rows):
    return [{"stageId": 0, "inputRecords": rows}]


def app(completed=False):
    return [{"id": APP, "name": "HCMC_CO_Realtime_Streaming", "attempts": [{"completed": completed, "startTimeEpoch": 1}]}]


class Progress(unittest.TestCase):
    def setUp(self):
        self.fake = FakeNameNode(extra={"/api/v1/applications": app()})
        self.old = config.SPARK_UI_PORTS
        config.SPARK_UI_PORTS = [self.fake.server.server_address[1]]

    def tearDown(self):
        config.SPARK_UI_PORTS = self.old
        self.fake.close()

    def set_jobs(self, jobs, rows=1):
        e = self.fake.extra
        e[f"/api/v1/applications/{APP}/jobs"] = jobs
        for j in jobs:
            e[f"/api/v1/applications/{APP}/stages/{j['jobId']}"] = stage(rows if not isinstance(rows, dict) else rows[j["jobId"]])

    def poll(self, tracker):
        async def go():
            try:
                return await tracker.poll()
            finally:
                await hdfs.aclose()
        return asyncio.run(go())

    def test_first_poll_only_sets_mark_then_reports_new_batches(self):
        t = spark_progress.ProgressTracker()
        self.set_jobs([job(2, 2, "10.000", "10.200"), job(1, 1, "00.000", "00.100")])
        self.assertEqual(self.poll(t), [])  # lịch sử không phát lại
        self.set_jobs([job(4, 4, "30.000", "30.500"), job(3, 3, "20.000", "20.020"), job(2, 2, "10.000", "10.200"), job(1, 1, "00.000", "00.100")],
                      rows={1: 1, 2: 1, 3: 1, 4: 5})
        got = self.poll(t)
        self.assertEqual([g["batchId"] for g in got], [3, 4])
        # batch 3: nộp 20.000, batch trước (2) nộp 10.000 -> cách 10 s; chạy 0,02 s
        self.assertEqual(got[0]["numInputRows"], 1)
        self.assertEqual(got[0]["inputRowsPerSecond"], 0.1)
        self.assertEqual(got[0]["processedRowsPerSecond"], 50.0)
        # batch 4: 5 dòng, cách batch 3 10 s, chạy 0,5 s
        self.assertEqual((got[1]["numInputRows"], got[1]["inputRowsPerSecond"], got[1]["processedRowsPerSecond"]), (5, 0.5, 10.0))
        self.assertEqual(got[1]["queryName"], "HCMC_CO_Realtime_Streaming")
        for g in got:
            ev = {"type": "spark.progress", "ts": "2026-10-12T21:00:00+07:00", "seq": 0, "data": g}
            self.assertEqual(schema_errors("SparkProgressEvent", ev), [])
        self.assertEqual(self.poll(t), [])  # không phát lặp

    def test_running_job_is_not_skipped(self):
        t = spark_progress.ProgressTracker()
        self.set_jobs([job(1, 1, "00.000", "00.100")])
        self.poll(t)
        self.set_jobs([job(3, 3, "10.000", "10.100"), job(2, 2, "05.000", None, status="RUNNING"), job(1, 1, "00.000", "00.100")])
        self.assertEqual(self.poll(t), [])  # batch 2 còn chạy: chưa báo batch 3 vượt mặt nó
        self.set_jobs([job(3, 3, "10.000", "10.100"), job(2, 2, "05.000", "05.100"), job(1, 1, "00.000", "00.100")])
        self.assertEqual([g["batchId"] for g in self.poll(t)], [2, 3])

    def test_new_query_first_batches_are_reported(self):
        t = spark_progress.ProgressTracker()
        self.set_jobs([])
        self.assertEqual(self.poll(t), [])
        self.set_jobs([job(1, 0, "00.000", "00.400")])
        got = self.poll(t)
        self.assertEqual([(g["batchId"], g["inputRowsPerSecond"]) for g in got], [(0, 0.0)])  # chưa có batch trước: 0

    def test_ignores_completed_apps_and_unreachable_spark(self):
        t = spark_progress.ProgressTracker()
        self.fake.extra["/api/v1/applications"] = app(completed=True)
        self.set_jobs([job(1, 1, "00.000", "00.100")])
        self.assertEqual(self.poll(t), [])
        config.SPARK_UI_PORTS = [1]
        self.assertEqual(self.poll(t), [])

    def test_hub_publishes_spark_progress(self):
        self.set_jobs([job(1, 1, "00.000", "00.100")])
        old = hubmod.SPARK_POLL_SECONDS
        hubmod.SPARK_POLL_SECONDS = 0.1

        async def scenario():
            try:
                h = hubmod.Hub()
                q = asyncio.Queue()
                h._subs.add(q)
                task = asyncio.create_task(h._poll_spark())
                await asyncio.sleep(0.4)
                self.assertTrue(q.empty())
                self.set_jobs([job(2, 2, "10.000", "10.100"), job(1, 1, "00.000", "00.100")])
                await asyncio.sleep(0.6)
                task.cancel()
                return [q.get_nowait() for _ in range(q.qsize())]
            finally:
                await hdfs.aclose()

        try:
            got = asyncio.run(scenario())
        finally:
            hubmod.SPARK_POLL_SECONDS = old
        self.assertEqual([(t, d["batchId"]) for t, d in got], [("spark.progress", 2)])


if __name__ == "__main__":
    unittest.main()
