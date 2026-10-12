"""P4: /api/flows, đọc log NameNode, hdfs.op / flow.step. Dòng log mẫu là dòng thật từ NameNode của cụm (2026-10-01)."""
import asyncio
import json
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient

from app import config, flows, hadoop_logs, hdfs_flow, hub as hubmod
from app.main import create_app
from helpers import FakeNameNode, ROOT, schema_errors

SC = "org.apache.hadoop.hdfs.StateChange"
# Lượt `hdfs dfs -put` thật của /raw/Dataset.csv: 4 block rồi đóng (1 bản sao mỗi block)
PUT = [
    f"2026-10-01 12:14:25,302 INFO {SC}: BLOCK* allocate blk_1073741850_1026, replicas=127.0.0.1:9876 for /raw/Dataset.csv._COPYING_",
    f"2026-10-01 12:14:25,420 INFO {SC}: BLOCK* allocate blk_1073741851_1027, replicas=127.0.0.1:9876 for /raw/Dataset.csv._COPYING_",
    f"2026-10-01 12:14:25,548 INFO {SC}: BLOCK* allocate blk_1073741852_1028, replicas=127.0.0.1:9866 for /raw/Dataset.csv._COPYING_",
    f"2026-10-01 12:14:25,678 INFO {SC}: BLOCK* allocate blk_1073741853_1029, replicas=127.0.0.1:9896 for /raw/Dataset.csv._COPYING_",
    f"2026-10-01 12:14:25,693 INFO {SC}: DIR* completeFile: /raw/Dataset.csv._COPYING_ is closed by DFSClient_NONMAPREDUCE_740999925_1",
]
THREE = f"2026-10-01 13:51:57,373 INFO {SC}: BLOCK* allocate blk_1073741867_1043, replicas=127.0.0.1:9876, 127.0.0.1:9866, 127.0.0.1:9896 for /output/x/part-1.parquet"
AUDIT = "2026-10-12 16:00:00,123 INFO org.apache.hadoop.hdfs.server.namenode.FSNamesystem.audit: allowed=true\tugi=trong (auth:SIMPLE)\tip=/127.0.0.1\tcmd={cmd}\tsrc={src}\tdst={dst}\tperm=trong:supergroup:rw-r--r--\tproto=rpc"


class Definitions(unittest.TestCase):
    def test_match_mock_files(self):
        mock = ROOT / "mock"
        self.assertEqual(json.loads((mock / "flows.json").read_text(encoding="utf-8")), {"flows": flows.SUMMARIES})
        self.assertEqual(json.loads((mock / "flow-hdfs-write.json").read_text(encoding="utf-8")), flows.DEFINITIONS["hdfs-write"])

    def test_routes(self):
        with TestClient(create_app()) as c:
            self.assertEqual(schema_errors("FlowList", c.get("/api/flows").json()), [])
            r = c.get("/api/flows/hdfs-write")
            self.assertEqual(schema_errors("FlowDefinition", r.json()), [])
            for planned in ("hdfs-read", "nope"):
                r = c.get(f"/api/flows/{planned}")
                self.assertEqual(r.status_code, 404)
                self.assertEqual(schema_errors("Error", r.json()), [])

    def test_steps_reference_existing_components_and_flow_step_numbers(self):
        d = flows.DEFINITIONS["hdfs-write"]
        ids = {c["id"] for c in d["components"]}
        self.assertTrue(all(s["from"] in ids and s["to"] in ids for s in d["steps"]))
        orders = {s["order"] for s in d["steps"]}
        used = {getattr(flows, n) for n in dir(flows) if n.startswith("STEP_")}
        self.assertTrue(used <= orders)


class Parsing(unittest.TestCase):
    def test_real_lines(self):
        a = hadoop_logs.parse_line(PUT[0])
        self.assertEqual((a.kind, a.block, a.replicas, a.path), ("allocate", "blk_1073741850", ["127.0.0.1:9876"], "/raw/Dataset.csv._COPYING_"))
        self.assertEqual(a.ts, datetime(2026, 10, 1, 12, 14, 25, 302000))
        self.assertEqual(hadoop_logs.parse_line(THREE).replicas, ["127.0.0.1:9876", "127.0.0.1:9866", "127.0.0.1:9896"])
        c = hadoop_logs.parse_line(PUT[-1])
        self.assertEqual((c.kind, c.path), ("complete", "/raw/Dataset.csv._COPYING_"))

    def test_audit_lines(self):
        e = hadoop_logs.parse_line(AUDIT.format(cmd="rename", src="/a/x._COPYING_", dst="/a/x"))
        self.assertEqual((e.kind, e.path, e.dst), ("rename", "/a/x._COPYING_", "/a/x"))
        self.assertEqual(hadoop_logs.parse_line(AUDIT.format(cmd="mkdirs", src="/d", dst="null")).kind, "mkdir")
        self.assertIsNone(hadoop_logs.parse_line(AUDIT.format(cmd="getfileinfo", src="/d", dst="null")))

    def test_noise_is_ignored(self):
        for l in (f"2026-10-01 13:51:58,092 INFO {SC}: STATE* Safe mode is OFF", "", "java.lang.Exception: x", f"2026-10-01 12:00:00,000 INFO {SC}: BLOCK* registerDatanode: from DatanodeRegistration"):
            self.assertIsNone(hadoop_logs.parse_line(l))


class TailerTest(unittest.TestCase):
    def test_starts_at_end_reads_new_keeps_partial_line_and_handles_truncate(self):
        d = Path(tempfile.mkdtemp())
        f = d / "hadoop-trong-namenode-x.log"
        f.write_text("old1\nold2\n")
        t = hadoop_logs.Tailer(str(d / "hadoop-*-namenode-*.log"))
        self.assertEqual(t.read_new(), [])  # lịch sử bỏ qua
        with f.open("a") as fh:
            fh.write("new1\nnew")
        self.assertEqual(t.read_new(), ["new1"])
        with f.open("a") as fh:
            fh.write("2\n")
        self.assertEqual(t.read_new(), ["new2"])
        f.write_text("fresh\n")  # file bị cắt/xoay
        self.assertEqual(t.read_new(), ["fresh"])

    def test_file_appearing_later_is_read_from_start(self):
        d = Path(tempfile.mkdtemp())
        t = hadoop_logs.Tailer(str(d / "hdfs-audit.log"))
        self.assertFalse(t.exists())
        self.assertEqual(t.read_new(), [])
        (d / "hdfs-audit.log").write_text("a\nb\n")
        self.assertEqual(t.read_new(), ["a", "b"])


def make_tracker(blocks, final_exists=True, audit=False):
    async def fetch_blocks(path):
        return blocks

    async def exists(path):
        return final_exists

    async def resolve(addr):
        return "HOST:" + addr.split(":")[1]

    return hdfs_flow.WriteTracker(fetch_blocks, exists, resolve, audit)


def run_events(tracker, lines):
    async def go():
        out = []
        for l in lines:
            ev = hadoop_logs.parse_line(l)
            out += await tracker.feed(ev)
        return out
    return asyncio.run(go())


DATASET_BLOCKS = [
    {"blockId": "blk_1073741850", "lengthBytes": 1048576, "locations": ["HOST:9876"]},
    {"blockId": "blk_1073741851", "lengthBytes": 1048576, "locations": ["HOST:9876"]},
    {"blockId": "blk_1073741852", "lengthBytes": 1048576, "locations": ["HOST:9866"]},
    {"blockId": "blk_1073741853", "lengthBytes": 33276, "locations": ["HOST:9896"]},
]


class Tracker(unittest.TestCase):
    def steps(self, out):
        return [d["step"] for t, d in out if t == "flow.step"]

    def test_real_put_produces_steps_in_order_and_valid_events(self):
        out = run_events(make_tracker(DATASET_BLOCKS), PUT)
        for i, (t, d) in enumerate(out):
            self.assertEqual(schema_errors("StreamEvent", {"type": t, "ts": "2026-10-12T12:00:00+07:00", "seq": i, "data": d}), [], (t, d))
        # 1 (suy ra) rồi mỗi block 2,3; sau đó 4,5,6 cho từng block; 7; 8
        self.assertEqual(self.steps(out), [1, 2, 3] + [2, 3] * 3 + [4, 5, 6] * 4 + [7, 8])
        self.assertEqual(len({d["runId"] for t, d in out if t == "flow.step"}), 1)
        ops = [d for t, d in out if t == "hdfs.op"]
        self.assertEqual([o["op"] for o in ops], ["addBlock"] * 4 + ["close", "rename"])
        self.assertEqual(ops[0]["datanode"], "HOST:9876")
        self.assertEqual((ops[4]["bytes"], ops[4]["path"]), (3179004, "/raw/Dataset.csv._COPYING_"))
        self.assertEqual(ops[5]["path"], "/raw/Dataset.csv")

    def test_first_block_order_is_1_to_8_for_single_block_file(self):
        lines = [PUT[0].replace("/raw/Dataset.csv", "/stream_input/replay_7.json"), PUT[-1].replace("/raw/Dataset.csv", "/stream_input/replay_7.json")]
        blocks = [{"blockId": "blk_1073741850", "lengthBytes": 120, "locations": ["HOST:9876"]}]
        self.assertEqual(self.steps(run_events(make_tracker(blocks), lines)), [1, 2, 3, 4, 5, 6, 7, 8])

    def test_two_files_get_different_run_ids(self):
        t = make_tracker(DATASET_BLOCKS[:1])
        a = run_events(t, [PUT[0].replace("Dataset", "a"), PUT[-1].replace("Dataset", "a")])
        b = run_events(t, [PUT[0].replace("Dataset", "b"), PUT[-1].replace("Dataset", "b")])
        self.assertNotEqual({d["runId"] for ty, d in a if ty == "flow.step"}, {d["runId"] for ty, d in b if ty == "flow.step"})

    def test_blocks_not_confirmed_by_namenode_skip_steps_4_to_6(self):
        out = run_events(make_tracker(None), PUT[:1] + PUT[-1:])
        self.assertEqual(self.steps(out), [1, 2, 3, 7, 8])
        close = [d for t, d in out if t == "hdfs.op" and d["op"] == "close"][0]
        self.assertIsNone(close["bytes"])
        empty_loc = [{"blockId": "blk_1073741850", "lengthBytes": 5, "locations": []}]
        self.assertEqual(self.steps(run_events(make_tracker(empty_loc), PUT[:1] + PUT[-1:])), [1, 2, 3, 7, 8])

    def test_no_rename_step_when_final_name_not_visible(self):
        out = run_events(make_tracker(DATASET_BLOCKS[:1], final_exists=False), PUT[:1] + PUT[-1:])
        self.assertEqual(self.steps(out)[-1], 7)

    def test_close_without_seen_allocate_is_close_op_only(self):
        out = run_events(make_tracker(None), PUT[-1:])
        self.assertEqual([(t, d["op"]) for t, d in out], [("hdfs.op", "close")])

    def test_pipeline_uses_first_replica_as_datanode(self):
        out = run_events(make_tracker(None), [THREE])
        self.assertEqual([d for t, d in out if t == "hdfs.op"][0]["datanode"], "HOST:9876")

    def test_with_audit_create_and_rename_are_real(self):
        t = make_tracker(DATASET_BLOCKS[:1], audit=True)
        lines = [AUDIT.format(cmd="create", src="/raw/Dataset.csv._COPYING_", dst="null"), PUT[0], PUT[-1],
                 AUDIT.format(cmd="rename", src="/raw/Dataset.csv._COPYING_", dst="/raw/Dataset.csv")]
        out = run_events(t, lines)
        self.assertEqual(self.steps(out), [1, 2, 3, 4, 5, 6, 7, 8])
        self.assertEqual([d["op"] for ty, d in out if ty == "hdfs.op"], ["create", "addBlock", "close", "rename"])
        self.assertEqual(t.runs, {})

    def test_audit_mkdir_delete(self):
        out = run_events(make_tracker(None, audit=True), [AUDIT.format(cmd="mkdirs", src="/d", dst="null"), AUDIT.format(cmd="delete", src="/d", dst="null")])
        self.assertEqual([d["op"] for t, d in out], ["mkdir", "delete"])


class HubIntegration(unittest.TestCase):
    """Hub đọc file log giả và NameNode giả, phát sự kiện đúng thứ tự."""

    def test_events_flow_through_hub(self):
        logdir = Path(tempfile.mkdtemp())
        log = logdir / "hadoop-trong-namenode-test.log"
        log.write_text(PUT[0] + "\n")  # lịch sử: phải bị bỏ qua
        blocks = [{"block": {"blockId": 1073741850, "numBytes": 120}, "locations": [{"hostName": "H", "xferPort": 9876}]}]
        nn = FakeNameNode(
            files={"/stream_input/r.json#status": {"type": "FILE"}},
            blocks={"/stream_input/r.json._COPYING_": blocks},
            jmx={"Hadoop:service=NameNode,name=NameNodeInfo": {"LiveNodes": json.dumps({"H:9876": {"xferaddr": "127.0.0.1:9876"}}), "DeadNodes": "{}"}},
        )
        old = (config.HADOOP_LOG_DIR, config.NAMENODE_HTTP, hubmod.LOG_POLL_SECONDS)
        config.HADOOP_LOG_DIR, config.NAMENODE_HTTP, hubmod.LOG_POLL_SECONDS = str(logdir), nn.url, 0.05

        async def scenario():
            from app.clients import hdfs
            try:
                h = hubmod.Hub()
                q = asyncio.Queue()
                h._subs.add(q)
                task = asyncio.create_task(h._poll_hdfs_logs())
                await asyncio.sleep(0.3)
                self.assertTrue(q.empty())
                with log.open("a") as f:
                    f.write(PUT[1].replace("blk_1073741851_1027", "blk_1073741850_1026").replace("/raw/Dataset.csv", "/stream_input/r.json") + "\n")
                    f.write(PUT[-1].replace("/raw/Dataset.csv", "/stream_input/r.json") + "\n")
                await asyncio.sleep(1.5)
                task.cancel()
                return [q.get_nowait() for _ in range(q.qsize())]
            finally:
                await hdfs.aclose()

        try:
            got = asyncio.run(scenario())
        finally:
            config.HADOOP_LOG_DIR, config.NAMENODE_HTTP, hubmod.LOG_POLL_SECONDS = old
            nn.close()
        steps = [d["step"] for t, d in got if t == "flow.step"]
        self.assertEqual(steps, [1, 2, 3, 4, 5, 6, 7, 8])
        ops = [d for t, d in got if t == "hdfs.op"]
        self.assertEqual([o["op"] for o in ops], ["addBlock", "close", "rename"])
        self.assertEqual((ops[0]["datanode"], ops[1]["bytes"]), ("H:9876", 120))


if __name__ == "__main__":
    unittest.main()
