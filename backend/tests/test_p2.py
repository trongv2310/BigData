"""P2: /api/hdfs/*, /api/kafka/topics, /api/spark/applications với NameNode/Spark giả (số liệu lấy từ cụm thật 2026-10-12)."""
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient

from app import config
from app.clients import kafka
from app.main import create_app
from helpers import FakeNameNode, schema_errors

HOST = "DESKTOP-HHFANUM.localdomain"


def dn(port, blocks, used=1000, admin="In Service"):
    return {"infoAddr": f"127.0.0.1:{port - 2}", "xferaddr": f"127.0.0.1:{port}", "lastContact": 1, "usedSpace": used, "adminState": admin,
            "capacity": 10_000, "numBlocks": blocks, "version": "3.3.6", "used": used}


NAMENODE_INFO = {
    "ClusterId": "CID-test", "Version": "3.3.6, r1be78238728da9266a4f88195058f08fd012bf9c", "Safemode": "",
    "LiveNodes": json.dumps({f"{HOST}:9866": dn(9866, 9), f"{HOST}:9876": dn(9876, 7, admin="Decommission In Progress")}),
    "DeadNodes": json.dumps({f"{HOST}:9886": {"lastContact": 700, "decommissioned": False, "adminState": "In Service", "xferaddr": "127.0.0.1:9886"}}),
}
FS = {"CapacityTotal": 40_000, "CapacityUsed": 2_000, "CapacityRemaining": 30_000, "FilesTotal": 29, "BlocksTotal": 24,
      "UnderReplicatedBlocks": 1, "MissingBlocks": 0}


def st(name, typ="FILE", length=0, mtime=1790831665693, rep=1, bs=1048576):
    d = typ == "DIRECTORY"
    return {"pathSuffix": name, "type": typ, "length": 0 if d else length, "replication": 0 if d else rep, "blockSize": 0 if d else bs,
            "modificationTime": mtime, "owner": "trong"}


def loc(port):
    return {"hostName": HOST, "xferPort": port}


class Hdfs(unittest.TestCase):
    def setUp(self):
        self.nn = FakeNameNode(
            jmx={"Hadoop:service=NameNode,name=NameNodeInfo": NAMENODE_INFO, "Hadoop:service=NameNode,name=FSNamesystem": FS},
            defaults={"blockSize": 134217728, "replication": 1},
            conf={"dfs.heartbeat.interval": "3", "dfs.namenode.heartbeat.recheck-interval": "300000"},
            files={
                "/": [st("raw", "DIRECTORY"), st("stream_input", "DIRECTORY")],
                "/raw": [st("Dataset.csv", length=3179004)],
                "/raw/Dataset.csv": [st("", length=3179004)],  # LISTSTATUS trên file trả một phần tử pathSuffix rỗng
                "/raw/Dataset.csv#status": st("", length=3179004),
                "/raw#status": st("", "DIRECTORY"),
            },
            blocks={"/raw/Dataset.csv": [
                {"block": {"blockId": 1073741850, "numBytes": 1048576}, "locations": [loc(9876), loc(9866)]},
                {"block": {"blockId": 1073741851, "numBytes": 33276}, "locations": [loc(9866)]},
            ]},
        )
        self._old = config.NAMENODE_HTTP
        config.NAMENODE_HTTP = self.nn.url
        self.client = TestClient(create_app())

    def tearDown(self):
        self.client.close()
        config.NAMENODE_HTTP = self._old
        self.nn.close()

    def get(self, url, schema, status=200):
        r = self.client.get(url)
        self.assertEqual(r.status_code, status, r.text)
        self.assertEqual(schema_errors(schema, r.json()), [], r.text)
        return r.json()

    def test_summary(self):
        b = self.get("/api/hdfs/summary", "HdfsSummary")
        self.assertEqual((b["version"], b["safeMode"], b["liveDataNodes"], b["deadDataNodes"], b["underReplicatedBlocks"]), ("3.3.6", False, 2, 1, 1))
        self.assertEqual((b["defaultBlockSizeBytes"], b["capacityTotalBytes"]), (134217728, 40000))

    def test_summary_safe_mode(self):
        NAMENODE_INFO["Safemode"] = "Safe mode is ON."
        try:
            self.assertTrue(self.get("/api/hdfs/summary", "HdfsSummary")["safeMode"])
        finally:
            NAMENODE_INFO["Safemode"] = ""

    def test_datanodes(self):
        b = self.get("/api/hdfs/datanodes", "HdfsDataNodes")
        self.assertEqual(b["deadAfterSeconds"], 630)
        self.assertEqual([(n["name"], n["state"]) for n in b["dataNodes"]],
                         [(f"{HOST}:9866", "live"), (f"{HOST}:9876", "decommissioning"), (f"{HOST}:9886", "dead")])
        self.assertEqual(b["dataNodes"][2]["lastContactSeconds"], 700)

    def test_files_and_blocks_use_same_datanode_names(self):
        files = self.get("/api/hdfs/files?path=/raw", "HdfsListing")
        self.assertEqual(files["entries"][0]["name"], "Dataset.csv")
        self.assertEqual(files["entries"][0]["modifiedAt"], "2026-10-01T12:14:25+07:00")
        root = self.get("/api/hdfs/files", "HdfsListing")
        self.assertEqual([e["type"] for e in root["entries"]], ["directory", "directory"])
        blocks = self.get("/api/hdfs/blocks?path=/raw/Dataset.csv", "HdfsFileBlocks")
        names = {n["name"] for n in self.get("/api/hdfs/datanodes", "HdfsDataNodes")["dataNodes"]}
        self.assertEqual(blocks["blockCount"], 2)
        self.assertEqual(blocks["blocks"][0]["blockId"], "blk_1073741850")
        self.assertTrue({loc_ for b in blocks["blocks"] for loc_ in b["locations"]} <= names)

    def test_errors_follow_error_schema(self):
        self.get("/api/hdfs/files?path=/nope", "Error", 404)
        self.get("/api/hdfs/blocks?path=/nope", "Error", 404)
        self.get("/api/hdfs/files?path=/raw/Dataset.csv", "Error", 400)
        self.get("/api/hdfs/files?path=raw", "Error", 400)
        self.get("/api/hdfs/files?path=/raw/../etc", "Error", 400)
        self.get("/api/hdfs/blocks?path=/raw", "Error", 400)
        self.get("/api/hdfs/blocks", "Error", 400)

    def test_namenode_down_is_503(self):
        config.NAMENODE_HTTP = "http://127.0.0.1:1"
        for u in ("/api/hdfs/summary", "/api/hdfs/datanodes", "/api/hdfs/files", "/api/hdfs/blocks?path=/raw/Dataset.csv"):
            self.assertEqual(self.get(u, "Error", 503)["error"]["code"], "HDFS_UNREACHABLE")


class Spark(unittest.TestCase):
    def test_applications(self):
        fake = FakeNameNode(extra={
            "/api/v1/applications": [
                {"id": "local-1", "name": "hcmc_co_streaming", "attempts": [{"startTimeEpoch": 1791793417770, "completed": False}]},
                {"id": "local-0", "name": "noattempt", "attempts": []},
            ],
            "/api/v1/applications/local-1/jobs": [{"status": "SUCCEEDED"}, {"status": "SUCCEEDED"}, {"status": "RUNNING"}, {"status": "FAILED"}],
        })
        old = config.SPARK_UI_PORTS
        config.SPARK_UI_PORTS = [fake.server.server_address[1]]
        try:
            with TestClient(create_app()) as c:
                b = c.get("/api/spark/applications").json()
        finally:
            config.SPARK_UI_PORTS = old
            fake.close()
        self.assertEqual(schema_errors("SparkApplications", b), [])
        self.assertEqual(len(b["applications"]), 1)
        a = b["applications"][0]
        self.assertEqual((a["state"], a["jobsRunning"], a["jobsSucceeded"], a["jobsFailed"]), ("running", 1, 2, 1))

    def test_no_ui_means_empty_list_not_error(self):
        old = config.SPARK_UI_PORTS
        config.SPARK_UI_PORTS = [1]
        try:
            with TestClient(create_app()) as c:
                r = c.get("/api/spark/applications")
        finally:
            config.SPARK_UI_PORTS = old
        self.assertEqual((r.status_code, r.json()), (200, {"applications": []}))


class Kafka(unittest.TestCase):
    """Chưa thử với broker thật (Kafka không khởi động được trên máy lúc làm). Chỉ kiểm cách đổi dữ liệu sang hợp đồng."""

    def test_topics_mapping(self):
        async def fake_snapshot():
            return [{"name": "hcmc_co_stream", "partitions": 1, "replicationFactor": 1, "approxMessages": 12, "endOffsetTotal": 12,
                     "consumerGroups": [{"group": "spark", "lag": 3}]}]
        old = kafka.snapshot
        kafka.snapshot = fake_snapshot
        try:
            with TestClient(create_app()) as c:
                b = c.get("/api/kafka/topics").json()
        finally:
            kafka.snapshot = old
        self.assertEqual(schema_errors("KafkaTopics", b), [])
        self.assertNotIn("endOffsetTotal", b["topics"][0])

    def test_broker_down_is_503(self):
        old = config.KAFKA_BOOTSTRAP
        config.KAFKA_BOOTSTRAP = "127.0.0.1:1"
        try:
            with TestClient(create_app()) as c:
                r = c.get("/api/kafka/topics")
        finally:
            config.KAFKA_BOOTSTRAP = old
        self.assertEqual(r.status_code, 503)
        self.assertEqual(schema_errors("Error", r.json()), [])


if __name__ == "__main__":
    unittest.main()
