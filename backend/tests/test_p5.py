"""P5: /api/predict, /api/sql (SparkSQL thật trên CSV nhỏ), /ws/terminal (bash thật). Cần Linux/WSL, pyspark, Java."""
import json
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from app import aqi_data, config, sql_engine, sql_guard
from app.main import create_app
from app.routes import predict as predict_route, terminal
from helpers import ROOT, schema_errors
from test_p3 import write_csv

ORIGIN = {"origin": "http://localhost:5173"}


class Guard(unittest.TestCase):
    def ok(self, q):
        return sql_guard.check(q)

    def bad(self, q):
        with self.assertRaises(sql_guard.SqlNotAllowed, msg=q):
            sql_guard.check(q)

    def test_allowed(self):
        self.assertEqual(self.ok("SELECT 1;"), "SELECT 1")
        self.ok("  select avg(co) from aqi where hour_vn = 7 group by month_vn order by 1 limit 5 ")
        self.ok("WITH m AS (SELECT month_vn, avg(co) c FROM aqi GROUP BY 1) SELECT * FROM m")
        self.ok("SELECT 'insert; drop' AS s")  # từ khóa và dấu ; nằm trong chuỗi thì được
        self.ok("SELECT 1 -- ; drop table x")
        self.ok("SELECT date_add(datetime_vn, 1) FROM aqi")  # date_add không phải ADD

    def test_not_select(self):
        for q in ("INSERT INTO t VALUES (1)", "DROP TABLE aqi", "CREATE TABLE x AS SELECT 1", "SET spark.x=1", "SHOW TABLES", "EXPLAIN SELECT 1",
                  "CACHE TABLE aqi", "", "   ;  ", "-- chỉ comment"):
            self.bad(q)

    def test_multiple_statements(self):
        self.bad("SELECT 1; SELECT 2")
        self.bad("SELECT 1; DROP TABLE aqi")
        self.bad("SELECT 1 /* x */ ; /* y */ SELECT 2")

    def test_dangerous_functions_and_paths(self):
        self.bad("SELECT reflect('java.lang.Runtime', 'getRuntime')")
        self.bad("SELECT JAVA_METHOD('java.lang.System', 'exit', 1)")
        self.bad("SELECT * FROM csv.`/etc/passwd`")
        self.bad("SELECT input_file_name() FROM aqi")
        self.bad("SELECT 1 WHERE x IN (SELECT 1) UNION ALL SELECT insert FROM t")

    def test_unterminated(self):
        self.bad("SELECT 'abc")
        self.bad("SELECT 1 /* abc")
        self.bad("SELECT " + "1," * 3000)


class Predict(unittest.TestCase):
    def setUp(self):
        d = Path(tempfile.mkdtemp())
        # 60 ngày trước điểm gãy (CO 1000) + 60 ngày sau (CO theo giờ: 100 + giờ VN * 10)
        start = datetime(2025, 5, 2, 17)
        write_csv(d / "p.csv", start, 24 * 120, lambda t: 1000 if t < datetime(2025, 6, 30, 17) else 100 + ((t.hour + 7) % 24) * 10)
        self._old = config.AQI_CSV_PATH
        config.AQI_CSV_PATH = str(d / "p.csv")
        aqi_data._cache = None
        predict_route._cache = None
        self.client = TestClient(create_app())

    def tearDown(self):
        self.client.close()
        config.AQI_CSV_PATH = self._old
        aqi_data._cache = None
        predict_route._cache = None

    def post(self, body, status=200, schema="PredictResponse"):
        r = self.client.post("/api/predict", json=body)
        self.assertEqual(r.status_code, status, r.text)
        self.assertEqual(schema_errors(schema, r.json()), [], r.text)
        return r.json()

    REQ = {"hour": 12, "month": 11, "temperatureC": 30, "windSpeedKmh": 8}

    def test_uses_only_current_regime_and_hour(self):
        b = self.post(self.REQ)
        # giờ 12 ±1 -> CO 210, 220, 230 -> trung vị 220; không bị kéo bởi giai đoạn trước điểm gãy (CO 1000)
        self.assertEqual(b["coPredicted"], 220.0)
        self.assertEqual((b["interval"]["low"], b["interval"]["high"]), (210.0, 230.0))
        self.assertEqual((b["model"]["kind"], b["status"]), ("heuristic", "SAFE"))
        self.assertIn("Không phải mô hình học máy", b["model"]["trainedOn"])

    def test_other_inputs_do_not_change_result(self):
        a = self.post(self.REQ)
        b = self.post({**self.REQ, "month": 2, "temperatureC": 20, "windSpeedKmh": 0, "dayOfWeek": 3})
        self.assertEqual(a["coPredicted"], b["coPredicted"])

    def test_hour_wraps_around_midnight(self):
        # CO theo giờ VN h là 100 + 10h. Cửa sổ giờ 0 gồm các giờ 23, 0, 1 -> 330, 100, 110 -> trung vị 110; giờ 23 gồm 22, 23, 0 -> 320
        self.assertEqual(self.post({**self.REQ, "hour": 0})["coPredicted"], 110.0)
        self.assertEqual(self.post({**self.REQ, "hour": 23})["coPredicted"], 320.0)

    def test_validation_is_400_with_error_schema(self):
        for bad in ({**self.REQ, "hour": 24}, {**self.REQ, "month": 0}, {**self.REQ, "windSpeedKmh": -1}, {**self.REQ, "extra": 1},
                    {"hour": 1}, {**self.REQ, "dayOfWeek": 7}):
            self.assertEqual(self.post(bad, 400, "Error")["error"]["code"], "BAD_REQUEST")


class Sql(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        d = Path(tempfile.mkdtemp())
        write_csv(d / "s.csv", datetime(2025, 6, 27, 17), 144, lambda t: 100 if t < datetime(2025, 6, 30, 17) else 10)
        cls._old = (config.SQL_DATASET_URI, config.SQL_TIMEOUT_SECONDS)
        config.SQL_DATASET_URI = (d / "s.csv").as_uri()
        cls.client = TestClient(create_app())

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        sql_engine.stop()
        config.SQL_DATASET_URI, config.SQL_TIMEOUT_SECONDS = cls._old

    def sql(self, query, status=200, **kw):
        r = self.client.post("/api/sql", json={"query": query, **kw})
        self.assertEqual(r.status_code, status, r.text)
        self.assertEqual(schema_errors("SqlResponse" if status == 200 else "Error", r.json()), [], r.text)
        return r.json()

    def test_select_and_types(self):
        b = self.sql("SELECT year_vn, count(*) AS n, avg(co) AS avg_co FROM aqi GROUP BY year_vn")
        self.assertEqual([c["name"] for c in b["columns"]], ["year_vn", "n", "avg_co"])
        self.assertEqual(b["rows"], [[2025, 144, (100 * 72 + 10 * 72) / 144]])
        self.assertFalse(b["truncated"])

    def test_vietnam_time_is_utc_plus_7_and_formatted(self):
        b = self.sql("SELECT datetime_utc, datetime_vn, hour_vn FROM aqi ORDER BY datetime_utc LIMIT 1")
        self.assertEqual(b["rows"][0], ["2025-06-27 17:00:00", "2025-06-28 00:00:00", 0])
        self.assertEqual(b["columns"][0]["type"], "timestamp")

    def test_limit_and_truncated(self):
        b = self.sql("SELECT co FROM aqi", limit=5)
        self.assertEqual((b["rowCount"], b["truncated"]), (5, True))
        b = self.sql("SELECT co FROM aqi", limit=1000)
        self.assertEqual((b["rowCount"], b["truncated"]), (144, False))

    def test_duplicate_column_names_and_special_values(self):
        b = self.sql("SELECT co, co, CAST('NaN' AS DOUBLE) AS n, 1/0 AS z, array(1,2) AS a FROM aqi LIMIT 1")
        self.assertEqual(len(b["columns"]), 5)
        self.assertEqual(b["rows"][0][2:], [None, None, "[1, 2]"])  # NaN và chia 0 thành null, mảng thành chữ

    def test_bad_requests(self):
        self.assertEqual(self.sql("SELECT nope FROM aqi", 400)["error"]["code"], "SQL_ERROR")
        self.assertEqual(self.sql("SELECT * FROM khong_co_bang", 400)["error"]["code"], "SQL_ERROR")
        self.assertEqual(self.sql("SELECT FROM", 400)["error"]["code"], "SQL_ERROR")
        self.assertEqual(self.sql("DROP TABLE aqi", 400)["error"]["code"], "SQL_NOT_ALLOWED")
        self.assertEqual(self.client.post("/api/sql", json={"query": "SELECT 1", "limit": 5000}).status_code, 400)

    def test_files_cannot_be_read_even_if_guard_were_bypassed(self):
        spark = sql_engine._session()
        with self.assertRaises(Exception):
            spark.sql("SELECT * FROM csv.`/etc/passwd`").collect()

    def test_timeout_cancels(self):
        old = config.SQL_TIMEOUT_SECONDS
        config.SQL_TIMEOUT_SECONDS = 2
        try:
            r = self.sql("SELECT sum(id * id) FROM range(100000000000)", 504)
        finally:
            config.SQL_TIMEOUT_SECONDS = old
        self.assertEqual(r["error"]["code"], "SQL_TIMEOUT")
        self.assertEqual(self.sql("SELECT 1 AS x")["rows"], [[1]])  # sau khi hủy vẫn dùng được

    def test_unavailable_when_dataset_missing(self):
        sql_engine.stop()
        old = config.SQL_DATASET_URI
        config.SQL_DATASET_URI = "file:///khong/co/file.csv"
        try:
            self.assertEqual(self.sql("SELECT 1", 503)["error"]["code"], "SQL_UNAVAILABLE")
        finally:
            config.SQL_DATASET_URI = old


def read_until(ws, needle, limit=200):
    seen = ""
    for _ in range(limit):
        m = json.loads(ws.receive_text())
        if m["type"] == "output":
            seen += m["data"]
            if needle in seen:
                return seen
        elif m["type"] == "exit":
            return seen + f"<exit {m['code']}>"
    raise AssertionError(f"không thấy {needle!r} trong: {seen[-300:]!r}")


class Terminal(unittest.TestCase):
    def setUp(self):
        self._old = (config.ALLOW_WRITES, config.TERMINAL_IDLE_SECONDS)
        config.ALLOW_WRITES = True
        self.client = TestClient(create_app())
        self.url = f"/ws/terminal?token={terminal.TOKEN}"

    def tearDown(self):
        self.client.close()
        config.ALLOW_WRITES, config.TERMINAL_IDLE_SECONDS = self._old

    def expect_close(self, url, headers, code):
        with self.client.websocket_connect(url, headers=headers) as ws:
            with self.assertRaises(WebSocketDisconnect) as cm:
                ws.receive_text()
        self.assertEqual(cm.exception.code, code)

    def test_disabled_by_default(self):
        config.ALLOW_WRITES = False
        self.expect_close(self.url, ORIGIN, terminal.CLOSE_DISABLED)

    def test_wrong_or_missing_token_is_4401(self):
        self.expect_close("/ws/terminal?token=sai", ORIGIN, 4401)
        self.expect_close("/ws/terminal", ORIGIN, 4401)

    def test_wrong_or_missing_origin_is_refused_before_handshake(self):
        for headers in ({"origin": "http://evil.example"}, {}):
            with self.assertRaises(WebSocketDisconnect):
                with self.client.websocket_connect(self.url, headers=headers):
                    pass

    def test_non_loopback_client_is_refused(self):
        terminal.LOOPBACK.discard("testclient")
        try:
            with self.assertRaises(WebSocketDisconnect):
                with self.client.websocket_connect(self.url, headers=ORIGIN):
                    pass
        finally:
            terminal.LOOPBACK.add("testclient")

    def test_runs_commands_in_repo_dir_and_resizes(self):
        with self.client.websocket_connect(self.url, headers=ORIGIN) as ws:
            ws.send_text(json.dumps({"type": "resize", "cols": 120, "rows": 30}))
            ws.send_text(json.dumps({"type": "input", "data": "echo hello-$((6*7)); pwd; stty size\n"}))
            out = read_until(ws, "30 120")
            self.assertIn("hello-42", out)
            self.assertIn(str(ROOT), out)

    def test_single_session_only(self):
        with self.client.websocket_connect(self.url, headers=ORIGIN) as first:
            first.send_text(json.dumps({"type": "input", "data": "echo ready\n"}))
            read_until(first, "ready")
            self.expect_close(self.url, ORIGIN, terminal.CLOSE_BUSY)
        # sau khi phiên đầu đóng thì mở lại được
        with self.client.websocket_connect(self.url, headers=ORIGIN) as again:
            again.send_text(json.dumps({"type": "input", "data": "echo again\n"}))
            read_until(again, "again")

    def test_exit_code_is_reported(self):
        with self.client.websocket_connect(self.url, headers=ORIGIN) as ws:
            ws.send_text(json.dumps({"type": "input", "data": "exit 3\n"}))
            self.assertIn("<exit 3>", read_until(ws, "never-appears"))

    def test_idle_timeout(self):
        config.TERMINAL_IDLE_SECONDS = 1
        with self.client.websocket_connect(self.url, headers=ORIGIN) as ws:
            with self.assertRaises(WebSocketDisconnect) as cm:
                for _ in range(100):
                    ws.receive_text()  # chỉ có lời nhắc của bash rồi im lặng cho đến khi hết hạn
        self.assertEqual(cm.exception.code, terminal.CLOSE_IDLE)

    def test_garbage_messages_do_not_kill_session(self):
        with self.client.websocket_connect(self.url, headers=ORIGIN) as ws:
            for junk in ("khong phai json", "[1,2]", '{"type":"resize","cols":-5,"rows":"x"}', '{"type":"input","data":5}'):
                ws.send_text(junk)
            ws.send_text(json.dumps({"type": "input", "data": "echo alive\n"}))
            read_until(ws, "alive")


if __name__ == "__main__":
    unittest.main()
