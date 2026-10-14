"""Bước A2: /api/pipeline/steps (đọc step_summary.json) và phép làm sạch của pipeline/cleaning.py trên bảng nhỏ có đủ loại lỗi.
Chạy: cd backend && python3 -m unittest discover tests"""
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(ROOT / "pipeline"))

from fastapi.testclient import TestClient

from app import config, pipeline_steps
from app.main import create_app
from helpers import FakeNameNode, schema_errors

# Dạng và số liệu thật do pipeline/01_prepare_upload.py ghi ngày 14/10/2026 (rút gọn bước còn lại)
REAL = {"version": 1, "steps": {
    "prepare-hdfs": {"state": "done", "updatedAt": "2026-10-14T13:44:19+07:00",
                     "summary": [{"label": "Thư mục của dự án", "value": 3, "unit": "thư mục"}], "detail": "Tạo /co/raw, /co/processed, /co/output."},
    "upload-raw": {"state": "done", "updatedAt": "2026-10-14T13:44:35+07:00",
                   "summary": [{"label": "Số dòng dữ liệu", "value": 35064, "unit": "dòng"}, {"label": "md5 (12 ký tự đầu)", "value": "f2c8a08ba6a6", "unit": None}],
                   "detail": None},
    "clean": {"state": "running", "updatedAt": "2026-10-14T13:46:00+07:00", "summary": [], "detail": None}}}


class Build(unittest.TestCase):
    def test_no_file_means_nine_not_started(self):
        b = pipeline_steps.build(None)
        self.assertEqual(schema_errors("PipelineSteps", b), [])
        self.assertEqual([s["state"] for s in b["steps"]], ["notStarted"] * 9)
        self.assertEqual([s["order"] for s in b["steps"]], list(range(1, 10)))

    def test_real_shape_is_mapped_in_order(self):
        b = pipeline_steps.build(REAL)
        self.assertEqual(schema_errors("PipelineSteps", b), [])
        by = {s["id"]: s for s in b["steps"]}
        self.assertEqual((by["prepare-hdfs"]["state"], by["upload-raw"]["summary"][0]["value"], by["clean"]["state"]), ("done", 35064, "running"))
        self.assertEqual(by["upload-raw"]["summary"][1], {"label": "md5 (12 ký tự đầu)", "value": "f2c8a08ba6a6", "unit": None})
        self.assertEqual(by["read-spark"]["state"], "notStarted")  # không có trong file: không bịa

    def test_malformed_entries_are_isolated_and_reported(self):
        doc = {"steps": {"clean": {"state": "bogus"}, "read-spark": {"state": "done", "updatedAt": "hôm qua",
                                                                     "summary": [{"label": "ok", "value": 1, "unit": None}, {"label": "x", "value": True, "unit": None}, "rác"],
                                                                     "detail": "chi tiết"},
                         "unknown-step": {"state": "done"}, "spark-sql": "không phải dict"}}
        b = pipeline_steps.build(doc)
        self.assertEqual(schema_errors("PipelineSteps", b), [])
        by = {s["id"]: s for s in b["steps"]}
        self.assertEqual(by["clean"]["state"], "failed")
        self.assertIn("không hợp lệ", by["clean"]["detail"])
        self.assertEqual((by["read-spark"]["state"], by["read-spark"]["updatedAt"], len(by["read-spark"]["summary"])), ("done", None, 1))
        self.assertIn("bỏ 2 mục", by["read-spark"]["detail"])
        self.assertEqual(by["spark-sql"]["state"], "notStarted")
        self.assertNotIn("unknown-step", [s["id"] for s in b["steps"]])

    def test_ids_and_titles_match_the_contract(self):
        import yaml
        spec = yaml.safe_load((ROOT / "docs/contract/openapi.yaml").read_text(encoding="utf-8"))
        self.assertEqual([i for i, _ in pipeline_steps.STEPS], spec["components"]["schemas"]["PipelineStep"]["properties"]["id"]["enum"])


class Route(unittest.TestCase):
    def setUp(self):
        self._old = config.NAMENODE_HTTP
        self.addCleanup(lambda: setattr(config, "NAMENODE_HTTP", self._old))
        self.client = TestClient(create_app())
        self.addCleanup(self.client.close)

    def get(self, nn_files=None, status=200):
        if nn_files is None:
            config.NAMENODE_HTTP = "http://127.0.0.1:1"
        else:
            nn = FakeNameNode(files=nn_files)
            self.addCleanup(nn.close)
            config.NAMENODE_HTTP = nn.url
        r = self.client.get("/api/pipeline/steps")
        self.assertEqual(r.status_code, status, r.text)
        self.assertEqual(schema_errors("PipelineSteps" if status == 200 else "Error", r.json()), [], r.text)
        return r.json()

    def test_reads_summary_from_hdfs(self):
        b = self.get({config.PIPELINE_SUMMARY_PATH: json.dumps(REAL)})
        self.assertEqual([s["state"] for s in b["steps"]][:4], ["done", "done", "notStarted", "running"])

    def test_missing_file_is_all_not_started_not_an_error(self):
        self.assertEqual({s["state"] for s in self.get({})["steps"]}, {"notStarted"})

    def test_hdfs_down_is_503_not_a_guess(self):
        self.assertEqual(self.get(None, 503)["error"]["code"], "PIPELINE_UNAVAILABLE")

    def test_corrupt_json_is_503(self):
        self.assertEqual(self.get({config.PIPELINE_SUMMARY_PATH: "{không phải json"}, 503)["error"]["code"], "PIPELINE_UNAVAILABLE")


try:
    import pyspark  # noqa: F401
    HAVE_SPARK = True
except ImportError:
    HAVE_SPARK = False

HEADER = "datetime,aqi_label,co,no,no2,o3,so2,pm2_5,pm10,nh3,temperature_C,humidity_%,rain_mm,wind_speed_kmh,is_event_anomaly,hour,month,dayofweek,o3_was_missing"


def line(dt, co="300", aqi="1", hum="80", rain="0", wind="5", anomaly="0"):
    return f"{dt},{aqi},{co},1,2,3,4,5,6,7,30,{hum},{rain},{wind},{anomaly},17,6,0,0"


@unittest.skipUnless(HAVE_SPARK, "không có pyspark")
class Cleaning(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master("local[1]").appName("test_a2").config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def run_clean(self, lines):
        import cleaning
        p = Path(tempfile.mkdtemp()) / "t.csv"
        p.write_text("\n".join([HEADER, *lines]) + "\n", encoding="utf-8")
        raw = self.spark.read.option("header", True).option("inferSchema", False).csv(str(p))
        df, st = cleaning.clean(raw)
        from pyspark.sql import functions as F
        fmt = "yyyy-MM-dd'T'HH:mm:ss"  # định dạng trong Spark (múi giờ phiên UTC): thu timestamp về Python sẽ bị đổi sang giờ máy
        shown = df.withColumn("datetime_utc", F.date_format("datetime_utc", fmt)).withColumn("datetime_vn", F.date_format("datetime_vn", fmt))
        rows = {r["datetime_utc"]: r.asDict() for r in shown.collect()}
        return df, st, rows

    def test_every_rule_on_a_table_with_every_kind_of_defect(self):
        import cleaning
        df, st, rows = self.run_clean([
            line("30-06-25 16:00"),                          # 23:00 +07 ngày 30/06: trước điểm gãy
            line("30-06-25 17:00"),                          # 00:00 +07 ngày 01/07: sau điểm gãy
            line("30-06-25 17:00", co="999"),                # trùng datetime -> bỏ một dòng
            line("khong-phai-ngay"),                         # datetime không đọc được -> bỏ
            line("01-07-25 0:00", co=""),                    # thiếu CO -> bỏ
            line("01-07-25 1:00", co="abc"),                 # CO không phải số -> bỏ
            line("01-07-25 2:00", co="-5"),                  # CO âm -> bỏ (không để CO null lọt ra đầu ra)
            line("01-07-25 3:00", hum="110.5"),              # độ ẩm > 100 -> cắt về 100 + cờ
            line("01-07-25 4:00", rain="-1", wind="n/a", aqi="7", anomaly="2"),  # ngoài miền / không phải số -> null
        ])
        self.assertEqual((st["rowsIn"], st["droppedBadDatetime"], st["droppedBadCo"], st["droppedDuplicates"]), (9, 1, 3, 1))
        self.assertEqual(df.count(), 9 - 1 - 3 - 1)
        self.assertEqual((st["humidityClipped"], st["nulledTotal"]), (1, 4))
        self.assertEqual(st["nulledByColumn"], {"rain_mm": 1, "wind_speed_kmh": 1, "aqi_label": 1, "is_event_anomaly": 1})
        self.assertEqual(list(df.columns), cleaning.OUTPUT_COLUMNS)
        self.assertTrue(all(r["co"] is not None for r in rows.values()))

        a, b = rows["2025-06-30T16:00:00"], rows["2025-06-30T17:00:00"]
        self.assertEqual((a["after_2025_07_01"], b["after_2025_07_01"]), (0, 1))          # điểm gãy tính theo giờ +07
        self.assertEqual((b["datetime_vn"], b["hour"], b["month"], b["year"], b["dayofweek"]), ("2025-07-01T00:00:00", 0, 7, 2025, 1))  # thứ Ba = 1
        self.assertEqual((a["hour"], a["month"], a["dayofweek"]), (23, 6, 0))              # thứ Hai 30/06 23:00 +07; hour 17 của file (UTC) bị thay
        self.assertEqual(rows["2025-06-30T17:00:00"]["co"], 300.0)                         # giữ dòng đầu của cặp trùng (co=300, không phải 999)
        h = rows["2025-07-01T03:00:00"]
        self.assertEqual((h["humidity_pct"], h["humidity_clipped"]), (100.0, 1))
        self.assertEqual(a["humidity_clipped"], 0)
        n = rows["2025-07-01T04:00:00"]
        self.assertEqual((n["rain_mm"], n["wind_speed_kmh"], n["aqi_label"], n["is_event_anomaly"]), (None, None, None, None))
        self.assertEqual(n["co"], 300.0)                                                    # các cột khác của dòng đó giữ nguyên

    def test_clean_input_passes_through_unchanged_in_count(self):
        df, st, rows = self.run_clean([line("01-04-22 0:00"), line("01-04-22 1:00", hum="100")])
        self.assertEqual((st["rowsIn"], df.count(), st["nulledTotal"], st["humidityClipped"]), (2, 2, 0, 0))
        self.assertEqual(rows["2022-04-01T00:00:00"]["datetime_vn"], "2022-04-01T07:00:00")  # UTC + 7
        self.assertEqual(rows["2022-04-01T01:00:00"]["humidity_pct"], 100.0)               # đúng 100 không bị coi là >100


if __name__ == "__main__":
    unittest.main()
