"""Bước A5: đầu vào mô hình do producer dựng (infra/model_inputs.py), làm giàu bằng mô hình trên Spark (infra/enrich.py), reading mang coPredicted/modelVersion.
Quan trọng nhất: đầu vào dựng ở producer PHẢI bằng đặc trưng lúc huấn luyện (modeling.build_features), và dự đoán qua enrich() bằng dự đoán của PipelineModel offline.
Chạy: cd backend && python3 -m unittest discover tests"""
import json
import math
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(ROOT / "pipeline"))
sys.path.insert(0, str(ROOT / "infra"))

import model_inputs as mi
from app import config, kafka_stream, reading
from helpers import schema_errors

try:
    import pyspark  # noqa: F401
    import requests
    HAVE = True
except ImportError:
    HAVE = False

VN = timezone(timedelta(hours=7))
GASES = ("no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3")


def dataset_rows(n=60):
    """n dòng giờ liên tục từ 2025-08-01 07:00 (+07); mọi giá trị khác nhau theo dòng để lỗi lấy nhầm hàng lộ ra."""
    start = datetime(2025, 8, 1, 7, tzinfo=VN)
    rows = []
    for i in range(n):
        rows.append({"orig": start + timedelta(hours=i), "co": 200.0 + 3.5 * i + (i % 5), "wind": 4.0 + (i % 7) * 0.5, "temp": 28.0 + (i % 6) * 0.25,
                     "no": 0.1 * i, "no2": 2.0 + 0.2 * i, "o3": 30.0 + (i % 9), "so2": 1.0 + 0.05 * i, "pm2_5": 8.0 + 0.3 * i, "pm10": 12.0 + 0.4 * i, "nh3": 2.0 + 0.01 * i})
    return rows


class FromDataset(unittest.TestCase):
    def test_values_come_from_the_right_rows(self):
        rows = dataset_rows()
        i = 30
        x = mi.from_dataset(rows, i)
        self.assertEqual(list(x), mi.FEATURES)
        self.assertEqual((x["hour"], x["month"]), (rows[i]["orig"].hour, 8))                      # giờ và tháng của chính giờ được dự đoán
        self.assertEqual((x["coLag1h"], x["coLag2h"], x["coLag3h"], x["coLag24h"]), (rows[29]["co"], rows[28]["co"], rows[27]["co"], rows[6]["co"]))
        self.assertEqual((x["temperatureC"], x["windSpeedKmh"], x["pm25"], x["no2"]), (rows[29]["temp"], rows[29]["wind"], rows[29]["pm2_5"], rows[29]["no2"]))  # thời tiết/chất khí của giờ t-1
        self.assertNotEqual(x["coLag1h"], rows[i]["co"])                                          # không dùng CO của chính giờ được dự đoán

    def test_missing_history_or_gaps_give_none_not_a_guess(self):
        rows = dataset_rows()
        self.assertIsNone(mi.from_dataset(rows, 23))                                              # chưa đủ 24 giờ trước
        self.assertIsNotNone(mi.from_dataset(rows, 24))
        gap = rows[:10] + rows[11:]                                                               # thiếu một giờ nên độ trễ theo hàng sai giờ
        self.assertIsNone(mi.from_dataset(gap, 30))
        blank = dataset_rows()
        blank[29]["o3"] = None
        self.assertIsNone(mi.from_dataset(blank, 30))                                             # thiếu một chất khí của giờ trước

    def test_features_list_matches_training(self):
        import modeling
        self.assertEqual(mi.FEATURES, modeling.BASE_FEATURES)                                     # producer và huấn luyện dùng đúng một danh sách, một thứ tự


def history(n=30, start=None):
    start = start or datetime(2026, 10, 13, 10, tzinfo=VN)
    out = []
    for i in range(n):
        t = start + timedelta(hours=i)
        out.append({"dt": int(t.timestamp()), "components": {"co": 250.0 + i * 2.0, "no": 0.2 + i * 0.01, "no2": 3.0 + i * 0.1, "o3": 40.0 + i, "so2": 2.0 + i * 0.02,
                                                              "pm2_5": 7.0 + i * 0.1, "pm10": 10.0 + i * 0.1, "nh3": 1.0 + i * 0.01}})
    return out


class FromHistory(unittest.TestCase):
    W = {"temperatureC": 31.5, "windSpeedKmh": 9.0}

    def test_lags_come_from_the_hours_before_the_matching_point(self):
        h = history()
        j = 28
        x, note = mi.from_history(h, h[j]["components"], self.W)
        t = datetime.fromtimestamp(h[j]["dt"], timezone.utc).astimezone(VN)
        self.assertEqual((x["hour"], x["month"]), (t.hour, t.month))
        self.assertEqual((x["coLag1h"], x["coLag2h"], x["coLag3h"], x["coLag24h"]), tuple(h[j - k]["components"]["co"] for k in (1, 2, 3, 24)))
        self.assertEqual((x["no2"], x["pm25"], x["nh3"]), (h[j - 1]["components"]["no2"], h[j - 1]["components"]["pm2_5"], h[j - 1]["components"]["nh3"]))
        self.assertEqual((x["temperatureC"], x["windSpeedKmh"]), (31.5, 9.0))                       # thời tiết hiện tại (xấp xỉ), có ghi ở note
        self.assertIn("nhiệt độ và gió", note)

    def test_a_value_matching_a_future_point_is_flagged_as_forecast(self):
        """Đo thật 14/10/2026: lúc 16:30:56 API hiện tại trả giá trị của điểm 17:00 (dự báo). Điểm khớp sau dt của API thì ghi chú phải nói là dự báo."""
        h = history()
        j = 28
        at = h[j]["dt"]
        _, early = mi.from_history(h, h[j]["components"], self.W, observed_dt=at - 1800)       # API báo 30 phút trước điểm khớp
        self.assertIn("DỰ BÁO", early)
        x, later = mi.from_history(h, h[j]["components"], self.W, observed_dt=at + 600)        # API báo sau điểm khớp: giá trị của giờ hiện hành
        self.assertNotIn("DỰ BÁO", later)
        self.assertIsNotNone(x)
        _, unknown = mi.from_history(h, h[j]["components"], self.W)                            # không biết dt của API: không khẳng định gì
        self.assertNotIn("DỰ BÁO", unknown)

    def test_unsorted_history_and_the_newest_of_duplicate_values(self):
        h = history()
        x1, _ = mi.from_history(h, h[28]["components"], self.W)
        x2, _ = mi.from_history(list(reversed(h)), h[28]["components"], self.W)
        self.assertEqual(x1, x2)

    def test_no_match_short_history_and_gaps_give_none_with_a_reason(self):
        h = history()
        other = dict(h[28]["components"], co=999.0)
        x, note = mi.from_history(h, other, self.W)
        self.assertIsNone(x)
        self.assertIn("không có điểm lịch sử nào khớp", note)
        x, note = mi.from_history(h, h[10]["components"], self.W)                                   # quan sát khớp điểm quá sớm: chưa đủ 24 giờ trước
        self.assertIsNone(x)
        self.assertIn("cần 24", note)
        gappy = h[:20] + h[21:]                                                                      # thiếu một giờ trong 24 giờ trước
        x, note = mi.from_history(gappy, gappy[-1]["components"], self.W)
        self.assertIsNone(x)
        self.assertIn("không liên tục", note)
        x, note = mi.from_history(h, h[28]["components"], {"temperatureC": None, "windSpeedKmh": 3.0})
        self.assertIsNone(x)


@unittest.skipUnless(HAVE, "cần pyspark và requests")
class Producers(unittest.TestCase):
    def test_owm_attach_model_inputs_and_failure_is_not_fatal_or_leaky(self):
        import live_producer_owm as owm
        h = history()
        rec = {"owmDt": h[28]["dt"], "co": h[28]["components"]["co"], **{n: h[28]["components"][n] for n in owm.POLLUTANTS}, "temperature_C": 31.5, "wind_speed_kmh": 9.0}
        owm.attach_model_inputs(rec, "KEY123", history_fn=lambda key, dt, s: h)
        self.assertEqual(rec["modelInputs"]["coLag1h"], h[27]["components"]["co"])
        self.assertTrue(rec["featureNote"])

        bad = dict(rec)
        bad.pop("modelInputs"), bad.pop("featureNote")

        def boom(key, dt, s):
            raise requests.ConnectionError(f"url có appid={key}")
        owm.attach_model_inputs(bad, "SECRETKEY", history_fn=boom)
        self.assertNotIn("modelInputs", bad)                                                         # bản ghi vẫn đi tiếp, chỉ không có dự đoán
        self.assertNotIn("SECRETKEY", json.dumps(bad, ensure_ascii=False))                           # ghi chú chỉ nêu loại lỗi, không chứa khóa
        self.assertIn("ConnectionError", bad["featureNote"])

    def test_replay_message_has_inputs_only_when_history_is_complete(self):
        import replay_kafka_producer as rp
        rows = dataset_rows()
        now = datetime(2026, 10, 14, 17, 0, tzinfo=VN)
        m = rp.message_for(rows, 30, 1, now)
        self.assertEqual(m["modelInputs"], mi.from_dataset(rows, 30))
        self.assertEqual(m["originalDatetime"], rows[30]["orig"].strftime("%Y-%m-%d %H:%M:%S"))
        self.assertNotIn("modelInputs", rp.message_for(rows, 5, 2, now))                              # chưa đủ 24 giờ trước: không có trường, không đoán

    def test_replay_auto_start_falls_back_with_a_reason(self):
        import replay_kafka_producer as rp
        s, why = rp.auto_start(hdfs_bin="/không/có/hdfs")
        self.assertEqual(s, rp.FALLBACK_START)
        self.assertIn("mốc dự phòng", why)


class ReadingPrediction(unittest.TestCase):
    BASE = {"datetime": "2026-10-14 17:00:00", "station": "Tram_TPHCM_OWM", "co": 281.0, "wind_speed_kmh": 9.0, "temperature_C": 31.0, "provider": "openweather"}

    def test_prediction_and_version_pass_through_only_together(self):
        r = reading.from_record({**self.BASE, "co_predicted": 275.4, "model_version": "random-forest-20261014"})
        self.assertEqual((r["coPredicted"], r["modelVersion"]), (275.4, "random-forest-20261014"))
        self.assertEqual(schema_errors("AqiReading", r), [])
        for bad in ({"co_predicted": 275.4}, {"model_version": "v"}, {"co_predicted": "abc", "model_version": "v"}, {"co_predicted": True, "model_version": "v"},
                    {"co_predicted": None, "model_version": "v"}, {"co_predicted": 1.0, "model_version": ""}):
            r = reading.from_record({**self.BASE, **bad})
            self.assertEqual((r["coPredicted"], r["modelVersion"]), (None, None), bad)              # thiếu một nửa thì cả hai null: không bịa phiên bản hay số

    def test_reading_topic_is_what_the_backend_reads(self):
        old = config.READING_TOPIC
        self.addCleanup(lambda: setattr(config, "READING_TOPIC", old))
        config.READING_TOPIC = "hcmc_co_enriched"
        c = mock.Mock()
        c.partitions_for_topic.return_value = {0}
        parts = kafka_stream._partitions(c)
        self.assertEqual([(tp.topic, tp.partition) for tp in parts], [("hcmc_co_enriched", 0)])
        c.partitions_for_topic.assert_called_with("hcmc_co_enriched")


@unittest.skipUnless(HAVE, "cần pyspark và requests")
class SparkChain(unittest.TestCase):
    """Chuỗi đầy đủ trên Spark thật: dòng dữ liệu -> modelInputs (producer) -> enrich (consumer) phải cho đúng dự đoán của PipelineModel offline trên đặc trưng huấn luyện."""
    @classmethod
    def setUpClass(cls):
        import modeling as m
        from pyspark.sql import SparkSession, functions as F
        cls.spark = (SparkSession.builder.master("local[2]").appName("test_a5").config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.rows = dataset_rows(120)
        data = [((r["orig"] - timedelta(hours=7)).strftime("%Y-%m-%d %H:%M:%S"), r["co"], r["temp"], r["wind"], r["no"], r["no2"], r["o3"], r["so2"], r["pm2_5"], r["pm10"], r["nh3"])
                for r in cls.rows]
        df = cls.spark.createDataFrame(data, ["t", "co", "temperature_C", "wind_speed_kmh", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3"])
        df = df.withColumn("datetime_utc", F.to_timestamp("t")).withColumn("datetime_vn", F.col("datetime_utc") + F.expr("interval 7 hours"))
        cls.df = df.withColumn("hour", F.hour("datetime_vn")).withColumn("month", F.month("datetime_vn"))
        cls.feats = m.build_features(cls.df).cache()
        cls.pipe = m.fit("linear-regression", cls.feats, m.BASE_FEATURES)
        cls.tmp = tempfile.mkdtemp()
        cls.pipe.write().overwrite().save(f"file://{cls.tmp}/model")
        Path(cls.tmp, "ref.json").write_text(json.dumps({"modelVersion": "linear-regression-test", "features": mi.FEATURES}), encoding="utf-8")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_producer_inputs_equal_training_features_for_every_row(self):
        from pyspark.sql import functions as F
        got = {r["t"]: r.asDict() for r in self.feats.select(F.date_format("datetime_utc", "yyyy-MM-dd HH:mm:ss").alias("t"), *mi.FEATURES).collect()}
        checked = 0
        for i in range(24, len(self.rows)):
            key = (self.rows[i]["orig"] - timedelta(hours=7)).strftime("%Y-%m-%d %H:%M:%S")
            want = got[key]
            have = mi.from_dataset(self.rows, i)
            for f in mi.FEATURES:
                self.assertAlmostEqual(float(have[f]), float(want[f]), places=9, msg=f"{key} {f}")
            checked += 1
        self.assertEqual(checked, 96)                                                                 # mọi dòng đủ độ trễ đều khớp

    def test_enrich_equals_offline_pipeline_and_nulls_when_inputs_are_missing(self):
        import enrich as en
        from pyspark.sql import functions as F
        regressor, features, version = en.load_model(self.spark, f"file://{self.tmp}/model", f"file://{self.tmp}/ref.json")
        self.assertEqual((features, version), (mi.FEATURES, "linear-regression-test"))
        picks = [30, 50, 99]
        recs = [{"co": self.rows[i]["co"], "k": i, "modelInputs": mi.from_dataset(self.rows, i)} for i in picks]
        recs.append({"co": 111.0, "k": -1, "modelInputs": dict(mi.from_dataset(self.rows, 60), coLag24h=None)})   # thiếu một đầu vào
        recs.append({"co": 222.0, "k": -2, "modelInputs": None})                                                # không có modelInputs
        from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType
        schema = StructType([StructField("co", DoubleType()), StructField("k", IntegerType()), StructField("modelInputs", en.model_inputs_schema())])
        fl = lambda d: None if d is None else {k: (None if v is None else float(v)) for k, v in d.items()}  # noqa: E731 - cột DoubleType chỉ nhận float
        out = en.enrich(self.spark.createDataFrame([(r["co"], r["k"], fl(r["modelInputs"])) for r in recs], schema), regressor, version)
        by = {r["k"]: r.asDict(recursive=True) for r in out.collect()}
        offline = {r["t"]: r["prediction"] for r in self.pipe.transform(self.feats).select(F.date_format("datetime_utc", "yyyy-MM-dd HH:mm:ss").alias("t"), "prediction").collect()}
        for i in picks:
            key = (self.rows[i]["orig"] - timedelta(hours=7)).strftime("%Y-%m-%d %H:%M:%S")
            self.assertEqual(by[i]["co_predicted"], round(offline[key], 1))                          # đúng bằng dự đoán offline của mô hình
            self.assertEqual((by[i]["model_version"], by[i]["co_actual"]), ("linear-regression-test", self.rows[i]["co"]))
        for k in (-1, -2):
            self.assertEqual((by[k]["co_predicted"], by[k]["model_version"]), (None, None))         # thiếu đầu vào: null, không điền 0
        self.assertEqual((by[-1]["co_actual"], by[-2]["co_actual"]), (111.0, 222.0))
        self.assertIn("model_inputs", out.columns)
        self.assertNotIn("modelInputs", out.columns)

    def test_load_model_reports_why_it_cannot_be_used(self):
        import enrich as en
        r, f, why = en.load_model(self.spark, f"file://{self.tmp}/khong-co", f"file://{self.tmp}/ref.json")
        self.assertEqual((r, f), (None, None))
        self.assertTrue(why)
        Path(self.tmp, "ref_bad.json").write_text(json.dumps({"modelVersion": "x", "features": ["hour", "month"]}), encoding="utf-8")
        r, f, why = en.load_model(self.spark, f"file://{self.tmp}/model", f"file://{self.tmp}/ref_bad.json")
        self.assertIsNone(r)
        self.assertIn("khác model_inputs.FEATURES", why)                                              # danh sách đặc trưng lệch: không dùng mô hình

    def test_without_model_keeps_the_same_columns_with_null_prediction(self):
        import enrich as en
        from pyspark.sql.types import DoubleType, StructField, StructType
        schema = StructType([StructField("co", DoubleType()), StructField("modelInputs", en.model_inputs_schema())])
        out = en.without_model(self.spark.createDataFrame([(5.0, None)], schema)).first().asDict()
        self.assertEqual((out["co_actual"], out["co_predicted"], out["model_version"]), (5.0, None, None))


if __name__ == "__main__":
    unittest.main()
