"""Bước A4: xác định chuyển tiếp theo dữ liệu (changepoint.py), đặc trưng/chia/mốc/mô hình (modeling.py, Spark thật, dữ liệu tổng hợp có đáp án biết trước),
lời kết luận của 06b_train.py và route /api/model/metrics.   Chạy: cd backend && python3 -m unittest discover tests"""
import importlib
import json
import math
import sys
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(ROOT / "pipeline"))

from fastapi.testclient import TestClient

from app import config
from app.main import create_app
from helpers import FakeNameNode, schema_errors

MOCK = ROOT / "mock"

try:
    import numpy as np
    import pyspark  # noqa: F401
    HAVE = True
except ImportError:
    HAVE = False


def synthetic_days(ramp_from=date(2025, 6, 15), ramp_days=10, drop=-1.5):
    """Chuỗi CO ngày 02/04/2022 đến 31/03/2026: mùa vụ hình sin (biên độ 0,6 trên log) + nhiễu tất định nhỏ; từ ramp_from CO giảm dần `drop` (log) trong ramp_days ngày rồi giữ mức thấp."""
    days, means = [], []
    d = date(2022, 4, 2)
    rng = np.random.RandomState(7)
    while d <= date(2026, 3, 31):
        base = 7.0 + 0.6 * math.sin(2 * math.pi * d.timetuple().tm_yday / 365.25)
        shift = 0.0 if d < ramp_from else drop * min(1.0, (d - ramp_from).days / ramp_days)
        days.append(d)
        means.append(math.exp(base + shift + float(rng.normal(0, 0.08))))
        d += timedelta(days=1)
    return days, means


@unittest.skipUnless(HAVE, "cần numpy và pyspark")
class ChangePoint(unittest.TestCase):
    def test_finds_a_known_ramp_despite_seasonality(self):
        import changepoint as cp
        days, means = synthetic_days()
        res = cp.detect(days, means)
        step = date.fromisoformat(res["stepDate"])
        self.assertLessEqual(abs((step - date(2025, 6, 20)).days), 12)                      # bước đơn nằm quanh giữa đoạn dốc
        main = res["main"]
        self.assertLessEqual(abs((date.fromisoformat(main["from"]) - date(2025, 6, 15)).days), 10)
        self.assertLessEqual(abs((date.fromisoformat(main["to"]) - date(2025, 6, 25)).days), 12)
        self.assertEqual([w["halfWidthDays"] for w in res["windows"]], [45, 60, 90])
        self.assertLess(res["stepResidualAfter"], res["stepResidualBefore"] - 1.0)           # mức sau thấp hơn mức trước rõ rệt
        self.assertTrue(all(r["month"] >= "2025-01" for r in res["monthlyResidual"]))        # phần dư tháng chỉ từ sau các năm tham chiếu

    def test_a_pure_seasonal_dip_is_not_mistaken_for_the_break(self):
        """Chuỗi chỉ có mùa vụ mạnh (không có điểm gãy): phần dư sau khử mùa gần như phẳng, nên chênh lệch hai mức của bước đơn nhỏ."""
        import changepoint as cp
        days, means = synthetic_days(drop=0.0)
        res = cp.detect(days, means)
        self.assertLess(abs(res["stepResidualAfter"] - res["stepResidualBefore"]), 0.2)

    def test_season_profile_and_input_validation(self):
        import changepoint as cp
        days, means = synthetic_days()
        s = cp.season_profile(days, np.log(np.array(means)), date(2025, 1, 1))
        self.assertFalse(np.isnan(s[1:367]).any())                                          # đủ 366 vị trí trong năm
        with self.assertRaises(ValueError):
            cp.detect(days[:100], means[:100])
        with self.assertRaises(ValueError):
            cp.season_profile(days, np.log(np.array(means)), date(2020, 1, 1))              # không có ngày tham chiếu


def hourly_frame(spark, hours=700, gap_at=None):
    """DataFrame giống đầu ra 04_clean. CO = hồ sơ theo giờ + một độ lệch dai dẳng AR(1, 0,95) (nên CO giờ trước có giá trị dự đoán mà trung bình theo giờ không có);
    datetime_utc tăng 1 giờ/dòng (có thể cắt một giờ để thử phát hiện khoảng trống)."""
    from pyspark.sql import functions as F
    start = datetime(2025, 8, 1, 0, 0)
    rng = np.random.RandomState(3)
    rows, dev = [], 0.0
    for i in range(hours):
        t = start + timedelta(hours=i + (1 if gap_at is not None and i >= gap_at else 0))
        hour_vn = (t.hour + 7) % 24
        dev = 0.95 * dev + float(rng.normal(0, 20))
        co = 300 + 80 * math.sin(2 * math.pi * hour_vn / 24) + dev
        rows.append((t.strftime("%Y-%m-%d %H:%M:%S"), round(co, 4), 30.0 + (i % 5), 5.0 + (i % 3), 1.0 + (i % 7), 2.0 + (i % 4), 3.0 + (i % 6), 4.0 + (i % 3), 5.0 + (i % 5), 6.0 + (i % 2), 7.0 + (i % 9)))
    cols = ["t", "co", "temperature_C", "wind_speed_kmh", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3"]
    df = spark.createDataFrame(rows, cols)
    df = df.withColumn("datetime_utc", F.to_timestamp("t")).drop("t")
    df = df.withColumn("datetime_vn", F.col("datetime_utc") + F.expr("interval 7 hours"))
    return df.withColumn("hour", F.hour("datetime_vn")).withColumn("month", F.month("datetime_vn"))


@unittest.skipUnless(HAVE, "cần numpy và pyspark")
class Modeling(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master("local[2]").appName("test_a4").config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_features_use_only_previous_hours_and_drop_the_first_24(self):
        import modeling as m
        df = hourly_frame(self.spark, 100)
        f = m.build_features(df)
        self.assertEqual(f.count(), 100 - 24)
        src = {r["datetime_utc"]: r.asDict() for r in df.collect()}
        row = f.orderBy("datetime_utc").collect()[10].asDict()
        t = row["datetime_utc"]
        at = lambda h: src[t - timedelta(hours=h)]  # noqa: E731
        self.assertEqual((row["coLag1h"], row["coLag2h"], row["coLag3h"], row["coLag24h"]), (at(1)["co"], at(2)["co"], at(3)["co"], at(24)["co"]))
        self.assertEqual((row["temperatureC"], row["windSpeedKmh"], row["pm25"]), (at(1)["temperature_C"], at(1)["wind_speed_kmh"], at(1)["pm2_5"]))  # chất khác lấy của giờ t-1, không phải giờ t
        self.assertNotEqual(row["co"], row["coLag1h"])
        self.assertEqual(m.check_hourly_continuity(df), 0)
        self.assertEqual(m.check_hourly_continuity(hourly_frame(self.spark, 100, gap_at=50)), 1)  # thiếu một giờ thì báo

    def test_time_split_is_chronological_with_no_overlap(self):
        import modeling as m
        f = m.build_features(hourly_frame(self.spark, 224))
        train, test, cut = m.time_split(f)
        self.assertEqual((train.count(), test.count(), cut), (160, 40, 160))
        from pyspark.sql import functions as F
        self.assertLess(train.agg(F.max("datetime_utc")).first()[0], test.agg(F.min("datetime_utc")).first()[0])

    def test_baselines_on_known_series(self):
        import modeling as m
        f = m.build_features(hourly_frame(self.spark, 224))
        train, test, _ = m.time_split(f)
        b = m.baselines(train, test)
        rows = test.collect()
        want = math.sqrt(sum((r["co"] - r["coLag1h"]) ** 2 for r in rows) / len(rows))
        self.assertAlmostEqual(b["previous-hour"]["rmse"], want, places=6)                  # RMSE của "CO giờ trước" tính tay khớp
        by_hour = {}
        for r in train.collect():
            by_hour.setdefault(r["hour"], []).append(r["co"])
        want_hm = math.sqrt(sum((r["co"] - sum(by_hour[r["hour"]]) / len(by_hour[r["hour"]])) ** 2 for r in rows if r["hour"] in by_hour) / len([r for r in rows if r["hour"] in by_hour]))
        self.assertAlmostEqual(b["hourly-mean"]["rmse"], want_hm, places=6)                # mốc trung bình theo giờ chỉ dùng tập huấn luyện

    def test_run_setup_fits_all_models_and_test_is_never_used_for_selection(self):
        import modeling as m
        f = m.build_features(hourly_frame(self.spark, 524))
        train, test, _ = m.time_split(f)
        res = m.run_setup(train, test, m.BASE_FEATURES)
        self.assertEqual(set(res["models"]), set(m.KINDS))
        self.assertEqual((res["trainRows"], res["testRows"]), (train.count(), test.count()))
        self.assertEqual(res["best"], min(m.KINDS, key=lambda k: res["models"][k]["val"]))    # chọn theo kiểm định
        for k in m.KINDS:
            self.assertGreater(res["models"][k]["test"]["r2"], 0.5)                         # CO phụ thuộc mạnh vào CO giờ trước: mô hình phải học được
            self.assertIn(k, res["fitted"])
        self.assertLess(res["models"]["linear-regression"]["test"]["rmse"], res["baselines"]["hourly-mean"]["rmse"])
        out = res["fitted"][res["best"]].transform(test)
        self.assertEqual(out.count(), test.count())
        self.assertIn("prediction", out.columns)


class Report(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.t = importlib.import_module("06b_train")

    def res(self, test_rmse, prev=50.0, hm=200.0):
        r = lambda x: {"rmse": x, "mae": x / 2, "r2": 0.9}  # noqa: E731
        return {"models": {"linear-regression": {"val": 30, "test": r(test_rmse[0])}, "random-forest": {"val": 20, "test": r(test_rmse[1])}, "gbt": {"val": 25, "test": r(test_rmse[2])}},
                "best": "random-forest", "baselines": {"previous-hour": r(prev), "hourly-mean": r(hm)}}

    def test_conclusion_says_so_when_no_model_beats_the_baseline(self):
        text = self.t.conclusion_text(self.res((60, 55, 58)), 100)
        self.assertIn("KHÔNG thắng", text)
        self.assertIn("không có mô hình nào", text)
        self.assertIn("báo đúng như vậy", text)

    def test_conclusion_when_models_win_names_the_winners(self):
        text = self.t.conclusion_text(self.res((45, 40, 55)), 100)
        self.assertIn("THẮNG (-20.0%)", text)
        self.assertIn("Hồi quy tuyến tính", text)
        self.assertNotIn("GBT", text.split("Mô hình có RMSE thấp hơn mốc CO giờ trước:")[1])  # GBT (55) không thắng mốc 50

    def test_models_json_flags_follow_the_numbers_and_one_best(self):
        out = self.t.models_json(self.res((45, 40, 55)), lambda k: f"{k}-v")
        self.assertEqual([x["isBest"] for x in out], [False, True, False])
        self.assertEqual([(x["beatsPreviousHour"], x["beatsHourlyMean"]) for x in out], [(True, True), (True, True), (False, True)])
        self.assertEqual(self.t.iso("2025-07-12 00:00:00"), "2025-07-12T00:00:00+07:00")


class ModelRoute(unittest.TestCase):
    def setUp(self):
        self._old = config.NAMENODE_HTTP
        self.addCleanup(lambda: setattr(config, "NAMENODE_HTTP", self._old))
        self.client = TestClient(create_app())
        self.addCleanup(self.client.close)
        self.real = json.loads((MOCK / "model-metrics.json").read_text(encoding="utf-8"))

    def serve(self, files):
        nn = FakeNameNode(files={k: (v if isinstance(v, str) else json.dumps(v)) for k, v in files.items()})
        self.addCleanup(nn.close)
        config.NAMENODE_HTTP = nn.url

    def get(self, status=200):
        r = self.client.get("/api/model/metrics")
        self.assertEqual(r.status_code, status, r.text)
        self.assertEqual(schema_errors("ModelMetrics" if status == 200 else "Error", r.json()), [], r.text)
        return r.json()

    def test_returns_the_file_verbatim(self):
        self.serve({config.MODEL_METRICS_PATH: self.real})
        b = self.get()
        self.assertEqual(b, self.real)
        self.assertIn("CHƯA XÁC ĐỊNH", b["breakpoint"]["cause"])
        self.assertEqual(sum(1 for m in b["models"] if m["isBest"]), 1)

    def test_not_trained_and_unavailable(self):
        self.serve({})
        self.assertEqual(self.get(404)["error"]["code"], "MODEL_NOT_TRAINED")
        config.NAMENODE_HTTP = "http://127.0.0.1:1"
        self.assertEqual(self.get(503)["error"]["code"], "MODEL_UNAVAILABLE")
        self.serve({config.MODEL_METRICS_PATH: "{hỏng"})
        self.assertEqual(self.get(503)["error"]["code"], "MODEL_UNAVAILABLE")
        self.serve({config.MODEL_METRICS_PATH: "[1]"})
        self.assertEqual(self.get(503)["error"]["code"], "MODEL_UNAVAILABLE")


if __name__ == "__main__":
    unittest.main()


@unittest.skipUnless(HAVE, "cần numpy và pyspark")
class MlPredict(unittest.TestCase):
    """/api/predict dùng mô hình Spark thật (huấn luyện trên dữ liệu tổng hợp, lưu ở thư mục tạm) qua NameNode giả cho tệp mô tả."""
    @classmethod
    def setUpClass(cls):
        import tempfile
        import modeling as m
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master("local[2]").appName("test_a4_ml").config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        feats = m.build_features(hourly_frame(cls.spark, 400))
        cls.model = m.fit("linear-regression", feats, m.BASE_FEATURES)
        cls.tmp = tempfile.mkdtemp()
        cls.model.write().overwrite().save(f"file://{cls.tmp}/model")
        cls.medians = {k: round(float(v), 4) for k, v in feats.agg(*[__import__("pyspark.sql.functions", fromlist=["x"]).percentile_approx(f, 0.5).alias(f) for f in m.BASE_FEATURES]).first().asDict().items()}

    @classmethod
    def tearDownClass(cls):
        from app import ml_engine
        ml_engine.stop()
        cls.spark.stop()

    def setUp(self):
        from app import ml_engine
        ml_engine._spark = None
        ml_engine._cache.clear()
        self._old = (config.NAMENODE_HTTP, config.MODEL_DIR_URI)
        self.addCleanup(lambda: (setattr(config, "NAMENODE_HTTP", self._old[0]), setattr(config, "MODEL_DIR_URI", self._old[1])))
        import modeling as m
        self.ref = {"modelVersion": "linear-regression-test", "features": m.BASE_FEATURES, "median": self.medians}
        metrics = json.loads((MOCK / "model-metrics.json").read_text(encoding="utf-8"))
        for x in metrics["models"]:
            x["version"] = x["kind"] + "-test"
        self.metrics = metrics
        self.metrics["models"] = [x for x in self.metrics["models"] if x["kind"] == "linear-regression"]
        self.metrics["models"][0]["isBest"] = True
        self.client = TestClient(create_app())
        self.addCleanup(self.client.close)

    def serve(self, ref=True, metrics=True):
        files = {}
        if ref:
            files[config.MODEL_REFERENCE_PATH] = json.dumps(self.ref)
        if metrics:
            files[config.MODEL_METRICS_PATH] = json.dumps(self.metrics)
        nn = FakeNameNode(files=files)
        self.addCleanup(nn.close)
        config.NAMENODE_HTTP = nn.url
        config.MODEL_DIR_URI = f"file://{self.tmp}/model"

    BODY = {"hour": 9, "month": 11, "temperatureC": 29.5, "windSpeedKmh": 6.0, "coLag1h": 240.0}

    def test_ml_prediction_matches_the_spark_model_and_reports_filled_inputs(self):
        import modeling as m
        self.serve()
        r = self.client.post("/api/predict", json=self.BODY)
        self.assertEqual(r.status_code, 200, r.text)
        b = r.json()
        self.assertEqual(schema_errors("PredictResponse", b), [], r.text)
        self.assertEqual((b["model"]["kind"], b["model"]["version"], b["interval"]), ("ml", "linear-regression-test", None))
        values = {**self.medians, "hour": 9, "month": 11, "temperatureC": 29.5, "windSpeedKmh": 6.0, "coLag1h": 240.0}
        row = self.spark.createDataFrame([tuple(float(values[k]) for k in m.BASE_FEATURES)], m.BASE_FEATURES)
        self.assertEqual(b["coPredicted"], round(float(self.model.transform(row).first()["prediction"]), 1))   # đúng bằng dự đoán trực tiếp của mô hình
        given = {"hour", "month", "temperatureC", "windSpeedKmh", "coLag1h"}
        self.assertEqual({f["name"] for f in b["filledInputs"]}, set(m.BASE_FEATURES) - given)                  # liệt kê đủ và đúng những gì được điền
        self.assertTrue(all(f["value"] == self.medians[f["name"]] for f in b["filledInputs"]))
        self.assertIn("trung vị", b["filledInputs"][0]["origin"])

    def test_explicit_inputs_change_the_prediction_and_are_not_reported_as_filled(self):
        self.serve()
        base = self.client.post("/api/predict", json=self.BODY).json()
        full = self.client.post("/api/predict", json={**self.BODY, "coLag1h": 900.0, "coLag2h": 900.0, "coLag3h": 900.0, "coLag24h": 900.0}).json()
        self.assertGreater(full["coPredicted"], base["coPredicted"])                                           # CO các giờ trước cao hơn thì dự đoán cao hơn
        self.assertNotIn("coLag2h", {f["name"] for f in full["filledInputs"]})

    def test_no_model_or_mismatched_files_means_no_ml(self):
        import asyncio
        from app import ml_predict
        from app.clients import hdfs
        from app.routes.predict import PredictRequest
        req = PredictRequest(**self.BODY)

        async def run():
            try:
                return await ml_predict.try_predict(req)
            finally:
                await hdfs.aclose()  # client HTTP dùng chung gắn với vòng lặp sự kiện này: đóng để test sau không dùng nhầm vòng lặp đã đóng

        self.serve(ref=False)
        self.assertIsNone(asyncio.run(run()))                                                                  # chưa có tệp tham chiếu
        self.ref["modelVersion"] = "khac-phien-ban"
        self.serve()
        self.assertIsNone(asyncio.run(run()))                                                                  # hai tệp không cùng một lần huấn luyện: không đoán

    def test_model_present_but_unloadable_is_503_not_a_silent_fallback(self):
        self.serve()
        config.MODEL_DIR_URI = f"file://{self.tmp}/khong-co"
        r = self.client.post("/api/predict", json=self.BODY)
        self.assertEqual((r.status_code, r.json()["error"]["code"]), (503, "PREDICT_UNAVAILABLE"))
        self.assertEqual(schema_errors("Error", r.json()), [])
