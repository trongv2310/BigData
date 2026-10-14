"""Bước A3: phép tính Spark SQL (pipeline/analysis.py) và phân cụm ngày (pipeline/day_clusters.py) trên dữ liệu tổng hợp có đáp án biết trước (Spark thật).
Chạy: cd backend && python3 -m unittest discover tests"""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "pipeline"))

try:
    import pyspark  # noqa: F401
    HAVE_SPARK = True
except ImportError:
    HAVE_SPARK = False


@unittest.skipUnless(HAVE_SPARK, "không có pyspark")
class SparkCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master("local[1]").appName("test_a3").config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, rows):
        """rows: [(datetime_vn 'yyyy-MM-dd HH:mm:ss', co, extras dict)] -> DataFrame đúng cột của đầu ra 04_clean (chuỗi → timestamp trong Spark, tránh đổi múi giờ của Python)."""
        import analysis
        from pyspark.sql import functions as F
        cols = ["datetime_vn", "co", *analysis.CORR_VARIABLES]
        data = [(t, float(co), *[(float(e[v]) if v in e else None) for v in analysis.CORR_VARIABLES]) for t, co, e in rows]
        from pyspark.sql.types import DoubleType, StringType, StructField, StructType
        schema = StructType([StructField(cols[0], StringType()), *[StructField(c, DoubleType(), True) for c in cols[1:]]])  # cột toàn None vẫn có kiểu
        df = self.spark.createDataFrame(data, schema).withColumn("datetime_vn", F.to_timestamp("datetime_vn"))
        return (df.withColumn("year", F.year("datetime_vn")).withColumn("month", F.month("datetime_vn")).withColumn("hour", F.hour("datetime_vn"))
                .withColumn("dayofweek", (F.dayofweek("datetime_vn") + 5) % 7)
                .withColumn("after_2025_07_01", (F.col("datetime_vn") >= F.lit("2025-07-01 00:00:00").cast("timestamp")).cast("int")))


class Analysis(SparkCase):
    def test_periods_hourly_and_monthly_numbers(self):
        import analysis
        rows = [("2025-06-30 10:00:00", 1000, {}), ("2025-06-30 22:00:00", 3000, {}),   # trước điểm gãy
                ("2025-07-01 00:00:00", 100, {}), ("2025-07-01 10:00:00", 300, {})]      # từ 00:00 ngày 01/07 (giờ +07) là sau
        res = analysis.compute(self.spark, self.frame(rows))
        p = res["periods"]
        self.assertEqual((p["before"]["n"], p["before"]["mean"], p["before"]["min"], p["before"]["max"]), (2, 2000.0, 1000.0, 3000.0))
        self.assertEqual((p["after"]["n"], p["after"]["mean"]), (2, 200.0))
        self.assertEqual(p["meanChangePct"], -90.0)
        by_hour = {h["hour"]: h for h in res["hourly"]["all"]}
        self.assertEqual((by_hour[10]["n"], by_hour[10]["mean"]), (2, 650.0))               # (1000 + 300) / 2
        self.assertEqual([h["hour"] for h in res["hourly"]["before"]], [10, 22])
        self.assertEqual([(m["year"], m["month"], m["n"], m["mean"]) for m in res["monthly"]], [(2025, 6, 2, 2000.0), (2025, 7, 2, 200.0)])
        self.assertEqual(res["rows"], 4)

    def test_correlation_is_reported_overall_and_within_each_period(self):
        """Trước điểm gãy tương quan âm hoàn toàn, sau điểm gãy dương hoàn toàn (trái dấu để đảo nhầm giai đoạn bị lộ); hai giai đoạn lại khác mức nên tương quan chung dương:
        lý do phải báo riêng từng giai đoạn."""
        import analysis
        rows = [("2025-06-01 00:00:00", 1000, {"pm10": 30}), ("2025-06-01 01:00:00", 1100, {"pm10": 20}), ("2025-06-01 02:00:00", 1200, {"pm10": 10}),
                ("2025-08-01 00:00:00", 100, {"pm10": 1}), ("2025-08-01 01:00:00", 200, {"pm10": 2}), ("2025-08-01 02:00:00", 300, {"pm10": 3})]
        corr = analysis.compute(self.spark, self.frame(rows))["correlation"]
        self.assertEqual((corr["before"]["pm10"], corr["after"]["pm10"]), (-1.0, 1.0))
        self.assertGreater(corr["all"]["pm10"], 0.5)
        self.assertIsNone(corr["all"]["wind_speed_kmh"])                                      # cột không có dữ liệu: None, không bịa 0
        self.assertEqual(analysis.strongest(corr, "before", 1)[0][0], "pm10")

    def test_like_for_like_months_and_daily_series_around_break(self):
        import analysis
        rows = [("2024-08-10 12:00:00", 2000, {}), ("2024-08-11 12:00:00", 2000, {}), ("2025-02-10 12:00:00", 9999, {}),   # tháng 08 trước; tháng 02 chỉ có "trước"
                ("2025-08-10 12:00:00", 500, {}), ("2025-04-10 12:00:00", 777, {}),                                          # tháng 04 không nằm trong 07..03
                ("2025-06-30 12:00:00", 800, {}), ("2025-07-02 12:00:00", 400, {})]
        res = analysis.compute(self.spark, self.frame(rows))
        by = {m["month"]: m for m in res["seasonalByMonth"]}
        self.assertEqual((by[8]["before"]["mean"], by[8]["after"]["mean"], by[8]["changePct"]), (2000.0, 500.0, -75.0))
        self.assertEqual((by[2]["before"]["mean"], by[2]["after"], by[2]["changePct"]), (9999.0, None, None))                # thiếu bên "sau": không bịa
        like = res["seasonalLikeForLike"]
        self.assertNotIn(777.0, [like["before"]["mean"], like["after"]["mean"]])
        self.assertEqual(like["months"], [7, 8, 9, 10, 11, 12, 1, 2, 3])
        around = {d["date"]: d["mean"] for d in res["dailyAroundBreak"]}
        self.assertEqual((around["2025-06-30"], around["2025-07-02"]), (800.0, 400.0))
        self.assertNotIn("2024-08-10", around)


def day_rows(day: str, shape, level: float):
    """24 dòng giờ của một ngày: CO = level * shape[h]."""
    return [(f"{day} {h:02d}:00:00", level * shape[h], {}) for h in range(24)]


class DayClusters(SparkCase):
    A = [1.0 + (3.0 if h == 8 else 0.0) for h in range(24)]    # đỉnh lúc 8 giờ
    B = [1.0 + (3.0 if h == 20 else 0.0) for h in range(24)]   # đỉnh lúc 20 giờ

    def build(self):
        rows = []
        for i in range(20):  # mỗi hình dạng 20 ngày, mức CO lệch nhau tới 6 lần
            rows += day_rows(f"2025-01-{i + 1:02d}", self.A, 100 + 100 * (i % 6))
            rows += day_rows(f"2025-02-{i + 1:02d}", self.B, 100 + 100 * (i % 6))
        rows += [("2025-03-01 07:00:00", 500, {}), ("2025-03-01 08:00:00", 500, {})]  # ngày thiếu giờ
        return self.frame(rows)

    def test_incomplete_day_is_excluded_and_shape_not_level_decides_clusters(self):
        import day_clusters as dc
        days, excluded, total = dc.day_table(self.spark, self.build())
        self.assertEqual((days.count(), excluded, total), (40, 1, 41))
        table, k, model, assigned = dc.select_k(self.spark, days, 2, 4)
        self.assertEqual(k, 2)                                                              # đúng hai hình dạng, dù mức CO khác nhau tới 6 lần
        self.assertEqual([t["k"] for t in table], [2, 3, 4])
        self.assertGreater(table[0]["silhouette"], 0.9)
        labels = {r["date"]: r["cluster"] for r in assigned.select("date", "cluster").collect()}
        a = {labels[f"2025-01-{i + 1:02d}"] for i in range(20)}
        b = {labels[f"2025-02-{i + 1:02d}"] for i in range(20)}
        self.assertEqual((len(a), len(b), a & b), (1, 1, set()))                             # mỗi hình dạng đúng một cụm, hai cụm khác nhau

        stats = dc.cluster_stats(assigned, model.clusterCenters(), k)
        peaks = sorted(s["peakHour"] for s in stats)
        self.assertEqual(peaks, [8, 20])
        self.assertEqual(sorted(s["days"] for s in stats), [20, 20])
        self.assertAlmostEqual(stats[0]["peakToTrough"], 4.0, places=1)                      # đỉnh 4.0 so với đáy 1.0 (hình dạng đã chuẩn hóa: tỷ lệ giữ nguyên)

    def test_silhouette_labels(self):
        import day_clusters as dc
        self.assertEqual([dc.structure_label(x) for x in (0.8, 0.5, 0.4452, 0.25, 0.1)], ["rõ", "yếu", "yếu", "yếu", "gần như không có cấu trúc cụm"])


if __name__ == "__main__":
    unittest.main()
