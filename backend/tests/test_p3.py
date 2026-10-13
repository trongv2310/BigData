"""P3: /api/aqi/*. Dùng CSV nhỏ tự dựng (kiểm phép tính) và, nếu có, file Kaggle thật (kiểm giả định về dữ liệu)."""
import asyncio
import csv
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient

from app import aqi_data, config
from app.clients import hdfs
from app.main import create_app
from helpers import FakeNameNode, schema_errors

COLS = ["datetime", "aqi_label", "co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3", "temperature_C", "humidity_%", "rain_mm",
        "wind_speed_kmh", "is_event_anomaly", "hour", "month", "dayofweek", "o3_was_missing"]


def write_csv(path: Path, start_utc: datetime, hours: int, co_of):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, COLS)
        w.writeheader()
        for i in range(hours):
            t = start_utc + timedelta(hours=i)
            row = {c: 1 for c in COLS}
            row.update(datetime=f"{t.day:02d}-{t.month:02d}-{t:%y} {t.hour}:00", co=co_of(t), wind_speed_kmh=9, temperature_C=30, pm2_5=5)
            w.writerow(row)


class Base(unittest.TestCase):
    def use_csv(self, path):
        self._old = config.AQI_CSV_PATH
        config.AQI_CSV_PATH = str(path)
        aqi_data._cache = None
        self.addCleanup(self._restore)
        self.client = TestClient(create_app())
        self.addCleanup(self.client.close)

    def _restore(self):
        config.AQI_CSV_PATH = self._old
        aqi_data._cache = None

    def get(self, url, schema, status=200):
        r = self.client.get(url)
        self.assertEqual(r.status_code, status, r.text)
        self.assertEqual(schema_errors(schema, r.json()), [], r.text)
        return r.json()


class Computation(Base):
    def setUp(self):
        d = Path(tempfile.mkdtemp())
        # UTC 2025-06-30 12:00 .. 2025-07-02 12:00. CO = 100 trước 2025-07-01 00:00 giờ VN (tức 17:00 UTC 30/6), 10 sau đó.
        cut_utc = datetime(2025, 6, 30, 17)
        write_csv(d / "a.csv", datetime(2025, 6, 30, 12), 49, lambda t: 100 if t < cut_utc else 10)
        self.use_csv(d / "a.csv")

    def test_times_are_shifted_to_vietnam(self):
        ds = asyncio.run(aqi_data.get())
        self.assertEqual(ds.times[0].isoformat(), "2025-06-30T19:00:00+07:00")  # 12:00 UTC
        self.assertEqual(ds.times[-1].isoformat(), "2025-07-02T19:00:00+07:00")

    def test_hour_series_has_every_row(self):
        b = self.get("/api/aqi/series?granularity=hour", "AqiSeries")
        self.assertEqual(len(b["points"]), 49)
        self.assertEqual(b["points"][0], {"t": "2025-06-30T19:00:00+07:00", "value": 100.0})

    def test_day_series_drops_edge_days_with_little_coverage(self):
        # 30/6: 19h-23h = 5 dòng (<12) -> bỏ; 1/7: đủ 24 dòng; 2/7: 0h-19h = 20 dòng (>=12) -> giữ
        b = self.get("/api/aqi/series?granularity=day", "AqiSeries")
        self.assertEqual([p["t"] for p in b["points"]], ["2025-07-01T00:00:00+07:00", "2025-07-02T00:00:00+07:00"])
        self.assertEqual([p["value"] for p in b["points"]], [10.0, 10.0])

    def test_range_filter_uses_bucket_start(self):
        b = self.get("/api/aqi/series?granularity=hour&from=2025-07-01T00:00:00%2B07:00&to=2025-07-01T02:00:00%2B07:00", "AqiSeries")
        self.assertEqual([p["t"][11:16] for p in b["points"]], ["00:00", "01:00", "02:00"])
        self.get("/api/aqi/series?granularity=hour&from=2025-07-01&to=2025-07-01", "AqiSeries")  # ngày trần = giờ VN

    def test_other_metric(self):
        b = self.get("/api/aqi/series?metric=pm2_5&granularity=month", "AqiSeries")
        self.assertEqual(b["metric"], "pm2_5")

    def test_profile_with_missing_hours_is_503_not_crash(self):
        self.assertEqual(self.get("/api/aqi/hourly-profile", "Error", 503)["error"]["code"], "DATASET_INCOMPLETE")

    def test_bad_params_are_400_with_error_schema(self):
        for u in ("/api/aqi/series?metric=xyz", "/api/aqi/series?granularity=year", "/api/aqi/series?from=hello",
                  "/api/aqi/series?from=2026-01-01&to=2025-01-01"):
            self.assertEqual(self.get(u, "Error", 400)["error"]["code"], "BAD_REQUEST")


class Profile(Base):
    def setUp(self):
        d = Path(tempfile.mkdtemp())
        # giờ VN 28/6 00:00 .. 3/7 23:00 (144 giờ); UTC bắt đầu 27/6 17:00. Điểm gãy 1/7 00:00 VN = 30/6 17:00 UTC.
        write_csv(d / "b.csv", datetime(2025, 6, 27, 17), 144, lambda t: 100 if t < datetime(2025, 6, 30, 17) else 10)
        self.use_csv(d / "b.csv")

    def test_splits_at_break_in_local_time(self):
        b = self.get("/api/aqi/hourly-profile", "HourlyProfile")
        pre, post = b["profiles"]
        self.assertEqual((pre["from"], pre["to"]), ("2025-06-28T00:00:00+07:00", "2025-06-30T23:00:00+07:00"))
        self.assertEqual((post["from"], post["to"]), ("2025-07-01T00:00:00+07:00", "2025-07-03T23:00:00+07:00"))
        self.assertEqual(set(pre["values"]), {100.0})
        self.assertEqual(set(post["values"]), {10.0})
        self.assertIn("UTC", b["note"])


class Latest(Base):
    def setUp(self):
        d = Path(tempfile.mkdtemp())
        write_csv(d / "a.csv", datetime(2025, 6, 30, 12), 3, lambda t: 1500)
        self.use_csv(d / "a.csv")
        self._nn = config.NAMENODE_HTTP

    def tearDown(self):
        config.NAMENODE_HTTP = self._nn

    def test_falls_back_to_last_dataset_row_when_hdfs_down(self):
        config.NAMENODE_HTTP = "http://127.0.0.1:1"
        b = self.get("/api/aqi/latest", "AqiReading")
        self.assertEqual((b["source"], b["status"], b["isSimulatedStation"], b["datetime"]), ("replay-dataset", "SAFE", True, "2025-06-30T21:00:00+07:00"))

    def test_falls_back_when_stream_dir_empty(self):
        nn = FakeNameNode(files={"/stream_input": []})
        config.NAMENODE_HTTP = nn.url
        try:
            self.assertEqual(self.get("/api/aqi/latest", "AqiReading")["datetime"], "2025-06-30T21:00:00+07:00")
        finally:
            nn.close()

    def test_prefers_stream_record(self):
        rec = {"datetime": "2026-10-12 15:30:03", "station": "Tram_TPHCM_Live_API", "co": 210.0, "wind_speed_kmh": 9.0, "temperature_C": 30.0}
        nn = FakeNameNode(files={
            "/stream_input": [{"pathSuffix": "live_1.json", "type": "FILE", "modificationTime": 5, "length": 1, "blockSize": 1, "replication": 1, "owner": "t"}],
            "/stream_input/live_1.json": json.dumps(rec),
        })
        config.NAMENODE_HTTP = nn.url
        try:
            b = self.get("/api/aqi/latest", "AqiReading")
        finally:
            nn.close()
        self.assertEqual((b["source"], b["co"], b["datetime"]), ("live-open-meteo", 210.0, "2026-10-12T15:30:03+07:00"))

    def test_dataset_missing_is_503(self):
        config.AQI_CSV_PATH = "/không/có/file.csv"
        aqi_data._cache = None
        config.NAMENODE_HTTP = "http://127.0.0.1:1"
        self.assertEqual(self.get("/api/aqi/series", "Error", 503)["error"]["code"], "DATASET_UNAVAILABLE")


@unittest.skipUnless(Path(config.AQI_CSV_PATH).is_file(), "không có file Kaggle trên máy này")
class RealKaggleFile(Base):
    def setUp(self):
        aqi_data._cache = None
        self.client = TestClient(create_app())
        self.addCleanup(self.client.close)

    def test_assumptions_about_the_file(self):
        ds = asyncio.run(aqi_data.get())
        self.assertEqual(len(ds.times), 35064)
        # liên tục từng giờ, không trùng, không thiếu
        self.assertTrue(all(b - a == timedelta(hours=1) for a, b in zip(ds.times, ds.times[1:])))
        self.assertEqual(ds.times[0].isoformat(), "2022-04-01T07:00:00+07:00")  # dòng đầu 01-04-22 0:00 UTC
        # các cột hour/month/dayofweek của file khớp giờ UTC (không phải giờ VN) -> không được dùng chúng để nhóm
        with open(config.AQI_CSV_PATH, encoding="utf-8-sig") as f:
            for row, t in list(zip(csv.DictReader(f), ds.times))[:2000]:
                utc = t - timedelta(hours=7)
                self.assertEqual((int(row["hour"]), int(row["month"])), (utc.hour, utc.month))

    def test_month_series_matches_contract_and_breakpoint_visible(self):
        b = self.get("/api/aqi/series", "AqiSeries")
        self.assertEqual((b["points"][0]["t"], b["points"][-1]["t"], len(b["points"])), ("2022-04-01T00:00:00+07:00", "2026-03-01T00:00:00+07:00", 48))
        vals = {p["t"][:7]: p["value"] for p in b["points"]}
        self.assertGreater(vals["2025-06"], 2 * vals["2025-08"])  # CO giảm mạnh quanh 01/07/2025 (đã giảm một đợt từ 03/2025)

    def test_profile(self):
        b = self.get("/api/aqi/hourly-profile", "HourlyProfile")
        self.assertEqual([len(p["values"]) for p in b["profiles"]], [24, 24])
        self.assertEqual(b["profiles"][0]["to"], "2025-06-30T23:00:00+07:00")


if __name__ == "__main__":
    unittest.main()
