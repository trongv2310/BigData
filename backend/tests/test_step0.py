"""Bước 0 (hợp đồng 0.2.0): trường mới của AqiReading, /api/aqi/latest?source=, /api/predict nhận đầu vào mới và trả filledInputs.
Chạy: cd backend && python3 -m unittest discover tests"""
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import config, kafka_stream, reading
from helpers import schema_errors
from test_p3 import Base, write_csv

OPEN_METEO = {"datetime": "2026-10-12 23:15:00", "station": "Tram_TPHCM_Live_API", "co": 2508.0, "wind_speed_kmh": 1.6, "temperature_C": 25.9}
REPLAY = {"datetime": "2026-10-13 10:00:00", "station": "Tram_Quan_1", "co": 300.0, "wind_speed_kmh": 9.0, "temperature_C": 30.0}


class NewReadingFields(unittest.TestCase):
    def test_stream_records_carry_null_prediction_fields(self):
        for rec in (OPEN_METEO, REPLAY):
            r = reading.from_record(rec)
            self.assertEqual(schema_errors("AqiReading", r), [])
            # chưa có mô hình (Bước A4/A5) và producer chưa gửi thời điểm gốc (Bước A1): null, không điền số thay thế
            self.assertEqual((r["originalDatetime"], r["coPredicted"], r["modelVersion"]), (None, None, None))


class LatestBySource(Base):
    def setUp(self):
        d = Path(tempfile.mkdtemp())
        write_csv(d / "a.csv", datetime(2025, 6, 30, 12), 3, lambda t: 1500)
        self.use_csv(d / "a.csv")
        self._old = (config.STREAM_SOURCE, config.NAMENODE_HTTP)
        self.addCleanup(lambda: (setattr(config, "STREAM_SOURCE", self._old[0]), setattr(config, "NAMENODE_HTTP", self._old[1])))

    def latest(self, params, status=200):
        r = self.client.get("/api/aqi/latest", params=params)
        self.assertEqual(r.status_code, status, r.text)
        self.assertEqual(schema_errors("AqiReading" if status == 200 else "Error", r.json()), [], r.text)
        return r.json()

    def with_kafka_latest(self, rec):
        config.STREAM_SOURCE = "kafka"
        return mock.patch.object(kafka_stream, "latest_reading", mock.AsyncMock(return_value=reading.from_record(rec)))

    def test_matching_source_is_returned(self):
        with self.with_kafka_latest(OPEN_METEO):
            self.assertEqual(self.latest({"source": "live-open-meteo"})["co"], 2508.0)
            self.assertEqual(self.latest({})["source"], "live-open-meteo")

    def test_other_source_is_404_not_a_substitute(self):
        with self.with_kafka_latest(OPEN_METEO):
            for src in ("live-openweather", "replay-dataset"):
                self.assertEqual(self.latest({"source": src}, 404)["error"]["code"], "NO_READING_YET")

    def test_with_source_never_falls_back_to_dataset_row(self):
        config.STREAM_SOURCE, config.NAMENODE_HTTP = "hdfs", "http://127.0.0.1:1"  # HDFS tắt, luồng trống
        self.assertEqual(self.latest({"source": "replay-dataset"}, 404)["error"]["code"], "NO_READING_YET")
        # không có source: giữ hành vi cũ (dòng cuối bộ dữ liệu), nay kèm thời điểm gốc thật của dòng đó
        b = self.latest({})
        self.assertEqual((b["source"], b["originalDatetime"], b["coPredicted"]), ("replay-dataset", b["datetime"], None))

    def test_unknown_source_is_400(self):
        r = self.client.get("/api/aqi/latest", params={"source": "nope"})
        self.assertEqual(r.status_code, 400)
        self.assertEqual(schema_errors("Error", r.json()), [])


class PredictNewInputs(Base):
    def setUp(self):
        from app.routes import predict as predict_route
        self.route = predict_route
        d = Path(tempfile.mkdtemp())
        write_csv(d / "p.csv", datetime(2025, 5, 2, 17), 24 * 120, lambda t: 1000 if t < datetime(2025, 6, 30, 17) else 100 + ((t.hour + 7) % 24) * 10)
        self.use_csv(d / "p.csv")
        predict_route._cache = None
        self.addCleanup(lambda: setattr(predict_route, "_cache", None))
        nn = config.NAMENODE_HTTP
        config.NAMENODE_HTTP = "http://127.0.0.1:1"  # heuristic: không đọc HDFS thật (máy có thể đã có mô hình ml)
        self.addCleanup(lambda: setattr(config, "NAMENODE_HTTP", nn))

    REQ = {"hour": 12, "month": 11, "temperatureC": 30, "windSpeedKmh": 8}

    def test_accepts_ml_inputs_and_heuristic_reports_none_filled(self):
        body = {**self.REQ, "coLag1h": 300, "coLag2h": 290, "coLag3h": 280, "coLag24h": 310, "no": 1, "no2": 2, "o3": 3, "so2": 4, "pm25": 5, "pm10": 6, "nh3": 7}
        r = self.client.post("/api/predict", json=body)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(schema_errors("PredictResponse", r.json()), [])
        self.assertEqual((r.json()["model"]["kind"], r.json()["filledInputs"]), ("heuristic", []))

    def test_negative_lag_is_400(self):
        r = self.client.post("/api/predict", json={**self.REQ, "coLag1h": -1})
        self.assertEqual(r.status_code, 400)
        self.assertEqual(schema_errors("Error", r.json()), [])


if __name__ == "__main__":
    unittest.main()
