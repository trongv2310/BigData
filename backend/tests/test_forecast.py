"""Hợp đồng 0.5.0: coIsForecast (co là điểm DỰ BÁO của OpenWeather chứ không phải quan sát) đi xuyên suốt producer -> Spark -> backend.
Chạy: cd backend && python3 -m unittest discover tests"""
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(ROOT / "pipeline"))
sys.path.insert(0, str(ROOT / "infra"))

import model_inputs as mi
from app import aqi_data, reading
from helpers import schema_errors
from test_a5 import dataset_rows, history

try:
    import pyspark  # noqa: F401
    import requests
    HAVE = True
except ImportError:
    HAVE = False

VN = timezone(timedelta(hours=7))


class ForecastFlag(unittest.TestCase):
    def test_true_only_when_the_matching_point_is_after_the_apis_own_dt(self):
        h = history()
        j = 28
        comps, at = h[j]["components"], h[j]["dt"]
        self.assertIs(mi.forecast_flag(h, comps, at - 1800), True)                       # đo thật: lúc 16:30:56 API trả điểm 17:00
        self.assertIs(mi.forecast_flag(h, comps, at + 180), False)                       # lúc 13:03 API trả điểm 13:00
        self.assertIs(mi.forecast_flag(h, comps, at), False)                             # đúng :00 thì không phải tương lai
        self.assertIs(mi.forecast_flag(list(reversed(h)), comps, at - 1800), True)       # thứ tự lịch sử không ảnh hưởng

    def test_unknown_is_none_never_a_guess(self):
        h = history()
        self.assertIsNone(mi.forecast_flag(h, h[28]["components"], None))                # không biết dt của API
        self.assertIsNone(mi.forecast_flag(h, dict(h[28]["components"], co=999.0), h[28]["dt"]))  # không có điểm lịch sử khớp
        self.assertIsNone(mi.forecast_flag([], h[28]["components"], h[28]["dt"]))        # lịch sử rỗng

    def test_the_note_and_the_flag_agree(self):
        h = history()
        at = h[28]["dt"]
        for observed, want in ((at - 1800, True), (at + 600, False)):
            _, note = mi.from_history(h, h[28]["components"], {"temperatureC": 30.0, "windSpeedKmh": 5.0}, observed_dt=observed)
            self.assertEqual("DỰ BÁO" in note, want)
            self.assertIs(mi.forecast_flag(h, h[28]["components"], observed), want)


@unittest.skipUnless(HAVE, "cần pyspark và requests")
class Producers(unittest.TestCase):
    def rec(self, h, j, observed_offset):
        import live_producer_owm as owm
        return {"owmDt": h[j]["dt"] + observed_offset, "co": h[j]["components"]["co"], **{n: h[j]["components"][n] for n in owm.POLLUTANTS}, "temperature_C": 30.0, "wind_speed_kmh": 5.0}

    def test_owm_producer_writes_the_flag_when_known_and_omits_it_when_not(self):
        import live_producer_owm as owm
        h = history()
        early = self.rec(h, 28, -1800)                                                   # API báo trước điểm khớp 30 phút: dự báo
        owm.attach_model_inputs(early, "K", history_fn=lambda k, dt, s: h)
        self.assertIs(early["coIsForecast"], True)
        late = self.rec(h, 28, +180)
        owm.attach_model_inputs(late, "K", history_fn=lambda k, dt, s: h)
        self.assertIs(late["coIsForecast"], False)

        def boom(k, dt, s):
            raise requests.ConnectionError("lỗi mạng")
        failed = self.rec(h, 28, 0)
        owm.attach_model_inputs(failed, "K", history_fn=boom)
        self.assertNotIn("coIsForecast", failed)                                         # không lấy được lịch sử: không ghi, không đoán
        nomatch = self.rec(h, 28, 0)
        nomatch["co"] = 999.0
        owm.attach_model_inputs(nomatch, "K", history_fn=lambda k, dt, s: h)
        self.assertNotIn("coIsForecast", nomatch)

    def test_flag_is_independent_of_whether_lags_are_complete(self):
        """Điểm khớp quá sớm trong lịch sử (thiếu độ trễ) thì không có modelInputs, nhưng vẫn biết giá trị có phải dự báo hay không."""
        import live_producer_owm as owm
        h = history()
        r = self.rec(h, 10, -1800)
        owm.attach_model_inputs(r, "K", history_fn=lambda k, dt, s: h)
        self.assertNotIn("modelInputs", r)
        self.assertIs(r["coIsForecast"], True)

    def test_replay_messages_are_never_forecasts(self):
        import replay_kafka_producer as rp
        rows = dataset_rows()
        for i in (5, 30):
            m = rp.message_for(rows, i, 1, datetime(2026, 10, 14, 17, 0, tzinfo=VN))
            self.assertIs(m["coIsForecast"], False)
        r = reading.from_record(m)
        self.assertEqual(schema_errors("AqiReading", r), [])
        self.assertIs(r["coIsForecast"], False)


class BackendPassThrough(unittest.TestCase):
    BASE = {"datetime": "2026-10-14 16:30:56", "station": "Tram_TPHCM_OWM", "co": 298.52, "wind_speed_kmh": 14.5, "temperature_C": 28.1, "provider": "openweather"}

    def test_both_key_styles_and_only_real_booleans(self):
        for key in ("co_is_forecast", "coIsForecast"):                                   # bản ghi làm giàu (Spark) và bản ghi thô (producer)
            for value in (True, False):
                r = reading.from_record({**self.BASE, key: value})
                self.assertIs(r["coIsForecast"], value)
                self.assertEqual(schema_errors("AqiReading", r), [])
        for bad in (None, "true", 1, 0, "", [], {}):
            self.assertIsNone(reading.from_record({**self.BASE, "co_is_forecast": bad})["coIsForecast"], bad)   # không ép kiểu: chuỗi/số không thành boolean
        self.assertIsNone(reading.from_record(self.BASE)["coIsForecast"])                # bản ghi cũ không có trường: null

    def test_enriched_key_wins_over_raw_key(self):
        self.assertIs(reading.from_record({**self.BASE, "co_is_forecast": False, "coIsForecast": True})["coIsForecast"], False)

    def test_open_meteo_and_legacy_file_stream_are_null(self):
        om = {"datetime": "2026-10-12 23:15:00", "station": "Tram_TPHCM_Live_API", "co": 2508.0, "wind_speed_kmh": 1.6, "temperature_C": 25.9}
        self.assertIsNone(reading.from_record(om)["coIsForecast"])
        self.assertIsNone(reading.from_record({**om, "station": "Tram_Quan_1"})["coIsForecast"])

    def test_dataset_fallback_row_is_historical_not_forecast(self):
        from test_p3 import write_csv
        d = Path(tempfile.mkdtemp())
        write_csv(d / "a.csv", datetime(2025, 6, 30, 12), 3, lambda t: 1500)
        r = aqi_data.last_reading(aqi_data._parse((d / "a.csv").read_text(encoding="utf-8"), "test"))
        self.assertIs(r["coIsForecast"], False)
        self.assertEqual(schema_errors("AqiReading", r), [])


@unittest.skipUnless(HAVE, "cần pyspark")
class SparkPassThrough(unittest.TestCase):
    def test_enrich_keeps_the_flag_column(self):
        import enrich as en
        from pyspark.sql import SparkSession
        from pyspark.sql.types import BooleanType, DoubleType, StructField, StructType
        spark = (SparkSession.builder.master("local[1]").appName("test_forecast").config("spark.ui.enabled", "false")
                 .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        self.addCleanup(spark.stop)
        schema = StructType([StructField("co", DoubleType()), StructField("coIsForecast", BooleanType()), StructField("modelInputs", en.model_inputs_schema())])
        out = en.without_model(spark.createDataFrame([(1.0, True, None), (2.0, False, None), (3.0, None, None)], schema))
        self.assertEqual([r["coIsForecast"] for r in out.orderBy("co").collect()], [True, False, None])   # cột đi qua nguyên vẹn, kể cả null


if __name__ == "__main__":
    unittest.main()
