#!/usr/bin/env python3
"""Sinh lại mock/aqi-series-month.json và mock/aqi-hourly-profile.json từ chính mã backend, để mock và backend thật cùng số.

  cd backend && python3 tools/regen_mock_aqi.py        (cần file Kaggle, xem AQI_CSV_PATH)
"""
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import aqi_data

MOCK = Path(__file__).resolve().parents[2] / "mock"


def dump(name, obj):
    (MOCK / name).write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("đã ghi", MOCK / name)


ds = asyncio.run(aqi_data.get())
dump("aqi-series-month.json", aqi_data.series(ds, "co", "month", None, None))
dump("aqi-hourly-profile.json", aqi_data.hourly_profile(ds))
