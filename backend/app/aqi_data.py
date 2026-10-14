"""Bộ dữ liệu Kaggle (quyết định D4, tạm) cho /api/aqi/*.

Cột `datetime` của file là UTC, định dạng dd-mm-yy H:MM. Đã kiểm 2026-10-12: tương quan CO với bản thô OpenWeather
(`datetime_utc`) cao nhất ở độ lệch 0 giờ (0,711), nên ở đây cộng 7 giờ để ra giờ Việt Nam.
Các cột `hour`, `month`, `dayofweek` trong file tính theo UTC, KHÔNG dùng chúng để nhóm theo giờ địa phương.
"""
import asyncio
import calendar
import csv
import io
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from . import config, reading, timeutil
from .clients import hdfs

METRICS = ("co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3")
UTC_TO_VN = timedelta(hours=7)
BREAK = datetime(2025, 7, 1, tzinfo=timeutil.VN)  # điểm gãy dữ liệu (AGENTS.md mục 5)
MIN_COVERAGE = 0.5  # bỏ ô gộp mà bộ dữ liệu chỉ phủ dưới 50% (ví dụ 7 giờ cuối của 1/4/2026 sau khi đổi sang giờ VN)


@dataclass
class Dataset:
    times: list[datetime]  # giờ Việt Nam, có múi giờ, tăng dần
    cols: dict[str, list[float]]  # metric, windSpeed, temperature -> giá trị theo từng dòng
    source: str


def _parse(text: str, source: str) -> Dataset:
    times, cols = [], {k: [] for k in (*METRICS, "wind_speed_kmh", "temperature_C")}
    for row in csv.DictReader(io.StringIO(text)):
        utc = datetime.strptime(row["datetime"], "%d-%m-%y %H:%M")
        times.append((utc + UTC_TO_VN).replace(tzinfo=timeutil.VN))
        for k in cols:
            cols[k].append(float(row[k]))
    return Dataset(times, cols, source)


_cache: tuple[object, Dataset] | None = None  # (dấu hiệu phiên bản file, dữ liệu)


async def get() -> Dataset:
    """File cục bộ nếu có (nạp lại khi đổi mtime); không thì /raw/Dataset.csv trên HDFS (nạp một lần)."""
    global _cache
    path = Path(config.AQI_CSV_PATH)
    if path.is_file():
        stamp = ("file", path.stat().st_mtime_ns)
        if _cache is None or _cache[0] != stamp:
            ds = await asyncio.to_thread(lambda: _parse(path.read_text(encoding="utf-8-sig"), f"kaggle:{path.name}"))
            _cache = (stamp, ds)
        return _cache[1]
    if _cache is None or _cache[0] != ("hdfs",):
        text = await hdfs.read_text(config.HDFS_DATASET_PATH)
        _cache = (("hdfs",), await asyncio.to_thread(_parse, text.lstrip("﻿"), f"kaggle:hdfs{config.HDFS_DATASET_PATH}"))
    return _cache[1]


def _bucket_start(t: datetime, granularity: str) -> datetime:
    if granularity == "hour":
        return t.replace(minute=0, second=0, microsecond=0)
    if granularity == "day":
        return t.replace(hour=0, minute=0, second=0, microsecond=0)
    return t.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _expected_rows(start: datetime, granularity: str) -> int:
    if granularity == "hour":
        return 1
    if granularity == "day":
        return 24
    return calendar.monthrange(start.year, start.month)[1] * 24


_GRAN_VI = {"hour": "giờ", "day": "ngày", "month": "tháng"}


def series(ds: Dataset, metric: str, granularity: str, start: datetime | None, end: datetime | None) -> dict:
    """Trung bình theo ô giờ/ngày/tháng (giờ VN). Ô được chọn nếu thời điểm bắt đầu nằm trong [start, end]."""
    sums: dict[datetime, list[float]] = {}  # bắt đầu ô -> [tổng, số dòng]
    for t, v in zip(ds.times, ds.cols[metric]):
        acc = sums.setdefault(_bucket_start(t, granularity), [0.0, 0])
        acc[0] += v
        acc[1] += 1
    points = []
    for b in sorted(sums):
        total, n = sums[b]
        if n < MIN_COVERAGE * _expected_rows(b, granularity):
            continue
        if (start is not None and b < start) or (end is not None and b > end):
            continue
        points.append({"t": timeutil.iso(b), "value": round(total / n, 2)})
    return {
        "metric": metric, "unit": "µg/m³", "granularity": granularity, "timezone": "Asia/Ho_Chi_Minh",
        "source": f"{ds.source} (trung bình theo {_GRAN_VI[granularity]}; datetime gốc UTC, đã cộng 7 giờ)",
        "points": points,
    }


def _profile(ds: Dataset, idx: list[int], label: str) -> dict:
    sums, counts = [0.0] * 24, [0] * 24
    for i in idx:
        h = ds.times[i].hour
        sums[h] += ds.cols["co"][i]
        counts[h] += 1
    if min(counts) == 0:
        raise ValueError(f"{label}: thiếu dữ liệu ở một số giờ trong ngày")
    return {"label": label, "from": timeutil.iso(ds.times[idx[0]]), "to": timeutil.iso(ds.times[idx[-1]]),
            "values": [round(s / c, 1) for s, c in zip(sums, counts)]}


def hourly_profile(ds: Dataset) -> dict:
    before = [i for i, t in enumerate(ds.times) if t < BREAK]
    after = [i for i, t in enumerate(ds.times) if t >= BREAK]
    profiles = [_profile(ds, before, "Trước 01/07/2025"), _profile(ds, after, "Từ 01/07/2025")]
    parts = []
    for p, idx in zip(profiles, (before, after)):
        v = p["values"]
        mean = sum(ds.cols["co"][i] for i in idx) / len(idx)
        parts.append(f"{p['label']}: trung bình {mean:.0f} µg/m³, cao nhất lúc {v.index(max(v))} giờ, thấp nhất lúc {v.index(min(v))} giờ")
    note = ("Datetime gốc của Kaggle là UTC; đã cộng 7 giờ để ra giờ địa phương. " + "; ".join(parts) + ". "
            "Mức CO giảm mạnh sau 01/07/2025 (điểm gãy có trong cả bản Kaggle lẫn bản thô OpenWeather), nên không nối hai đoạn thành một đường liền. "
            "Nguồn: bộ Kaggle (quyết định D4, tạm).")
    return {"metric": "co", "unit": "µg/m³", "timezone": "Asia/Ho_Chi_Minh", "note": note, "profiles": profiles}


def last_reading(ds: Dataset) -> dict:
    """Dòng cuối của bộ dữ liệu, dạng AqiReading. Chỉ dùng khi luồng /stream_input chưa có bản ghi nào."""
    i = len(ds.times) - 1
    co, wind = ds.cols["co"][i], ds.cols["wind_speed_kmh"][i]
    return {
        "datetime": timeutil.iso(ds.times[i]),
        "originalDatetime": timeutil.iso(ds.times[i]),  # đây chính là thời điểm gốc của dòng lịch sử
        "station": "Bộ dữ liệu Kaggle (không có trạm)",
        "isSimulatedStation": True,  # quyết định D2 (tạm): luôn true
        "source": "replay-dataset",
        "co": co, "windSpeedKmh": wind, "temperatureC": ds.cols["temperature_C"][i],
        "coAccumulationIndex": round(co / (wind + 1.0), 2),
        "status": reading.status_for(co),
        "coPredicted": None,
        "modelVersion": None,
    }
