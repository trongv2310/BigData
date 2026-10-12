"""/api/aqi/*: dữ liệu CO (bộ Kaggle, D4 tạm) và bản ghi mới nhất của luồng."""
from typing import Literal

from fastapi import APIRouter, Query

from .. import aqi_data, stream_input, timeutil
from ..clients import hdfs
from ..errors import ApiError

router = APIRouter()


async def _dataset() -> aqi_data.Dataset:
    try:
        return await aqi_data.get()
    except (hdfs.HdfsUnreachable, hdfs.HdfsNotFound, hdfs.HdfsError, OSError, ValueError, KeyError) as e:
        raise ApiError(503, "DATASET_UNAVAILABLE", f"Không nạp được bộ dữ liệu CO ({type(e).__name__}); xem AQI_CSV_PATH và HDFS /raw/Dataset.csv")


def _time(name: str, value: str | None):
    if value is None:
        return None
    try:
        return timeutil.parse_query_time(value)
    except ValueError:
        raise ApiError(400, "BAD_REQUEST", f"{name} phải là ISO 8601, ví dụ 2025-07-01T00:00:00+07:00 (nhận: {value!r})")


@router.get("/api/aqi/latest")
async def aqi_latest():
    """Bản ghi mới nhất của luồng /stream_input; luồng trống hoặc HDFS tắt thì lấy dòng cuối bộ dữ liệu (source=replay-dataset, datetime cũ)."""
    try:
        r = await stream_input.latest_reading()
    except (hdfs.HdfsUnreachable, hdfs.HdfsError):
        r = None
    return r or aqi_data.last_reading(await _dataset())


@router.get("/api/aqi/series")
async def aqi_series(
    metric: Literal["co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3"] = "co",
    granularity: Literal["hour", "day", "month"] = "month",
    from_: str | None = Query(None, alias="from"),
    to: str | None = None,
):
    start, end = _time("from", from_), _time("to", to)
    if start and end and start > end:
        raise ApiError(400, "BAD_REQUEST", "from phải không sau to")
    return aqi_data.series(await _dataset(), metric, granularity, start, end)


@router.get("/api/aqi/hourly-profile")
async def aqi_hourly_profile():
    try:
        return aqi_data.hourly_profile(await _dataset())
    except (ValueError, IndexError) as e:  # nhóm rỗng hoặc thiếu giờ: hợp đồng đòi đủ 24 giá trị
        raise ApiError(503, "DATASET_INCOMPLETE", f"Bộ dữ liệu không đủ để tính nhịp theo giờ: {e}")
