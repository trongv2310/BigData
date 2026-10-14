from fastapi import APIRouter
from pydantic import BaseModel, ConfigDict, Field

from .. import aqi_data, predict
from ..clients import hdfs
from ..errors import ApiError

router = APIRouter()
_cache: tuple[int, predict.Baseline] | None = None


class PredictRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")  # additionalProperties: false
    hour: int = Field(ge=0, le=23)
    month: int = Field(ge=1, le=12)
    dayOfWeek: int | None = Field(default=None, ge=0, le=6)
    temperatureC: float
    windSpeedKmh: float = Field(ge=0)
    # Hợp đồng 0.2.0: đầu vào của mô hình ml (độ trễ CO, các chất khác). Heuristic hiện tại kiểm hợp lệ nhưng không dùng.
    coLag1h: float | None = Field(default=None, ge=0)
    coLag2h: float | None = Field(default=None, ge=0)
    coLag3h: float | None = Field(default=None, ge=0)
    coLag24h: float | None = Field(default=None, ge=0)
    no: float | None = Field(default=None, ge=0)
    no2: float | None = Field(default=None, ge=0)
    o3: float | None = Field(default=None, ge=0)
    so2: float | None = Field(default=None, ge=0)
    pm25: float | None = Field(default=None, ge=0)
    pm10: float | None = Field(default=None, ge=0)
    nh3: float | None = Field(default=None, ge=0)


@router.post("/api/predict")
async def predict_co(req: PredictRequest):
    global _cache
    try:
        ds = await aqi_data.get()
    except (hdfs.HdfsUnreachable, hdfs.HdfsNotFound, hdfs.HdfsError, OSError, ValueError, KeyError) as e:
        raise ApiError(503, "DATASET_UNAVAILABLE", f"Không nạp được bộ dữ liệu CO ({type(e).__name__})")
    if _cache is None or _cache[0] != id(ds):  # dựng lại khi bộ dữ liệu được nạp lại
        try:
            _cache = (id(ds), predict.build(ds))
        except ValueError as e:
            raise ApiError(503, "DATASET_INCOMPLETE", str(e))
    return predict.predict(_cache[1], req.hour)
