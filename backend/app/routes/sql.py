import asyncio

from fastapi import APIRouter
from pydantic import BaseModel, ConfigDict, Field

from .. import sql_engine, sql_guard
from ..errors import ApiError

router = APIRouter()


class SqlRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    query: str
    limit: int = Field(default=100, ge=1, le=1000)


@router.post("/api/sql")
async def run_sql(req: SqlRequest):
    try:
        return await asyncio.to_thread(sql_engine.run, req.query, req.limit)
    except sql_guard.SqlNotAllowed as e:
        raise ApiError(400, "SQL_NOT_ALLOWED", str(e))
    except sql_engine.SqlTimeout as e:
        raise ApiError(504, "SQL_TIMEOUT", str(e))
    except sql_engine.SqlError as e:
        raise ApiError(400, "SQL_ERROR", str(e))
    except sql_engine.SqlUnavailable as e:
        raise ApiError(503, "SQL_UNAVAILABLE", str(e))
