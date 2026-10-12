from fastapi import APIRouter

from .. import flows
from ..errors import ApiError

router = APIRouter()


@router.get("/api/flows")
async def list_flows():
    return {"flows": flows.SUMMARIES}


@router.get("/api/flows/{flow_id}")
async def get_flow(flow_id: str):
    d = flows.DEFINITIONS.get(flow_id)
    if d is None:
        raise ApiError(404, "NOT_FOUND", f"Chưa có định nghĩa cho luồng '{flow_id}'")
    return d
