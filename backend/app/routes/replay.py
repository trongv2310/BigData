"""/api/replay/*: trạng thái và điều khiển luồng PHÁT LẠI (mô phỏng). Không đụng luồng dữ liệu thật."""
from typing import Literal

from fastapi import APIRouter
from pydantic import BaseModel, ConfigDict

from .. import replay

router = APIRouter()


class ReplayControlRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")  # additionalProperties: false
    action: Literal["pause", "resume"]
    speed: Literal[1, 10, 60] | None = None


@router.get("/api/replay/status")
async def replay_status():
    return replay.status()


@router.post("/api/replay/control")
async def replay_control(req: ReplayControlRequest):
    return await replay.control(req.action, req.speed)
