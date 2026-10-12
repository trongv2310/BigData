from fastapi import APIRouter

from .. import services

router = APIRouter()


@router.get("/api/services")
async def get_services():
    return {"services": await services.check_all()}
