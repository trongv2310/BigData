"""Backend CO HCMC Dashboard. Chạy:  cd backend && python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000"""
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from . import config, errors
from .clients import hdfs
from .routes import aqi, core, flows, hdfs as hdfs_routes, infra, stream


@asynccontextmanager
async def lifespan(_: FastAPI):
    yield
    await hdfs.aclose()


def create_app() -> FastAPI:
    app = FastAPI(title="CO HCMC Dashboard API", version="0.1.0", lifespan=lifespan)
    app.add_middleware(CORSMiddleware, allow_origins=config.CORS_ORIGINS, allow_methods=["GET", "POST"], allow_headers=["Content-Type"])
    errors.install(app)
    app.include_router(core.router)
    app.include_router(hdfs_routes.router)
    app.include_router(infra.router)
    app.include_router(aqi.router)
    app.include_router(flows.router)
    app.include_router(stream.router)
    return app


app = create_app()
