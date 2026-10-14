"""Backend CO HCMC Dashboard. Chạy:  cd backend && python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000"""
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from . import config, errors, ml_engine, sql_engine
from .clients import hdfs
from .routes import analysis, aqi, core, flows, hdfs as hdfs_routes, infra, model, pipeline, predict, replay, sql, stream, terminal


@asynccontextmanager
async def lifespan(_: FastAPI):
    if config.ALLOW_WRITES:
        print(f"[backend] Token cửa sổ lệnh (dán vào giao diện một lần): {terminal.TOKEN}", flush=True)
    else:
        print("[backend] Cửa sổ lệnh /ws/terminal đang TẮT (đặt BACKEND_ALLOW_WRITES=1 để bật).", flush=True)
    yield
    ml_engine.stop()
    sql_engine.stop()
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
    app.include_router(predict.router)
    app.include_router(replay.router)
    app.include_router(pipeline.router)
    app.include_router(analysis.router)
    app.include_router(model.router)
    app.include_router(sql.router)
    app.include_router(terminal.router)
    app.include_router(stream.router)
    return app


app = create_app()
