"""Điều khiển luồng PHÁT LẠI (infra/replay_kafka_producer.py) qua tệp cục bộ, không qua HDFS/Kafka/Spark.

Producer ghi `status.json` (nhịp tim mỗi 2 giây) và đọc `control.json` trong config.REPLAY_STATE_DIR. Backend chỉ đọc `status.json`; chỉ ghi
`control.json` khi config.REPLAY_CONTROL bật (mặc định bật; đặt REPLAY_CONTROL=0 để backend không điều khiển được gì).
Không có nhịp tim mới trong STALE_SECONDS thì coi là "stopped": không bịa trạng thái đang chạy.
Chỉ ảnh hưởng luồng phát lại: luồng live-openweather không đọc các tệp này.
"""
import asyncio
import json
import os
from datetime import datetime
from pathlib import Path

from . import config, timeutil
from .errors import ApiError

STALE_SECONDS = 10
SPEEDS = (1, 10, 60)
CONFIRM_POLLS, CONFIRM_STEP = 30, 0.1  # chờ producer xác nhận lệnh tối đa 30 x 0,1 = 3 giây


def _dir() -> Path:
    return Path(config.REPLAY_STATE_DIR)


def _read_json(name: str) -> dict | None:
    try:
        v = json.loads((_dir() / name).read_text(encoding="utf-8"))
        return v if isinstance(v, dict) else None
    except (OSError, ValueError):
        return None


def _age_seconds(iso: str) -> float:
    return (datetime.now(timeutil.VN) - datetime.fromisoformat(iso)).total_seconds()


def status() -> dict:
    """ReplayStatus của hợp đồng, từ nhịp tim của producer."""
    base = float(config.REPLAY_BASE_INTERVAL)
    s = _read_json("status.json")
    alive = False
    if s:
        try:
            alive = _age_seconds(s["updatedAt"]) <= STALE_SECONDS and s["speed"] in SPEEDS and s["state"] in ("running", "paused")
        except (KeyError, ValueError, TypeError):
            s = None
    if not s or not alive:
        c = _read_json("control.json") or {}
        speed = c.get("speed") if c.get("speed") in SPEEDS else 1
        return {"state": "stopped", "speed": speed, "baseIntervalSeconds": base, "intervalSeconds": base / speed, "controllable": False,
                "emittedCount": int(s["emittedCount"]) if s and isinstance(s.get("emittedCount"), int) else 0,
                "lastOriginalDatetime": s.get("lastOriginalDatetime") if s else None,
                "updatedAt": timeutil.now_iso(),
                "detail": "Không có tiến trình phát lại nào đang chạy (chạy bash infra/live_stack.sh start). Luồng dữ liệu thật không bị ảnh hưởng."}
    ctrl = config.REPLAY_CONTROL
    return {"state": s["state"], "speed": s["speed"], "baseIntervalSeconds": float(s["baseIntervalSeconds"]),
            "intervalSeconds": float(s["intervalSeconds"]), "controllable": ctrl, "emittedCount": int(s["emittedCount"]),
            "lastOriginalDatetime": s.get("lastOriginalDatetime"), "updatedAt": s["updatedAt"],
            "detail": None if ctrl else "Điều khiển phát lại đang tắt trên backend (REPLAY_CONTROL=0)."}


async def control(action: str, speed: int | None) -> dict:
    """Ghi lệnh cho producer rồi chờ nó xác nhận (tối đa 3 giây). 409 REPLAY_UNAVAILABLE nếu không điều khiển được."""
    cur = status()
    if cur["state"] == "stopped" or not cur["controllable"]:
        raise ApiError(409, "REPLAY_UNAVAILABLE", cur["detail"] or "Không điều khiển được luồng phát lại lúc này.")
    want = {"paused": action == "pause", "speed": speed if speed is not None else cur["speed"]}
    d = _dir()
    d.mkdir(parents=True, exist_ok=True)
    tmp = d / "control.json.tmp"
    tmp.write_text(json.dumps(want), encoding="utf-8")
    os.replace(tmp, d / "control.json")  # ghi nguyên tử để producer không đọc phải nửa tệp
    for _ in range(CONFIRM_POLLS):
        await asyncio.sleep(CONFIRM_STEP)
        now = status()
        if now["state"] == ("paused" if want["paused"] else "running") and now["speed"] == want["speed"]:
            return now
    raise ApiError(409, "REPLAY_UNAVAILABLE", "Đã ghi lệnh nhưng tiến trình phát lại chưa xác nhận trong 3 giây (có thể đã dừng).")
