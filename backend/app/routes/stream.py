"""WebSocket /ws/stream (events.md mục 1). Chỉ đẩy xuống, không nhận lệnh."""
import asyncio
import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from .. import config, timeutil
from ..hub import hub

router = APIRouter()
MIN_GAP = 1.0 / config.STREAM_MAX_EVENTS_PER_SECOND


def _envelope(type_: str, seq: int, data: dict) -> str:
    return json.dumps({"type": type_, "ts": timeutil.now_iso(), "seq": seq, "data": data}, ensure_ascii=False)


async def _live(ws: WebSocket):
    q = hub.subscribe()
    seq = 0
    try:
        for s in await hub.snapshot_services():  # trạng thái ban đầu, để giao diện không phải chờ có thay đổi
            await ws.send_text(_envelope("service.state", seq, s))
            seq += 1
        while True:
            type_, data = await q.get()
            await ws.send_text(_envelope(type_, seq, data))
            seq += 1
            await asyncio.sleep(MIN_GAP)  # tối đa ~20 tin/giây
    finally:
        hub.unsubscribe(q)


async def _replay(ws: WebSocket):
    """GIẢ LẬP: phát lại mock/stream-events.ndjson, đổi ts/seq (và datetime của reading) sang bây giờ."""
    lines = [json.loads(l) for l in config.STREAM_REPLAY_FILE.read_text(encoding="utf-8").splitlines() if l.strip()]
    seq = 0
    while True:
        for ev in lines:
            now = timeutil.now_iso()
            data = dict(ev["data"])
            if ev["type"] == "reading":
                data["datetime"] = now
            elif ev["type"] == "service.state":
                data["checkedAt"] = now
            await ws.send_text(_envelope(ev["type"], seq, data))
            seq += 1
            await asyncio.sleep(1.0)


@router.websocket("/ws/stream")
async def ws_stream(ws: WebSocket):
    await ws.accept()
    sender = asyncio.create_task(_replay(ws) if config.STREAM_MODE == "replay" else _live(ws))
    try:
        while not sender.done():  # đọc để phát hiện client đóng; nội dung client gửi bị bỏ qua
            recv = asyncio.create_task(ws.receive_text())
            await asyncio.wait({recv, sender}, return_when=asyncio.FIRST_COMPLETED)
            if not recv.done():
                recv.cancel()
                break
            recv.result()  # ném WebSocketDisconnect khi client đóng
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        sender.cancel()
        try:
            await sender
        except (asyncio.CancelledError, Exception):
            pass
