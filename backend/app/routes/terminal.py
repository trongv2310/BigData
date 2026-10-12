"""WebSocket /ws/terminal (events.md mục 2): một pty chạy bash trong WSL. Chạy lệnh thật bằng quyền của người dùng.

Bảo mật (events.md): chỉ nhận kết nối từ loopback; kiểm Origin; token ngẫu nhiên mỗi lần khởi động (in ra console), sai thì đóng 4401;
một phiên tại một thời điểm; tự đóng sau TERMINAL_IDLE_SECONDS không hoạt động; không ghi nội dung người dùng gõ vào log.
Thêm: tắt mặc định, vì shell ghi được lên HDFS/Kafka/Spark; chỉ bật khi BACKEND_ALLOW_WRITES=1 (CLAUDE.md: ghi phải qua lớp có thể tắt).
Phần còn lại của backend (127.0.0.1) do cách chạy uvicorn quyết định: dùng --host 127.0.0.1, đừng dùng 0.0.0.0.
"""
import asyncio
import codecs
import fcntl
import json
import os
import pty
import secrets
import signal
import struct
import subprocess
import termios
import time

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from .. import config

router = APIRouter()
TOKEN = secrets.token_urlsafe(24)  # đổi mỗi lần backend khởi động
LOOPBACK = {"127.0.0.1", "::1", "testclient"}  # "testclient" chỉ là tên giả của TestClient của Starlette
MAX_INPUT = 64 * 1024
_busy = False

CLOSE_BAD_TOKEN, CLOSE_DISABLED, CLOSE_BUSY, CLOSE_IDLE = 4401, 4403, 4409, 4408


def _set_winsize(fd: int, rows: int, cols: int) -> None:
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))


def _spawn() -> tuple[subprocess.Popen, int]:
    master, slave = pty.openpty()
    env = {**os.environ, "TERM": "xterm-256color", "HISTFILE": "/dev/null"}  # không ghi lệnh vào ~/.bash_history
    env.setdefault("LANG", "C.UTF-8")
    try:
        proc = subprocess.Popen(
            ["bash", "-i"], stdin=slave, stdout=slave, stderr=slave, cwd=str(config.REPO_ROOT), env=env,
            start_new_session=True, close_fds=True, preexec_fn=lambda: fcntl.ioctl(0, termios.TIOCSCTTY, 1))
    finally:
        os.close(slave)
    return proc, master


def _kill(proc: subprocess.Popen) -> None:
    for sig in (signal.SIGHUP, signal.SIGKILL):
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            return
        try:
            proc.wait(timeout=1)
            return
        except subprocess.TimeoutExpired:
            continue


@router.websocket("/ws/terminal")
async def ws_terminal(ws: WebSocket):
    global _busy
    # 1. Từ chối ngay khi bắt tay (HTTP 403) nếu không phải loopback hoặc sai Origin: không cho cả việc thử token
    if (ws.client.host if ws.client else None) not in LOOPBACK:
        await ws.close(code=1008)
        return
    if ws.headers.get("origin") not in config.TERMINAL_ORIGINS:
        await ws.close(code=1008)
        return
    await ws.accept()
    # 2. Các điều kiện còn lại đóng bằng mã riêng để giao diện hiện được lý do
    if not config.ALLOW_WRITES:
        await ws.close(code=CLOSE_DISABLED, reason="Cửa sổ lệnh đang tắt. Đặt BACKEND_ALLOW_WRITES=1 rồi chạy lại backend.")
        return
    if not secrets.compare_digest(ws.query_params.get("token", ""), TOKEN):
        await ws.close(code=CLOSE_BAD_TOKEN, reason="Sai token")
        return
    if _busy:
        await ws.close(code=CLOSE_BUSY, reason="Đã có một phiên đang mở")
        return
    _busy = True
    loop = asyncio.get_running_loop()
    proc = master = None
    try:
        proc, master = _spawn()
        out_q: asyncio.Queue[bytes | None] = asyncio.Queue()

        def on_readable():
            try:
                data = os.read(master, 65536)
            except OSError:
                data = b""
            if data:
                out_q.put_nowait(data)
            else:
                loop.remove_reader(master)
                out_q.put_nowait(None)

        loop.add_reader(master, on_readable)
        last = [time.monotonic()]

        async def pump_output():
            dec = codecs.getincrementaldecoder("utf-8")("replace")
            while True:
                data = await out_q.get()
                if data is None:
                    break
                last[0] = time.monotonic()
                text = dec.decode(data)
                if text:
                    await ws.send_text(json.dumps({"type": "output", "data": text}, ensure_ascii=False))
            code = await loop.run_in_executor(None, proc.wait)
            await ws.send_text(json.dumps({"type": "exit", "code": code}))

        async def pump_input():
            while True:
                try:
                    msg = json.loads(await ws.receive_text())
                except json.JSONDecodeError:
                    continue
                last[0] = time.monotonic()
                kind = msg.get("type") if isinstance(msg, dict) else None
                if kind == "input" and isinstance(msg.get("data"), str):
                    os.write(master, msg["data"][:MAX_INPUT].encode("utf-8"))
                elif kind == "resize":
                    cols, rows = msg.get("cols"), msg.get("rows")
                    if isinstance(cols, int) and isinstance(rows, int) and 1 <= cols <= 500 and 1 <= rows <= 200:
                        _set_winsize(master, rows, cols)

        async def idle_watch():
            while time.monotonic() - last[0] < config.TERMINAL_IDLE_SECONDS:
                await asyncio.sleep(min(5.0, max(0.05, config.TERMINAL_IDLE_SECONDS / 10)))

        tasks = {asyncio.create_task(pump_output()): "out", asyncio.create_task(pump_input()): "in", asyncio.create_task(idle_watch()): "idle"}
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        reason = tasks[next(iter(done))]
        for t in tasks:
            t.cancel()
        if reason == "idle":
            await ws.close(code=CLOSE_IDLE, reason="Không hoạt động quá lâu")
        elif reason == "out":
            await ws.close(code=1000)
        else:
            for t in done:
                t.exception()  # đã đọc, tránh cảnh báo; WebSocketDisconnect nghĩa là client đã đóng
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        _busy = False
        if master is not None:
            try:
                loop.remove_reader(master)
            except Exception:
                pass
            os.close(master)
        if proc is not None:
            await loop.run_in_executor(None, _kill, proc)
