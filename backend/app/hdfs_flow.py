"""Biến sự kiện log NameNode thành `hdfs.op` và `flow.step` của luồng hdfs-write.

Bước nào quan sát được thật, bước nào suy ra (ghi trong docs/HANDOFF.md, UI cần ghi chú):
  1 create     audit `cmd=create` nếu audit bật (thật); không thì suy ra từ lần allocate đầu của file (không thể cấp block cho file chưa tạo)
  2 addBlock   dòng `allocate` của NameNode (thật)
  3 locations  cùng dòng allocate: NameNode đã chọn DataNode (suy ra: phản hồi cho client không có dòng log riêng)
  4,5,6        sau khi file đóng, bảng block của NameNode (WebHDFS) xác nhận block có độ dài và có DataNode giữ bản sao.
               Nghĩa là dữ liệu đã tới DataNode (4), được ACK (5), DataNode đã báo blockReceived (6). Suy ra từ trạng thái, không phải từ gói tin.
  7 complete   dòng `completeFile` (thật)
  8 rename     audit `cmd=rename` nếu audit bật (thật); không thì kiểm file tên cuối đã tồn tại qua WebHDFS (thật, quan sát trạng thái)
  9            không phát (nền, không gắn với một lượt ghi)
Backend chỉ biết sau khi bước xảy ra nên chỉ phát state `done`, không phát `started`.
"""
import time
import uuid
from typing import Awaitable, Callable

from . import flows
from .hadoop_logs import LogEvent

COPYING = "._COPYING_"
Out = tuple[str, dict]  # (type, data) cho Hub.publish
RUN_TTL = 120.0  # giây: bỏ run không còn tiến triển


class Run:
    def __init__(self, path: str, created: bool):
        self.id = "run-" + uuid.uuid4().hex[:8]
        self.path, self.created, self.blocks = path, created, []
        self.closed = False
        self.touched = time.monotonic()


def _step(run: Run, step: int) -> Out:
    return "flow.step", {"flowId": "hdfs-write", "runId": run.id, "step": step, "state": "done"}


def _op(op: str, path: str, block_id=None, datanode=None, nbytes=None) -> Out:
    return "hdfs.op", {"op": op, "path": path, "blockId": block_id, "datanode": datanode, "bytes": nbytes}


class WriteTracker:
    """fetch_blocks(path) -> [{"blockId": "blk_..", "lengthBytes": n, "locations": [...]}] hoặc None;
    path_exists(path) -> bool; resolve_datanode(ip:port) -> tên DataNode (khớp /api/hdfs/datanodes)."""

    def __init__(self, fetch_blocks: Callable[[str], Awaitable[list[dict] | None]],
                 path_exists: Callable[[str], Awaitable[bool]],
                 resolve_datanode: Callable[[str], Awaitable[str]], audit_enabled: bool):
        self.fetch_blocks, self.path_exists, self.resolve = fetch_blocks, path_exists, resolve_datanode
        self.audit = audit_enabled
        self.runs: dict[str, Run] = {}

    def _expire(self):
        now = time.monotonic()
        for p in [p for p, r in self.runs.items() if now - r.touched > RUN_TTL]:
            del self.runs[p]

    async def feed(self, ev: LogEvent) -> list[Out]:
        self._expire()
        if ev.kind == "create":
            run = self.runs[ev.path] = Run(ev.path, created=True)
            return [_op("create", ev.path), _step(run, flows.STEP_CREATE)]
        if ev.kind == "mkdir":
            return [_op("mkdir", ev.path)]
        if ev.kind == "delete":
            return [_op("delete", ev.path)]
        if ev.kind == "rename":
            return self._on_rename(ev)
        if ev.kind == "allocate":
            return await self._on_allocate(ev)
        if ev.kind == "complete":
            return await self._on_complete(ev)
        return []

    async def _on_allocate(self, ev: LogEvent) -> list[Out]:
        out: list[Out] = []
        run = self.runs.get(ev.path)
        if run is None or run.closed:
            run = self.runs[ev.path] = Run(ev.path, created=False)
        if not run.created:
            run.created = True
            out.append(_step(run, flows.STEP_CREATE))  # suy ra, xem docstring
        run.blocks.append(ev.block)
        run.touched = time.monotonic()
        dn = await self.resolve(ev.replicas[0])
        out += [_op("addBlock", ev.path, ev.block, dn), _step(run, flows.STEP_ADD_BLOCK), _step(run, flows.STEP_LOCATIONS)]
        return out

    async def _on_complete(self, ev: LogEvent) -> list[Out]:
        run = self.runs.get(ev.path)
        if run is None or run.closed:
            return [_op("close", ev.path)]  # đóng file mà ta không thấy lúc ghi (bắt đầu nghe giữa chừng, file rỗng...)
        out: list[Out] = []
        blocks = await self.fetch_blocks(ev.path)
        total = None
        if blocks is not None:
            by_id = {b["blockId"]: b for b in blocks}
            total = 0
            for bid in run.blocks:
                b = by_id.get(bid)
                if b is None or not b["locations"]:
                    continue  # NameNode chưa có bản sao nào của block này: không xác nhận được
                total += b["lengthBytes"]
                out += [_step(run, flows.STEP_SEND), _step(run, flows.STEP_ACK), _step(run, flows.STEP_RECEIVED)]
        out += [_op("close", ev.path, nbytes=total), _step(run, flows.STEP_COMPLETE)]
        run.closed, run.touched = True, time.monotonic()
        if ev.path.endswith(COPYING):
            if self.audit:
                return out  # giữ run, chờ dòng audit rename
            final = ev.path[: -len(COPYING)]
            if await self.path_exists(final):
                out += [_op("rename", final), _step(run, flows.STEP_RENAME)]
        del self.runs[ev.path]
        return out

    def _on_rename(self, ev: LogEvent) -> list[Out]:
        dst = ev.dst or ev.path
        out = [_op("rename", dst)]
        run = self.runs.get(ev.path)
        if run is not None and run.closed and ev.path.endswith(COPYING):
            out.append(_step(run, flows.STEP_RENAME))
            del self.runs[ev.path]
        return out


def live_tracker(audit_enabled: bool) -> WriteTracker:
    """WriteTracker nối với NameNode thật (chỉ đọc). Có thử lại ngắn vì NameNode cập nhật bảng block/đổi tên sau vài chục ms."""
    import asyncio

    from .clients import hdfs

    dn_names: dict[str, str] = {}
    last_refresh = [0.0]

    async def resolve(addr: str) -> str:
        if addr not in dn_names and time.monotonic() - last_refresh[0] > 5:
            last_refresh[0] = time.monotonic()
            try:
                info = await hdfs.jmx("Hadoop:service=NameNode,name=NameNodeInfo")
                for name, n in {**hdfs.parse_json_bean_field(info, "LiveNodes"), **hdfs.parse_json_bean_field(info, "DeadNodes")}.items():
                    if n.get("xferaddr"):
                        dn_names[n["xferaddr"]] = name
            except (hdfs.HdfsUnreachable, hdfs.HdfsError):
                pass
        return dn_names.get(addr, addr)

    async def fetch_blocks(path: str):
        candidates = [path] + ([path[: -len(COPYING)]] if path.endswith(COPYING) else [])
        result = None
        for _ in range(6):
            for p in candidates:
                try:
                    located = await hdfs.block_locations(p)
                except hdfs.HdfsNotFound:
                    continue
                except (hdfs.HdfsUnreachable, hdfs.HdfsError):
                    return None
                result = [{"blockId": f"blk_{b['block']['blockId']}", "lengthBytes": int(b["block"]["numBytes"]),
                           "locations": [f"{l['hostName']}:{l['xferPort']}" for l in b["locations"]]} for b in located]
                break
            if result is not None and all(b["locations"] for b in result):
                return result
            await asyncio.sleep(0.2)
        return result

    async def path_exists(path: str) -> bool:
        for _ in range(10):
            try:
                await hdfs.file_status(path)
                return True
            except hdfs.HdfsNotFound:
                await asyncio.sleep(0.2)
            except (hdfs.HdfsUnreachable, hdfs.HdfsError):
                return False
        return False

    return WriteTracker(fetch_blocks, path_exists, resolve, audit_enabled)
