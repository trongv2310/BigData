"""Phát sự kiện thật cho /ws/stream. Chỉ chạy các vòng thăm dò khi có ít nhất một người nghe."""
import asyncio
import logging
import time

from . import config, hadoop_logs, hdfs_flow, services, stream_input
from .clients import hdfs, kafka

log = logging.getLogger("hub")

STREAM_POLL_SECONDS = 1.0
SERVICE_POLL_SECONDS = 5.0
KAFKA_POLL_SECONDS = 5.0
LOG_POLL_SECONDS = 0.3
QUEUE_MAX = 200


class Hub:
    def __init__(self):
        self._subs: set[asyncio.Queue] = set()
        self._tasks: list[asyncio.Task] = []
        self.service_states: dict[str, dict] = {}

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(QUEUE_MAX)
        self._subs.add(q)
        if not self._tasks:
            self._tasks = [asyncio.create_task(self._poll_stream_input()), asyncio.create_task(self._poll_services()),
                           asyncio.create_task(self._poll_kafka()), asyncio.create_task(self._poll_hdfs_logs())]
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subs.discard(q)
        if not self._subs:
            for t in self._tasks:
                t.cancel()
            self._tasks = []

    def publish(self, type_: str, data: dict) -> None:
        for q in self._subs:
            if q.full():
                q.get_nowait()  # chậm quá thì bỏ tin cũ nhất, không để hàng đợi phình
            q.put_nowait((type_, data))

    async def snapshot_services(self) -> list[dict]:
        if not self.service_states:
            for s in await services.check_all():
                self.service_states[s["name"]] = s
        return list(self.service_states.values())

    async def _poll_services(self):
        while True:
            try:
                for s in await services.check_all():
                    old = self.service_states.get(s["name"])
                    self.service_states[s["name"]] = s
                    if old is not None and old["state"] != s["state"]:
                        self.publish("service.state", s)
            except Exception:
                log.exception("lỗi khi kiểm tra dịch vụ")
            await asyncio.sleep(SERVICE_POLL_SECONDS)

    async def _poll_hdfs_logs(self):
        """hdfs.op và flow.step từ log NameNode (và audit log nếu người dùng đã bật). Chỉ đọc file, bắt đầu từ cuối file."""
        nn, audit = hadoop_logs.namenode_tailer(), hadoop_logs.audit_tailer()
        nn.read_new(), audit.read_new()  # đặt mốc ở cuối file
        tracker = hdfs_flow.live_tracker(audit.exists())
        while True:
            try:
                tracker.audit = audit.exists()
                events = [e for e in map(hadoop_logs.parse_line, nn.read_new() + audit.read_new()) if e]
                for ev in sorted(events, key=lambda e: e.ts):
                    for type_, data in await tracker.feed(ev):
                        self.publish(type_, data)
            except Exception:
                log.exception("lỗi khi đọc log NameNode")
            await asyncio.sleep(LOG_POLL_SECONDS)

    async def _poll_kafka(self):
        """kafka.stats mỗi KAFKA_POLL_SECONDS: tốc độ tin/giây (từ độ tăng offset giữa hai lần hỏi) và tổng lag. Lần hỏi đầu chỉ lấy mốc."""
        prev: tuple[float, int] | None = None
        while True:
            try:
                topic = next((t for t in await kafka.snapshot() if t["name"] == config.KAFKA_TOPIC), None)
                now = time.monotonic()
                if topic is not None:
                    total = topic["endOffsetTotal"]
                    if prev is not None:
                        rate = max(total - prev[1], 0) / max(now - prev[0], 1e-6)
                        lag = sum(g["lag"] for g in topic["consumerGroups"])
                        self.publish("kafka.stats", {"topic": topic["name"], "messagesPerSecond": round(rate, 2), "lagTotal": lag})
                    prev = (now, total)
            except kafka.KafkaUnreachable:
                prev = None
            except Exception:
                log.exception("lỗi khi hỏi Kafka")
            await asyncio.sleep(KAFKA_POLL_SECONDS)

    async def _poll_stream_input(self):
        """Báo `reading` cho mỗi file mới xuất hiện trong /stream_input (file đã có lúc bắt đầu nghe thì bỏ qua)."""
        mark: tuple[int, set[str]] | None = None  # (mtime lớn nhất đã thấy, các tên có mtime đó)
        while True:
            try:
                files = await stream_input.list_files()
                if mark is None:
                    top = max((f["modificationTime"] for f in files), default=0)
                    mark = (top, {f["pathSuffix"] for f in files if f["modificationTime"] == top})
                else:
                    new = [f for f in files if f["modificationTime"] > mark[0]
                           or (f["modificationTime"] == mark[0] and f["pathSuffix"] not in mark[1])]
                    for f in new:
                        r = await stream_input.read_reading(f["pathSuffix"])
                        if r:
                            self.publish("reading", r)
                    if new:
                        top = max(f["modificationTime"] for f in new)
                        names = {f["pathSuffix"] for f in new if f["modificationTime"] == top}
                        mark = (top, names | (mark[1] if top == mark[0] else set()))
            except hdfs.HdfsUnreachable:
                mark = None  # HDFS tắt rồi bật lại: đặt mốc mới, không phát lại cả đống file cũ
            except Exception:
                log.exception("lỗi khi đọc /stream_input")
            await asyncio.sleep(STREAM_POLL_SECONDS)


hub = Hub()
