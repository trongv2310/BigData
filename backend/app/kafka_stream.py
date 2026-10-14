"""Đọc luồng `reading` thật từ Kafka (quyết định D1 phương án b). CHỈ ĐỌC: consumer không tham gia group, không commit offset, không tạo topic.

Giá trị mỗi tin cùng dạng bản ghi của luồng file-stream cũ: {datetime, station, co, wind_speed_kmh, temperature_C, ...}
nên dùng lại reading.from_record để đổi sang AqiReading của hợp đồng.
"""
import asyncio
import json
import threading

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from . import config, reading
from .clients.kafka import KafkaUnreachable, _broker_listening

_lock = threading.Lock()
_consumer: KafkaConsumer | None = None
POLL_MAX_RECORDS = 200


def _reset() -> None:
    global _consumer
    try:
        if _consumer is not None:
            _consumer.close()
    except Exception:
        pass
    _consumer = None


def _get() -> KafkaConsumer:
    global _consumer
    if _consumer is None:
        _consumer = KafkaConsumer(bootstrap_servers=config.KAFKA_BOOTSTRAP, group_id=None, enable_auto_commit=False,
                                  request_timeout_ms=5000, api_version_auto_timeout_ms=3000)
    return _consumer


def _parse(value: bytes | None) -> dict | None:
    """Giá trị tin -> AqiReading; tin hỏng hoặc thiếu trường thì None (bỏ qua, không làm sập luồng)."""
    if not value:
        return None
    try:
        return reading.from_record(json.loads(value.decode("utf-8")))
    except (ValueError, KeyError, TypeError):
        return None


def _partitions(c: KafkaConsumer) -> list[TopicPartition]:
    parts = c.partitions_for_topic(config.READING_TOPIC)
    return [TopicPartition(config.READING_TOPIC, p) for p in sorted(parts or [])]


def _call(fn):
    if not _broker_listening():
        raise KafkaUnreachable("cổng không mở")
    with _lock:
        try:
            return fn(_get())
        except KafkaUnreachable:
            raise
        except KafkaError as e:
            _reset()
            raise KafkaUnreachable(type(e).__name__) from e
        except Exception as e:  # kafka-python đôi khi ném lỗi khác khi broker tắt giữa chừng
            _reset()
            raise KafkaUnreachable(f"{type(e).__name__}: {e}") from e


def _end_offsets(c: KafkaConsumer) -> dict[int, int]:
    parts = _partitions(c)
    ends = c.end_offsets(parts) if parts else {}
    return {tp.partition: off for tp, off in ends.items()}


# Bản ghi mới nhất THEO TỪNG NGUỒN. Topic chứa lẫn bản phát lại (có thể hàng chục tin mỗi giây ở tốc độ 60x) và dữ liệu thật (vài giờ/lần
# hoặc ít hơn), nên "tin cuối cùng của topic" gần như luôn là bản phát lại. Giữ bộ nhớ đệm (nguồn -> bản ghi mới nhất) và chỉ đọc phần tin
# mới từ lần hỏi trước; nguồn nào chưa có trong đệm thì đọc lùi từng đoạn BACKFILL_CHUNK tin, tối đa BACKFILL_MAX tin mỗi lần hỏi.
BACKFILL_CHUNK = 500
BACKFILL_MAX = 20000
_next: dict[int, int] = {}    # partition -> offset kế tiếp cần đọc (phần phía trước đã vào đệm)
_low: dict[int, int] = {}     # partition -> offset thấp nhất đã quét lùi
_newest: dict[str, tuple[int, dict]] = {}  # source -> (kafka timestamp ms, AqiReading)


def reset_latest_cache() -> None:
    _next.clear(), _low.clear(), _newest.clear()


def _read_range(c: KafkaConsumer, tp: TopicPartition, lo: int, hi: int) -> list[tuple[int, dict]]:
    """[(kafka timestamp, AqiReading)] của các tin hợp lệ trong [lo, hi), cũ -> mới."""
    out: list[tuple[int, dict]] = []
    c.assign([tp])
    c.seek(tp, lo)
    while c.position(tp) < hi:
        batch = c.poll(timeout_ms=1500, max_records=BACKFILL_CHUNK).get(tp, [])
        if not batch:
            break
        for m in batch:
            if m.offset >= hi:
                break
            r = _parse(m.value)
            if r:
                out.append((m.timestamp, r))
    return out


def _absorb(items: list[tuple[int, dict]]) -> None:
    for ts, r in items:
        cur = _newest.get(r["source"])
        if cur is None or ts >= cur[0]:
            _newest[r["source"]] = (ts, r)


def _latest(c: KafkaConsumer, source: str | None = None) -> dict | None:
    budget = BACKFILL_MAX
    for tp in _partitions(c):
        begin, end = c.beginning_offsets([tp])[tp], c.end_offsets([tp])[tp]
        nxt = _next.get(tp.partition)
        if nxt is not None and not begin <= nxt <= end:  # topic bị xóa/tạo lại: đệm cũ không còn đúng
            reset_latest_cache()
            nxt = None
        if nxt is None:
            _low[tp.partition] = end
        elif end > nxt:
            _absorb(_read_range(c, tp, nxt, end))
        _next[tp.partition] = end
    # đọc lùi cho tới khi có thứ cần tìm (nguồn được hỏi, hoặc bất kỳ tin hợp lệ nào khi không hỏi nguồn)
    while (source not in _newest) if source else (not _newest):
        progressed = False
        for tp in _partitions(c):
            begin = c.beginning_offsets([tp])[tp]
            low = _low.get(tp.partition, begin)
            if low <= begin or budget <= 0:
                continue
            lo = max(begin, low - BACKFILL_CHUNK)
            _absorb(_read_range(c, tp, lo, low))
            _low[tp.partition], budget, progressed = lo, budget - (low - lo), True
        if not progressed:
            break
    if source:
        hit = _newest.get(source)
        return hit[1] if hit else None
    return max(_newest.values(), key=lambda x: x[0])[1] if _newest else None


def _read_from(c: KafkaConsumer, positions: dict[int, int]) -> tuple[list[dict], dict[int, int]]:
    out, new_pos = [], dict(positions)
    for tp in _partitions(c):
        start = positions.get(tp.partition)
        if start is None:  # partition mới xuất hiện sau lúc đặt mốc: đọc từ đầu
            start = c.beginning_offsets([tp])[tp]
        c.assign([tp])
        c.seek(tp, start)
        for m in c.poll(timeout_ms=500, max_records=POLL_MAX_RECORDS).get(tp, []):
            r = _parse(m.value)
            if r:
                out.append((m.timestamp, r))
            new_pos[tp.partition] = m.offset + 1
        new_pos.setdefault(tp.partition, start)
    return [r for _, r in sorted(out, key=lambda x: x[0])], new_pos


async def end_offsets() -> dict[int, int]:
    """Mốc 'bây giờ' của từng partition (để chỉ phát tin đến sau lúc bắt đầu nghe)."""
    return await asyncio.to_thread(_call, _end_offsets)


async def latest_reading(source: str | None = None) -> dict | None:
    """AqiReading mới nhất trong topic (của nguồn `source` nếu có); None nếu chưa có. Ném KafkaUnreachable nếu Kafka tắt."""
    return await asyncio.to_thread(_call, lambda c: _latest(c, source))


async def read_new(positions: dict[int, int]) -> tuple[list[dict], dict[int, int]]:
    """Các AqiReading mới từ vị trí `positions` (partition -> offset kế tiếp) và vị trí mới."""
    return await asyncio.to_thread(_call, lambda c: _read_from(c, positions))
