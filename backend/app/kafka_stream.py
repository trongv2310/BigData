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
LATEST_SCAN = 5      # khi tìm tin mới nhất: đọc lùi tối đa ngần này tin mỗi partition (tin hỏng thì thử tin trước)
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
    parts = c.partitions_for_topic(config.KAFKA_TOPIC)
    return [TopicPartition(config.KAFKA_TOPIC, p) for p in sorted(parts or [])]


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


def _latest(c: KafkaConsumer) -> dict | None:
    best, best_ts = None, -1
    for tp in _partitions(c):
        c.assign([tp])
        begin, end = c.beginning_offsets([tp])[tp], c.end_offsets([tp])[tp]
        if end <= begin:
            continue
        c.seek(tp, max(begin, end - LATEST_SCAN))
        got = []
        while True:
            batch = c.poll(timeout_ms=1500, max_records=LATEST_SCAN).get(tp, [])
            got += batch
            if not batch or c.position(tp) >= end:
                break
        for m in reversed(got):  # tin mới nhất hợp lệ của partition
            r = _parse(m.value)
            if r:
                if m.timestamp > best_ts:
                    best, best_ts = r, m.timestamp
                break
    return best


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


async def latest_reading() -> dict | None:
    """AqiReading của tin mới nhất trong topic; None nếu topic trống hoặc chưa có. Ném KafkaUnreachable nếu Kafka tắt."""
    return await asyncio.to_thread(_call, _latest)


async def read_new(positions: dict[int, int]) -> tuple[list[dict], dict[int, int]]:
    """Các AqiReading mới từ vị trí `positions` (partition -> offset kế tiếp) và vị trí mới."""
    return await asyncio.to_thread(_call, lambda c: _read_from(c, positions))
