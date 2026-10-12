"""Client Kafka CHỈ ĐỌC (kafka-python): topic, offset, độ trễ consumer group. Không tạo topic, không gửi tin, không commit offset."""
import asyncio
import socket
import threading

from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from .. import config

_lock = threading.Lock()
_admin: KafkaAdminClient | None = None
_consumer: KafkaConsumer | None = None


class KafkaUnreachable(Exception):
    pass


def _reset():
    global _admin, _consumer
    for c in (_admin, _consumer):
        try:
            if c is not None:
                c.close()
        except Exception:
            pass
    _admin = _consumer = None


def _clients():
    global _admin, _consumer
    if _admin is None:
        _admin = KafkaAdminClient(bootstrap_servers=config.KAFKA_BOOTSTRAP, request_timeout_ms=4000, api_version_auto_timeout_ms=3000)
        # group_id=None: consumer chỉ dùng để hỏi offset, không tham gia group, không commit
        _consumer = KafkaConsumer(bootstrap_servers=config.KAFKA_BOOTSTRAP, group_id=None, enable_auto_commit=False,
                                  request_timeout_ms=4000, api_version_auto_timeout_ms=3000)
    return _admin, _consumer


def _broker_listening() -> bool:
    host, _, port = config.KAFKA_BOOTSTRAP.partition(":")
    try:
        with socket.create_connection((host or "localhost", int(port or 9092)), timeout=config.PROBE_TIMEOUT):
            return True
    except OSError:
        return False


def _snapshot_sync() -> list[dict]:
    if not _broker_listening():  # nhanh hơn chờ kafka-python tự báo NoBrokersAvailable
        raise KafkaUnreachable("cổng không mở")
    with _lock:
        try:
            admin, consumer = _clients()
            names = sorted(t for t in admin.list_topics() if not t.startswith("__"))
            metas = {m["topic"]: m for m in admin.describe_topics(names)} if names else {}
            groups = [g[0] for g in admin.list_consumer_groups()]
            committed = {g: admin.list_consumer_group_offsets(g) for g in groups}
            topics = []
            for name in names:
                parts = [TopicPartition(name, p["partition"]) for p in metas[name]["partitions"]]
                begin, end = consumer.beginning_offsets(parts), consumer.end_offsets(parts)
                lags = []
                for g, offs in committed.items():
                    mine = {tp: o for tp, o in offs.items() if tp.topic == name}
                    if mine:
                        lags.append({"group": g, "lag": sum(max(end[tp] - o.offset, 0) for tp, o in mine.items() if tp in end)})
                topics.append({
                    "name": name,
                    "partitions": len(parts),
                    "replicationFactor": max(len(metas[name]["partitions"][0]["replicas"]), 1) if parts else 1,
                    "approxMessages": sum(end[tp] - begin[tp] for tp in parts),
                    "endOffsetTotal": sum(end[tp] for tp in parts),
                    "consumerGroups": lags,
                })
            return topics
        except KafkaError as e:
            _reset()
            raise KafkaUnreachable(f"{type(e).__name__}") from e
        except Exception as e:  # kafka-python đôi khi ném lỗi khác khi broker tắt giữa chừng
            _reset()
            raise KafkaUnreachable(f"{type(e).__name__}: {e}") from e


async def snapshot() -> list[dict]:
    """Mỗi topic: name, partitions, replicationFactor, approxMessages, endOffsetTotal, consumerGroups[{group, lag}]."""
    return await asyncio.to_thread(_snapshot_sync)
