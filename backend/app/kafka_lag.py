"""Độ trễ của Spark Structured Streaming đọc Kafka, tính từ checkpoint trên HDFS (CHỈ ĐỌC).

Spark đọc Kafka không tham gia consumer group và không commit offset lên Kafka, nên `kafka-consumer-groups` không thấy nó.
Vị trí thật của Spark nằm trong checkpoint: `commits/N` có mặt nghĩa là batch N đã xong, và dòng cuối của `offsets/N` là
{"topic": {"partition": offset_kế_tiếp}}. Độ trễ = end offset hiện tại của Kafka − offset kế tiếp đó.
Trong consumerGroups nó hiện với tên "spark:<tên thư mục checkpoint>" để không bị nhầm với consumer group thật.
"""
import json

from . import config
from .clients import hdfs, kafka


def parse_offsets(text: str) -> dict[str, dict[int, int]]:
    """Nội dung file offsets/N -> {topic: {partition: offset}}; thiếu hoặc hỏng thì {}."""
    for line in reversed([l.strip() for l in text.splitlines() if l.strip()]):
        if line.startswith("{") and not line.startswith('{"batchWatermarkMs"'):
            try:
                return {t: {int(p): int(o) for p, o in parts.items()} for t, parts in json.loads(line).items()}
            except (ValueError, AttributeError, TypeError):
                return {}
    return {}


async def committed_offsets(checkpoint: str) -> dict[str, dict[int, int]] | None:
    """Offset kế tiếp của batch đã commit mới nhất; None nếu chưa có checkpoint hoặc chưa có batch nào xong."""
    try:
        done = [int(e["pathSuffix"]) for e in await hdfs.list_status(f"{checkpoint}/commits") if e["pathSuffix"].isdigit()]
        if not done:
            return None
        return parse_offsets(await hdfs.read_text(f"{checkpoint}/offsets/{max(done)}"))
    except (hdfs.HdfsNotFound, hdfs.HdfsUnreachable, hdfs.HdfsError):
        return None


async def add_spark_groups(topics: list[dict]) -> list[dict]:
    """Thêm vào consumerGroups của mỗi topic một mục cho mỗi checkpoint Spark có đọc topic đó."""
    for ckpt in config.SPARK_KAFKA_CHECKPOINTS:
        committed = await committed_offsets(ckpt)
        if not committed:
            continue
        name = "spark:" + ckpt.rstrip("/").rsplit("/", 1)[-1]
        for t in topics:
            offs, ends = committed.get(t["name"]), t.get("endOffsets") or {}
            if offs:
                t["consumerGroups"].append({"group": name, "lag": sum(max(ends.get(p, 0) - o, 0) for p, o in offs.items())})
    return topics


async def snapshot() -> list[dict]:
    """kafka.snapshot() cộng độ trễ của Spark. Ném kafka.KafkaUnreachable như bản gốc."""
    return await add_spark_groups(await kafka.snapshot())
