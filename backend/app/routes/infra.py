"""/api/kafka/topics và /api/spark/applications."""
from fastapi import APIRouter

from .. import config, kafka_lag
from ..clients import kafka, spark
from ..errors import ApiError

router = APIRouter()


@router.get("/api/kafka/topics")
async def kafka_topics():
    try:
        topics = await kafka_lag.snapshot()
    except kafka.KafkaUnreachable as e:
        raise ApiError(503, "KAFKA_UNREACHABLE", f"Không nối được Kafka {config.KAFKA_BOOTSTRAP} ({e})")
    keep = ("name", "partitions", "replicationFactor", "approxMessages", "consumerGroups")
    return {"bootstrapServers": config.KAFKA_BOOTSTRAP, "topics": [{k: t[k] for k in keep} for t in topics]}


@router.get("/api/spark/applications")
async def spark_applications():
    return {"applications": await spark.applications()}
