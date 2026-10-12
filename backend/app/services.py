"""Kiểm tra thật các dịch vụ: gọi HTTP/TCP tới từng cổng, không đoán."""
import asyncio
import time

import httpx

from . import config, timeutil
from .clients import hdfs

_cache: tuple[float, list[dict]] | None = None
CACHE_SECONDS = 2.0


def _status(name, kind, state, url, detail):
    return {"name": name, "kind": kind, "state": state, "url": url, "checkedAt": timeutil.now_iso(), "detail": detail}


async def _http_json(url: str):
    """(ok, json hoặc None). ok=False khi không nối được hoặc mã khác 200."""
    try:
        r = await hdfs.client().get(url, timeout=config.PROBE_TIMEOUT)
        if r.status_code != 200:
            return False, None
        try:
            return True, r.json()
        except ValueError:
            return True, None
    except httpx.HTTPError:
        return False, None


async def _tcp_open(host: str, port: int) -> bool:
    try:
        _, w = await asyncio.wait_for(asyncio.open_connection(host, port), config.PROBE_TIMEOUT)
        w.close()
        return True
    except (OSError, asyncio.TimeoutError):
        return False


async def _namenode():
    url = config.NAMENODE_HTTP
    try:
        info = await hdfs.jmx("Hadoop:service=NameNode,name=NameNodeInfo")
    except (hdfs.HdfsUnreachable, hdfs.HdfsError):
        return _status("NameNode", "namenode", "down", url, "Không nối được JMX"), None
    safe = bool(info.get("Safemode"))
    return _status("NameNode", "namenode", "up", url, "hdfs://localhost:9000" + (" (đang ở Safe Mode)" if safe else "")), info


async def _datanode(info):
    ports = config.DATANODE_HTTP_PORTS
    oks = await asyncio.gather(*[_tcp_open("127.0.0.1", p) for p in ports])
    k = sum(oks)
    detail = f"{k}/{len(ports)} cổng HTTP phản hồi"
    if info is not None:
        live = len(hdfs.parse_json_bean_field(info, "LiveNodes"))
        dead = len(hdfs.parse_json_bean_field(info, "DeadNodes"))
        detail += f"; NameNode báo {live} live, {dead} dead"
    return _status("DataNode", "datanode", "up" if k else "down", f"http://localhost:{ports[0]}", detail)


async def _yarn():
    base = config.YARN_HTTP
    ok, _ = await _http_json(f"{base}/ws/v1/cluster/info")
    rm = _status("ResourceManager", "resourcemanager", "up" if ok else "down", base, None)
    if not ok:
        nm = _status("NodeManager", "nodemanager", "unknown", None, "Không hỏi được ResourceManager nên không biết")
        return rm, nm
    ok2, data = await _http_json(f"{base}/ws/v1/cluster/nodes")
    nodes = ((data or {}).get("nodes") or {}).get("node") or []
    running = sum(1 for n in nodes if n.get("state") == "RUNNING")
    nm = _status("NodeManager", "nodemanager", "up" if running else "down", None, f"{running} node RUNNING theo ResourceManager")
    return rm, nm


async def _kafka():
    host, _, port = config.KAFKA_BOOTSTRAP.partition(":")
    up = await _tcp_open(host or "localhost", int(port or 9092))
    return _status("Kafka", "kafka", "up" if up else "down", None, f"{config.KAFKA_BOOTSTRAP}, topic {config.KAFKA_TOPIC}")


async def _spark():
    for p in config.SPARK_UI_PORTS:
        ok, data = await _http_json(f"http://localhost:{p}/api/v1/applications")
        if ok and isinstance(data, list) and data:
            a = data[0]
            return _status("Spark Streaming", "spark", "up", f"http://localhost:{p}", f"{a.get('name')} ({a.get('id')})")
    return _status("Spark Streaming", "spark", "unknown", f"http://localhost:{config.SPARK_UI_PORTS[0]}", "Chưa có ứng dụng nào đang chạy")


async def _airflow():
    ok, _ = await _http_json(f"{config.AIRFLOW_HTTP}/health")
    return _status("Airflow", "airflow", "up" if ok else "down", config.AIRFLOW_HTTP, "DAG hcmc_co_pipeline")


async def check_all() -> list[dict]:
    global _cache
    if _cache and time.monotonic() - _cache[0] < CACHE_SECONDS:
        return _cache[1]
    (nn, info), (rm, nm), kafka, spark, airflow = await asyncio.gather(_namenode(), _yarn(), _kafka(), _spark(), _airflow())
    dn = await _datanode(info)
    backend = _status("Backend", "backend", "up", "http://localhost:8000", f"FastAPI, luồng /ws/stream: {config.STREAM_MODE}" + ("" if config.STREAM_SOURCE == "hdfs" else f", nguồn reading: {config.STREAM_SOURCE}"))
    result = [nn, dn, rm, nm, kafka, spark, airflow, backend]
    _cache = (time.monotonic(), result)
    return result
