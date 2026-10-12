"""Client Spark REST (UI của driver, cổng 4040 trở lên) CHỈ ĐỌC. Spark local[*] chỉ có UI khi ứng dụng đang chạy."""
import httpx

from .. import config, timeutil
from . import hdfs


async def _get(url: str):
    try:
        r = await hdfs.client().get(url, timeout=config.PROBE_TIMEOUT)
        return r.json() if r.status_code == 200 else None
    except (httpx.HTTPError, ValueError):
        return None


async def applications() -> list[dict]:
    """Quét các cổng UI; trả danh sách ứng dụng đúng dạng SparkApplication của hợp đồng."""
    out = []
    for port in config.SPARK_UI_PORTS:
        base = f"http://localhost:{port}"
        apps = await _get(f"{base}/api/v1/applications")
        for a in apps or []:
            if not a.get("attempts"):
                continue  # chưa có attempt thì chưa có thời điểm bắt đầu để báo, bỏ qua
            attempt = a["attempts"][0]
            jobs = await _get(f"{base}/api/v1/applications/{a['id']}/jobs") or []
            count = lambda s: sum(1 for j in jobs if j.get("status") == s)
            started = attempt["startTimeEpoch"]
            out.append({
                "id": a["id"],
                "name": a.get("name", ""),
                "state": "completed" if attempt.get("completed") else "running",
                "startedAt": timeutil.from_ms(started),
                "jobsRunning": count("RUNNING"),
                "jobsSucceeded": count("SUCCEEDED"),
                "jobsFailed": count("FAILED"),
                "uiUrl": base,
            })
    return out
