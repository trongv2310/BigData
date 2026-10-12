"""spark.progress từ Spark REST (CHỈ ĐỌC): mỗi micro-batch của Structured Streaming là một (hoặc vài) job có mô tả "id = <query> / runId = ... / batch = N".

Spark REST không lộ StreamingQueryProgress, nên các số dưới đây là SUY RA từ job và stage, không phải số của Spark:
  - numInputRows           = tổng inputRecords của các stage thuộc batch (đúng cho truy vấn một nguồn như spark_streaming_consumer.py);
  - processedRowsPerSecond = numInputRows / (từ lúc job đầu nộp đến lúc job cuối xong); chưa tính lập kế hoạch và commit, nên cao hơn số của Spark;
  - inputRowsPerSecond     = numInputRows / (thời gian giữa lúc nộp batch này và batch trước); 0 nếu chưa có batch trước;
  - queryName              = tên ứng dụng (REST không trả tên truy vấn; script không đặt tên).
Lần hỏi đầu của mỗi ứng dụng chỉ đặt mốc (không phát lại lịch sử).
"""
import re
from datetime import datetime

from . import config
from .clients import spark

BATCH_RE = re.compile(r"id = (\S+)\s+runId = (\S+)\s+batch = (\d+)")
MAX_EVENTS_PER_POLL = 20  # events.md: không quá ~20 tin/giây; chạy bù nhiều batch thì chỉ báo 20 batch mới nhất


def _ms(s: str) -> int:
    """'2026-10-12T14:04:55.036GMT' -> epoch ms."""
    return int(datetime.strptime(s.replace("GMT", "+0000"), "%Y-%m-%dT%H:%M:%S.%f%z").timestamp() * 1000)


class ProgressTracker:
    def __init__(self):
        self._mark: dict[str, int] = {}          # "cổng/appId" -> jobId lớn nhất đã xử lý
        self._prev_start: dict[tuple, int] = {}  # (cổng/appId, queryId) -> thời điểm nộp của batch trước (ms)

    async def poll(self) -> list[dict]:
        out: list[dict] = []
        for port in config.SPARK_UI_PORTS:
            base = f"http://localhost:{port}"
            for app in await spark._get(f"{base}/api/v1/applications") or []:
                if not app.get("attempts") or app["attempts"][0].get("completed"):
                    continue
                out += await self._poll_app(base, app)
        return out[-MAX_EVENTS_PER_POLL:]

    async def _poll_app(self, base: str, app: dict) -> list[dict]:
        key = f"{base}/{app['id']}"
        jobs = await spark._get(f"{base}/api/v1/applications/{app['id']}/jobs")
        if jobs is None:
            return []
        batches = [(j, BATCH_RE.search(j.get("description") or "")) for j in jobs]
        batches = [(j, m) for j, m in batches if m]
        if not batches:
            self._mark.setdefault(key, -1)  # ứng dụng đã có nhưng chưa có batch: các batch đầu tiên sau này không phải "lịch sử"
            return []
        first_poll = key not in self._mark
        mark = self._mark.get(key, -1)
        running = [j["jobId"] for j, _ in batches if j.get("status") not in ("SUCCEEDED", "FAILED")]
        # chỉ lấy job đã xong; không đẩy mốc qua job còn chạy để lần sau còn thấy nó xong
        ceiling = (min(running) - 1) if running else max(j["jobId"] for j, _ in batches)
        done = [(j, m) for j, m in batches if mark < j["jobId"] <= ceiling and j.get("status") == "SUCCEEDED"]
        self._mark[key] = max(mark, ceiling)

        grouped: dict[tuple[str, int], list[dict]] = {}
        for j, m in sorted(done, key=lambda x: x[0]["jobId"]):
            grouped.setdefault((m.group(1), int(m.group(3))), []).append(j)

        events = []
        for (query, batch), js in sorted(grouped.items(), key=lambda kv: kv[0][1]):
            start = min(_ms(j["submissionTime"]) for j in js)
            end = max(_ms(j["completionTime"]) for j in js if j.get("completionTime")) if any(j.get("completionTime") for j in js) else start
            prev = self._prev_start.get((key, query))
            self._prev_start[(key, query)] = start
            if first_poll:
                continue  # chỉ đặt mốc
            rows = 0
            for j in js:
                for sid in j.get("stageIds", []):
                    st = await spark._get(f"{base}/api/v1/applications/{app['id']}/stages/{sid}")
                    if st:
                        rows += int((st[0] if isinstance(st, list) else st).get("inputRecords") or 0)
            dur = max(end - start, 1) / 1000
            gap = (start - prev) / 1000 if prev is not None and start > prev else 0
            events.append({
                "queryName": app.get("name") or query,
                "batchId": batch,
                "numInputRows": rows,
                "inputRowsPerSecond": round(rows / gap, 2) if gap else 0.0,
                "processedRowsPerSecond": round(rows / dur, 2),
            })
        return events
