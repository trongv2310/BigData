"""/api/analysis/*: kết quả bước 5 (Spark SQL) và 6a (phân cụm ngày) của pipeline, đọc từ /co/output trên HDFS (chỉ đọc, trả nguyên nội dung do pipeline ghi).

Backend không tính lại và không sửa số nào. Chưa có tệp (chưa chạy bước đó) thì 404 ANALYSIS_NOT_READY; HDFS tắt hoặc tệp hỏng thì 503 ANALYSIS_UNAVAILABLE,
không đoán và không trả số cũ nào khác.
"""
import json

from fastapi import APIRouter

from .. import config
from ..clients import hdfs
from ..errors import ApiError

router = APIRouter()


async def _read(path: str, required: bool) -> dict | None:
    """JSON trong HDFS. Không có tệp: ném 404 nếu required, ngược lại trả None."""
    try:
        doc = json.loads(await hdfs.read_text(path))
    except hdfs.HdfsNotFound:
        if required:
            raise ApiError(404, "ANALYSIS_NOT_READY", f"Chưa có {path} trên HDFS; chạy bước tương ứng của pipeline (xem /api/pipeline/steps).")
        return None
    except (hdfs.HdfsUnreachable, hdfs.HdfsError) as e:
        raise ApiError(503, "ANALYSIS_UNAVAILABLE", f"Không đọc được {path} trên HDFS ({type(e).__name__}).")
    except ValueError:
        raise ApiError(503, "ANALYSIS_UNAVAILABLE", f"{path} không phải JSON hợp lệ.")
    if not isinstance(doc, dict):
        raise ApiError(503, "ANALYSIS_UNAVAILABLE", f"{path} không phải một đối tượng JSON.")
    return doc


@router.get("/api/analysis/sql")
async def analysis_sql():
    return await _read(config.ANALYSIS_SQL_PATH, True)


@router.get("/api/analysis/day-clusters")
async def analysis_day_clusters():
    doc = await _read(config.ANALYSIS_CLUSTERS_PATH, True)
    doc["stability"] = await _read(config.ANALYSIS_CLUSTERS_STABILITY_PATH, False)  # null = chưa chạy 06a_cluster_stability.py
    return doc
