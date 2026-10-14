"""/api/model/metrics: chỉ số mô hình dự đoán CO do pipeline/06b_train.py ghi vào /co/output/model_metrics.json (backend chỉ đọc, trả nguyên văn, không tính lại).

Chưa có tệp (chưa huấn luyện) thì 404 MODEL_NOT_TRAINED; HDFS tắt hoặc tệp hỏng thì 503 MODEL_UNAVAILABLE, không đoán.
"""
import json

from fastapi import APIRouter

from .. import config
from ..clients import hdfs
from ..errors import ApiError

router = APIRouter()


@router.get("/api/model/metrics")
async def model_metrics():
    path = config.MODEL_METRICS_PATH
    try:
        doc = json.loads(await hdfs.read_text(path))
    except hdfs.HdfsNotFound:
        raise ApiError(404, "MODEL_NOT_TRAINED", f"Chưa có {path} trên HDFS: chưa huấn luyện mô hình (bước 6b của pipeline). Xem /api/pipeline/steps.")
    except (hdfs.HdfsUnreachable, hdfs.HdfsError) as e:
        raise ApiError(503, "MODEL_UNAVAILABLE", f"Không đọc được {path} trên HDFS ({type(e).__name__}); không biết mô hình đã huấn luyện chưa.")
    except ValueError:
        raise ApiError(503, "MODEL_UNAVAILABLE", f"{path} không phải JSON hợp lệ.")
    if not isinstance(doc, dict):
        raise ApiError(503, "MODEL_UNAVAILABLE", f"{path} không phải một đối tượng JSON.")
    return doc
