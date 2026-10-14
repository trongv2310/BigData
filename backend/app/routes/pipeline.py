"""/api/pipeline/steps: 9 bước của pipeline, đọc /co/output/step_summary.json trên HDFS (chỉ đọc)."""
from fastapi import APIRouter

from .. import config, pipeline_steps
from ..clients import hdfs
from ..errors import ApiError

router = APIRouter()


@router.get("/api/pipeline/steps")
async def pipeline_steps_route():
    try:
        doc = pipeline_steps.parse(await hdfs.read_text(config.PIPELINE_SUMMARY_PATH))
    except hdfs.HdfsNotFound:
        doc = None  # chưa chạy bước nào: cả 9 bước notStarted là sự thật
    except (hdfs.HdfsUnreachable, hdfs.HdfsError) as e:
        raise ApiError(503, "PIPELINE_UNAVAILABLE", f"Không đọc được {config.PIPELINE_SUMMARY_PATH} trên HDFS ({type(e).__name__}); không biết trạng thái pipeline.")
    except ValueError:
        raise ApiError(503, "PIPELINE_UNAVAILABLE", f"{config.PIPELINE_SUMMARY_PATH} không phải JSON hợp lệ; không đoán trạng thái pipeline.")
    return pipeline_steps.build(doc)
