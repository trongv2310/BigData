"""Lớp chặn mọi thao tác GHI lên HDFS, Kafka, Spark.

Backend mặc định chỉ đọc. Hiện chưa route nào ghi. Khi sau này cần ghi (ví dụ P4/P5), mã ghi phải gọi
`require_writes("mô tả")` trước; thao tác chỉ chạy khi đặt BACKEND_ALLOW_WRITES=1.
"""
from . import config


class WritesDisabled(RuntimeError):
    pass


def require_writes(action: str) -> None:
    if not config.ALLOW_WRITES:
        raise WritesDisabled(f"Backend đang ở chế độ chỉ đọc, từ chối: {action} (đặt BACKEND_ALLOW_WRITES=1 để bật)")
