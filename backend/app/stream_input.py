"""Đọc luồng file-stream thật: các file JSON do producer đẩy vào HDFS /stream_input (quyết định D1, tạm)."""
from . import config, reading
from .clients import hdfs


def _is_data_file(st: dict) -> bool:
    n = st["pathSuffix"]
    return st["type"] == "FILE" and n.endswith(".json") and not n.endswith("._COPYING_")


async def list_files() -> list[dict]:
    """Các file dữ liệu trong /stream_input, cũ -> mới (theo thời điểm sửa, rồi theo tên)."""
    try:
        entries = await hdfs.list_status(config.HDFS_STREAM_DIR)
    except hdfs.HdfsNotFound:
        return []
    return sorted((e for e in entries if _is_data_file(e)), key=lambda e: (e["modificationTime"], e["pathSuffix"]))


async def read_reading(name: str) -> dict | None:
    try:
        text = await hdfs.read_text(f"{config.HDFS_STREAM_DIR}/{name}")
    except hdfs.HdfsNotFound:
        return None
    return reading.parse_file_text(text)


async def latest_reading() -> dict | None:
    """Bản ghi của file mới nhất trong /stream_input; None nếu thư mục trống. Ném HdfsUnreachable nếu HDFS tắt."""
    files = await list_files()
    for f in reversed(files[-5:]):  # file mới nhất có thể đang bị ghi dở; thử vài file gần nhất
        r = await read_reading(f["pathSuffix"])
        if r:
            return r
    return None
