"""Giờ Việt Nam. Hợp đồng luôn dùng ISO 8601 có +07:00; cột datetime của bộ Kaggle là UTC."""
from datetime import datetime, timedelta, timezone

VN = timezone(timedelta(hours=7))


def iso(dt: datetime) -> str:
    """datetime (có hoặc không có múi giờ; không có thì coi là giờ hệ thống) -> '2026-10-12T12:00:00+07:00'."""
    return dt.astimezone(VN).isoformat(timespec="seconds")


def now_iso() -> str:
    return iso(datetime.now(VN))


def from_ms(ms: int) -> str:
    return iso(datetime.fromtimestamp(ms / 1000, VN))


def parse_query_time(s: str) -> datetime:
    """Nhận ISO 8601; không có offset thì coi là +07:00. Sai định dạng -> ValueError."""
    s = s.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=VN)
