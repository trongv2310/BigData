"""/api/predict: heuristic, KHÔNG phải mô hình học máy.

Thử nghiệm 2026-10-12 trên bộ Kaggle giai đoạn từ 01/07/2025 (giờ VN): giờ, tháng, nhiệt độ, tốc độ gió không làm giảm sai số so với
việc lấy trung bình chung (kiểm tra chéo xen kẽ theo tuần: R² xấp xỉ 0 hoặc âm cho mọi cách kết hợp; kiểm tra theo thời gian 60 ngày cuối cũng vậy).
Nên ở đây chỉ trả phân phối thực của CO giai đoạn hiện tại quanh giờ được hỏi: trung vị và khoảng 10%-90% của các giá trị quan sát
trong cửa sổ ±1 giờ. Các trường còn lại của yêu cầu được kiểm hợp lệ nhưng không dùng; `model.trainedOn` ghi rõ điều này.
"""
from dataclasses import dataclass

from . import aqi_data, reading

WINDOW = 1  # giờ, mỗi bên
NAME, VERSION = "hourly-median-since-2025-07", "1.0"


@dataclass
class Baseline:
    by_hour: list[list[float]]  # 24 danh sách đã sắp xếp
    description: str


def _quantile(sorted_vals: list[float], q: float) -> float:
    pos = q * (len(sorted_vals) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (pos - lo)


def _band(by_hour: list[list[float]], hour: int) -> list[float]:
    return sorted(v for h in range(hour - WINDOW, hour + WINDOW + 1) for v in by_hour[h % 24])


def _cv_note(ds: aqi_data.Dataset, idx: list[int]) -> str:
    """Kiểm tra xen kẽ: cứ 4 tuần lấy 1 tuần làm tập thử; so sai số tuyệt đối trung bình của trung vị theo giờ với trung vị chung."""
    train, test = [], []
    for k, i in enumerate(idx):
        (test if (k // (24 * 7)) % 4 == 3 else train).append(i)
    co = ds.cols["co"]
    all_med = _quantile(sorted(co[i] for i in train), 0.5)
    by_hour = [[] for _ in range(24)]
    for i in train:
        by_hour[ds.times[i].hour].append(co[i])
    med = [_quantile(sorted(_band(by_hour, h)), 0.5) for h in range(24)]
    mae_hour = sum(abs(co[i] - med[ds.times[i].hour]) for i in test) / len(test)
    mae_all = sum(abs(co[i] - all_med) for i in test) / len(test)
    return f"sai số tuyệt đối trung bình trên tuần kiểm tra: {mae_hour:.0f} µg/m³ khi dùng giờ, {mae_all:.0f} khi chỉ dùng trung vị chung"


def build(ds: aqi_data.Dataset) -> Baseline:
    idx = [i for i, t in enumerate(ds.times) if t >= aqi_data.BREAK]
    if len(idx) < 24 * 30:
        raise ValueError("chưa đủ dữ liệu giai đoạn hiện tại để dựng heuristic")
    by_hour = [[] for _ in range(24)]
    for i in idx:
        by_hour[ds.times[i].hour].append(ds.cols["co"][i])
    first, last = ds.times[idx[0]], ds.times[idx[-1]]
    desc = (f"Không phải mô hình học máy. Trung vị và khoảng 10%-90% của CO quanh giờ được hỏi (±{WINDOW} giờ), lấy từ {len(idx)} dòng "
            f"của bộ Kaggle từ {first:%d/%m/%Y} đến {last:%d/%m/%Y} (giờ VN, giai đoạn sau điểm gãy 01/07/2025). "
            f"Chỉ giờ trong ngày được dùng. Tháng, thứ, nhiệt độ và gió được kiểm hợp lệ nhưng không dùng: kiểm tra chéo trên chính dữ liệu này "
            f"cho thấy giờ chỉ giảm sai số rất ít ({_cv_note(ds, idx)}) và các biến còn lại không giảm thêm đáng kể. "
            f"Không dự báo được các đợt ô nhiễm kéo dài nhiều ngày, nên khoảng 10%-90% mới là thông tin chính.")
    return Baseline(by_hour, desc)


def predict(model: Baseline, hour: int) -> dict:
    vals = _band(model.by_hour, hour)
    co = round(_quantile(vals, 0.5), 1)
    return {
        "coPredicted": co,
        "unit": "µg/m³",
        "status": reading.status_for(co),
        "interval": {"low": round(_quantile(vals, 0.1), 1), "high": round(_quantile(vals, 0.9), 1)},
        "model": {"name": NAME, "version": VERSION, "kind": "heuristic", "trainedOn": model.description},
    }
