"""Xác định ranh giới chuyển tiếp của "điểm gãy" theo DỮ LIỆU (không dùng mốc 01/07/2025), cho bước 6b. Python + numpy thuần, không cần Spark.

Phương pháp (mô tả để người đọc báo cáo kiểm lại được):
  1. Chuỗi: log của CO trung bình từng ngày (giờ +07, chỉ ngày đủ 24 giờ).
  2. Khử mùa vụ: mùa vụ của một ngày trong năm = trung bình log CO các ngày cùng vị trí trong năm (±`SEASON_HALF` ngày, vòng tròn) của các năm THAM CHIẾU
     (mọi ngày trước `reference_end`, mặc định 31/12/2024; giả định điểm gãy xảy ra sau các năm tham chiếu). Phần dư = log CO - mùa vụ.
     Lý do: khớp thẳng trên chuỗi chưa khử mùa chọn nhầm đáy mùa xuân (24/03/2025) vì tháng 3 đến 4 hằng năm vốn thấp.
  3. Bước đơn: trên phần dư từ 6 tháng trước `reference_end` trở đi, hai mức hằng số, chọn điểm cắt có tổng bình phương sai số nhỏ nhất.
  4. Chuyển tiếp: quanh điểm cắt (±w ngày), khớp hàm "mức a, dốc tuyến tính từ t0 đến t1, mức b" bằng bình phương tối thiểu, chọn (t0, t1) tốt nhất.
     [t0, t1] là khoảng chuyển tiếp. Làm với w = 45, 60, 90; w = 60 là mặc định. Các w cho kết quả KHÁC nhau nhiều: ranh giới không duy nhất, và cả ba đều được ghi lại.
Kết quả chỉ mô tả chuỗi CO trong dữ liệu; nó không nói gì về nguyên nhân của điểm gãy.
"""
from datetime import date, timedelta

import numpy as np

SEASON_HALF = 15
WINDOWS = (45, 60, 90)
MAIN_WINDOW = 60
MIN_SEGMENT = 30  # số ngày tối thiểu mỗi bên của bước đơn
EDGE = 5          # số ngày tối thiểu mỗi bên của đoạn dốc trong cửa sổ


def season_profile(days: list, y: np.ndarray, reference_end: date, half: int = SEASON_HALF) -> np.ndarray:
    """s[k] (k = 1..366) = trung bình y của các ngày tham chiếu cách vị trí-trong-năm k không quá `half` ngày (vòng tròn)."""
    doy = np.array([d.timetuple().tm_yday for d in days])
    ref = np.array([d < reference_end for d in days])
    if not ref.any():
        raise ValueError("không có ngày tham chiếu nào trước reference_end")
    s = np.full(367, np.nan)
    for k in range(1, 367):
        dist = np.minimum(np.abs(doy - k), 366 - np.abs(doy - k))
        m = ref & (dist <= half)
        if m.any():
            s[k] = y[m].mean()
    return s


def best_step(r: np.ndarray, min_segment: int = MIN_SEGMENT) -> int:
    """Chỉ số k (đầu đoạn thứ hai) để hai mức hằng số khớp r tốt nhất."""
    n = len(r)
    if n < 2 * min_segment + 1:
        raise ValueError("chuỗi quá ngắn để tìm bước đơn")
    c1, c2 = np.cumsum(r), np.cumsum(r * r)
    best_sse, best_k = np.inf, None
    for k in range(min_segment, n - min_segment):
        a1, a2 = c1[k - 1], c2[k - 1]
        b1, b2 = c1[-1] - a1, c2[-1] - a2
        sse = (a2 - a1 * a1 / k) + (b2 - b1 * b1 / (n - k))
        if sse < best_sse:
            best_sse, best_k = sse, k
    return best_k


def fit_ramp(r: np.ndarray, lo: int, hi: int):
    """Trong r[lo:hi], khớp 'mức a trước t0, dốc tuyến tính t0..t1, mức b sau t1'. Trả (t0, t1, a, b) theo chỉ số của r."""
    idx = np.arange(lo, hi)
    yy = r[lo:hi]
    best = (np.inf, None)
    for t0 in range(lo + EDGE, hi - EDGE):
        for t1 in range(t0 + 1, hi - EDGE):
            u = np.where(idx < t0, 1.0, np.where(idx > t1, 0.0, 1.0 - (idx - t0) / (t1 - t0)))
            X = np.stack([u, 1.0 - u], axis=1)
            coef = np.linalg.lstsq(X, yy, rcond=None)[0]
            sse = float(((X @ coef - yy) ** 2).sum())
            if sse < best[0]:
                best = (sse, (t0, t1, float(coef[0]), float(coef[1])))
    return best[1]


def detect(days: list, daily_mean_co: list, reference_end: date = date(2025, 1, 1), windows=WINDOWS, main_window: int = MAIN_WINDOW) -> dict:
    """days: các ngày (datetime.date, tăng dần, mỗi ngày đủ 24 giờ); daily_mean_co: CO trung bình từng ngày (µg/m³). Trả dict mô tả chuyển tiếp."""
    if len(days) != len(daily_mean_co) or len(days) < 400:
        raise ValueError("cần ít nhất ~400 ngày và hai danh sách cùng độ dài")
    y = np.log(np.asarray(daily_mean_co, dtype=float))
    s = season_profile(days, y, reference_end)
    doy = np.array([d.timetuple().tm_yday for d in days])
    resid = y - s[doy]
    start = next(i for i, d in enumerate(days) if d >= reference_end - timedelta(days=183))
    sel = np.arange(start, len(days))
    sel = sel[~np.isnan(resid[sel])]
    r, ds = resid[sel], [days[i] for i in sel]
    k = best_step(r)
    out = {"method": "log CO trung bình ngày, khử mùa vụ bằng chu kỳ các năm tham chiếu, khớp bước đơn rồi khớp dốc tuyến tính quanh bước (bình phương tối thiểu)",
           "referenceEnd": (reference_end - timedelta(days=1)).isoformat(), "seasonHalfWidthDays": SEASON_HALF,
           "stepDate": ds[k].isoformat(), "stepResidualBefore": round(float(r[:k].mean()), 3), "stepResidualAfter": round(float(r[k:].mean()), 3), "windows": []}
    n = len(r)
    for w in windows:
        lo, hi = max(0, k - w), min(n, k + w)
        t0, t1, a, b = fit_ramp(r, lo, hi)
        out["windows"].append({"halfWidthDays": w, "from": ds[t0].isoformat(), "to": ds[t1].isoformat(), "lengthDays": t1 - t0,
                               "residualBefore": round(a, 3), "residualAfter": round(b, 3)})
    main = next(x for x in out["windows"] if x["halfWidthDays"] == main_window)
    out["main"] = {"halfWidthDays": main_window, "from": main["from"], "to": main["to"]}
    monthly = {}
    for d, v in zip(days, resid):
        if not np.isnan(v) and d >= reference_end:
            monthly.setdefault(f"{d.year}-{d.month:02d}", []).append(float(v))
    out["monthlyResidual"] = [{"month": m, "meanLogResidual": round(float(np.mean(v)), 3), "days": len(v)} for m, v in sorted(monthly.items())]
    return out
