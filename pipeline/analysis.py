"""Phép tính của bước 5 (Spark SQL), tách khỏi việc đọc/ghi HDFS để kiểm được trên bảng nhỏ (backend/tests/test_a3.py).

`compute(spark, df)` nhận DataFrame đã làm sạch (đầu ra của 04_clean: datetime_vn, year, month, hour, after_2025_07_01, co, ...) và trả dict kết quả.
Mọi số do Spark SQL tính; không có số nào được viết sẵn.
"""
CORR_VARIABLES = ["pm2_5", "pm10", "no2", "no", "o3", "so2", "nh3", "wind_speed_kmh", "temperature_C", "humidity_pct", "rain_mm"]
VIEW = "aqi_clean"
# Giai đoạn sau điểm gãy chỉ phủ các tháng lịch này (07/2025 đến 03/2026); so "cùng tháng với cùng tháng" để không lẫn mùa vụ
AFTER_MONTHS = [7, 8, 9, 10, 11, 12, 1, 2, 3]
AROUND_DAYS = 14  # số ngày hai bên điểm gãy để xem bước giảm sắc đến đâu


def _r(v, nd=2):
    return None if v is None else round(float(v), nd)


def _rows(spark, sql):
    return [r.asDict() for r in spark.sql(sql).collect()]


def compute(spark, df) -> dict:
    df.createOrReplaceTempView(VIEW)
    total = spark.sql(f"SELECT COUNT(*) n FROM {VIEW}").first()["n"]

    def hourly(where):
        return [{"hour": r["hour"], "n": r["n"], "mean": _r(r["mean"]), "median": _r(r["median"])}
                for r in _rows(spark, f"SELECT hour, COUNT(*) n, AVG(co) mean, percentile_approx(co, 0.5) median FROM {VIEW} {where} GROUP BY hour ORDER BY hour")]

    monthly = [{"year": r["year"], "month": r["month"], "n": r["n"], "mean": _r(r["mean"]), "median": _r(r["median"]), "max": _r(r["max"])}
               for r in _rows(spark, f"SELECT year, month, COUNT(*) n, AVG(co) mean, percentile_approx(co, 0.5) median, MAX(co) max FROM {VIEW} GROUP BY year, month ORDER BY year, month")]

    periods = {}
    for r in _rows(spark, f"SELECT after_2025_07_01 a, COUNT(*) n, AVG(co) mean, STDDEV(co) std, MIN(co) min, MAX(co) max, percentile_approx(co, array(0.1, 0.5, 0.9)) q "
                          f"FROM {VIEW} GROUP BY after_2025_07_01"):
        periods["after" if r["a"] == 1 else "before"] = {"n": r["n"], "mean": _r(r["mean"]), "std": _r(r["std"]), "min": _r(r["min"]), "max": _r(r["max"]),
                                                          "p10": _r(r["q"][0]), "median": _r(r["q"][1]), "p90": _r(r["q"][2])}
    if "before" in periods and "after" in periods:
        b, a = periods["before"], periods["after"]
        periods["meanChangePct"] = _r((a["mean"] - b["mean"]) / b["mean"] * 100) if b["mean"] else None
        periods["medianChangePct"] = _r((a["median"] - b["median"]) / b["median"] * 100) if b["median"] else None

    corr_sql = ", ".join(f"corr(co, {v}) AS `{v}`" for v in CORR_VARIABLES)

    def corr(where):
        row = spark.sql(f"SELECT {corr_sql} FROM {VIEW} {where}").first().asDict()
        return {v: _r(row[v], 4) for v in CORR_VARIABLES}

    correlation = {"variables": CORR_VARIABLES, "all": corr(""), "before": corr("WHERE after_2025_07_01 = 0"), "after": corr("WHERE after_2025_07_01 = 1")}

    # Mùa vụ: giai đoạn trước gồm cả các mùa CO cao (ví dụ 08 đến 12 hằng năm) còn giai đoạn sau chỉ có 9 tháng, nên chênh lệch thô lẫn cả mùa vụ.
    months_sql = ", ".join(str(m) for m in AFTER_MONTHS)
    like = {}
    for r in _rows(spark, f"SELECT after_2025_07_01 a, COUNT(*) n, AVG(co) mean, percentile_approx(co, 0.5) median FROM {VIEW} WHERE month IN ({months_sql}) GROUP BY after_2025_07_01"):
        like["after" if r["a"] == 1 else "before"] = {"n": r["n"], "mean": _r(r["mean"]), "median": _r(r["median"])}
    like["months"] = AFTER_MONTHS
    if "before" in like and "after" in like and like["before"]["mean"]:
        like["meanChangePct"] = _r((like["after"]["mean"] - like["before"]["mean"]) / like["before"]["mean"] * 100)
    by_month: dict = {}
    for r in _rows(spark, f"SELECT month, after_2025_07_01 a, COUNT(*) n, AVG(co) mean FROM {VIEW} WHERE month IN ({months_sql}) GROUP BY month, after_2025_07_01"):
        by_month.setdefault(r["month"], {})["after" if r["a"] == 1 else "before"] = {"n": r["n"], "mean": _r(r["mean"])}
    seasonal = []
    for m in AFTER_MONTHS:
        e = by_month.get(m, {})
        row = {"month": m, "before": e.get("before"), "after": e.get("after")}
        row["changePct"] = _r((e["after"]["mean"] - e["before"]["mean"]) / e["before"]["mean"] * 100) if e.get("before") and e.get("after") and e["before"]["mean"] else None
        seasonal.append(row)
    around = [{"date": r["d"], "n": r["n"], "mean": _r(r["mean"])} for r in _rows(
        spark, f"SELECT date_format(datetime_vn, 'yyyy-MM-dd') d, COUNT(*) n, AVG(co) mean FROM {VIEW} "
               f"WHERE datetime_vn >= TIMESTAMP'2025-07-01 00:00:00' - INTERVAL {AROUND_DAYS} DAYS AND datetime_vn < TIMESTAMP'2025-07-01 00:00:00' + INTERVAL {AROUND_DAYS} DAYS "
               f"GROUP BY date_format(datetime_vn, 'yyyy-MM-dd') ORDER BY d")]

    return {"rows": total, "seasonalLikeForLike": like, "seasonalByMonth": seasonal, "dailyAroundBreak": around, "breakVn": "2025-07-01 00:00", "hourly": {"all": hourly(""), "before": hourly("WHERE after_2025_07_01 = 0"), "after": hourly("WHERE after_2025_07_01 = 1")},
            "monthly": monthly, "periods": periods, "correlation": correlation}


def strongest(corr: dict, key: str, k: int = 3) -> list[tuple[str, float]]:
    """k biến có |hệ số tương quan với CO| lớn nhất trong nhóm `key` ("all", "before", "after")."""
    items = [(v, r) for v, r in corr[key].items() if r is not None]
    return sorted(items, key=lambda x: -abs(x[1]))[:k]
