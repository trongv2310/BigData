"""Bước 5 (Spark SQL): phân tích CO trên /co/processed/aqi_clean, ghi /co/output/sql_analysis.json.

  spark-submit --driver-memory 1g pipeline/05_sql_analysis.py [--overwrite]

Gồm: CO theo giờ trong ngày (giờ +07; chung và tách trước/sau 01/07/2025), CO theo tháng, so sánh hai giai đoạn trước/sau điểm gãy 01/07/2025 (+07),
và hệ số tương quan Pearson của CO với PM2.5, PM10, NO2, NO, O3, SO2, NH3, gió, nhiệt độ, độ ẩm, mưa (chung và trong từng giai đoạn).
Lưu ý khi đọc: điểm gãy làm CO đổi mức giữa hai giai đoạn, nên tương quan "chung" trộn hai mức và có thể khác hẳn tương quan trong từng giai đoạn; đừng trích dẫn số chung mà không nói điều đó.
Phép tính nằm ở analysis.py (có test). Kết quả đã có thì dừng, chỉ ghi đè khi có --overwrite.
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as c  # noqa: E402
import analysis  # noqa: E402

OUT = f"{c.OUTPUT_DIR}/sql_analysis.json"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--overwrite", action="store_true", help="ghi đè /co/output/sql_analysis.json nếu đã có")
    a = ap.parse_args()
    if c.hdfs_exists(OUT) and not a.overwrite:
        print(f"[05_sql_analysis] {OUT} đã có; chạy lại với --overwrite nếu chắc chắn (hỏi người dùng trước).", file=sys.stderr)
        return 2

    with c.step("spark-sql") as r:
        spark = c.spark_session("HCMC_CO_05_sql_analysis")
        res = analysis.compute(spark, spark.read.parquet(f"{c.HDFS_URI}{c.CLEAN_DIR}"))
        res = {"generatedAt": c.now_iso(), "source": c.CLEAN_DIR, **res}
        c.hdfs_write_text(OUT, json.dumps(res, ensure_ascii=False, indent=2) + "\n")

        p, corr = res["periods"], res["correlation"]
        b, af = p["before"], p["after"]
        peak = lambda tbl: max(tbl, key=lambda x: x["mean"])["hour"]  # noqa: E731
        s_all, s_b, s_a = (analysis.strongest(corr, k) for k in ("all", "before", "after"))
        fmt = lambda xs: ", ".join(f"{v} {x:+.2f}" for v, x in xs)  # noqa: E731
        r.summary = [c.item("Số dòng phân tích", res["rows"], "dòng"),
                     c.item("CO trung bình trước 01/07/2025", b["mean"], "µg/m³"), c.item("CO trung bình từ 01/07/2025", af["mean"], "µg/m³"),
                     c.item("Thay đổi CO trung bình (thô)", p["meanChangePct"], "%"),
                     c.item("Thay đổi cùng tháng lịch (07 đến 03)", res["seasonalLikeForLike"].get("meanChangePct"), "%"), c.item("CO trung vị trước / từ 01/07/2025", f"{b['median']} / {af['median']}", "µg/m³"),
                     c.item("Giờ CO trung bình cao nhất (trước / sau)", f"{peak(res['hourly']['before'])}h / {peak(res['hourly']['after'])}h", None),
                     c.item("Số tháng có dữ liệu", len(res["monthly"]), "tháng")]
        r.detail = (f"Spark SQL trên {c.CLEAN_DIR}; kết quả đầy đủ ở {OUT}. Tương quan Pearson của CO mạnh nhất: chung [{fmt(s_all)}]; trước điểm gãy [{fmt(s_b)}]; từ điểm gãy [{fmt(s_a)}]. "
                    f"Giai đoạn sau chỉ phủ tháng lịch 07 đến 03, nên đã so thêm cùng tháng lịch: CO trung bình đổi {res['seasonalLikeForLike'].get('meanChangePct')}% "
                    f"(trước {res['seasonalLikeForLike']['before']['mean']}, sau {res['seasonalLikeForLike']['after']['mean']} µg/m³) so với {p['meanChangePct']}% khi so thô; chi tiết theo tháng ở seasonalByMonth. "
                    "Diễn biến theo từng ngày quanh 01/07/2025 ở dailyAroundBreak: đọc trước khi coi 01/07 là một bước nhảy tức thời. "
                    "Số 'chung' trộn hai mức CO của hai giai đoạn nên không thay được số trong từng giai đoạn. Tương quan không phải quan hệ nhân quả. "
                    "Tháng đầu (04/2022) và tháng cuối (04/2026) chỉ có một phần dữ liệu (xem cột n).")
        spark.stop()
    print(f"[05_sql_analysis] xong: CO TB trước {b['mean']} -> sau {af['mean']} µg/m³ ({p['meanChangePct']}%); tương quan trong từng giai đoạn: trước [{fmt(s_b)}], sau [{fmt(s_a)}]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
