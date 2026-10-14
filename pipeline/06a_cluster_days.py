"""Bước 6a: phân cụm ngày theo hình dạng 24 giờ của CO (K-Means, Spark MLlib).

  spark-submit --driver-memory 1g pipeline/06a_cluster_days.py [--overwrite] [--k-min 2] [--k-max 8]

Đầu vào /co/processed/aqi_clean. Mỗi ngày (giờ +07, đủ 24 giờ) là một điểm 24 chiều = CO từng giờ chia cho CO trung bình của ngày đó (hình dạng, không phải mức).
k chọn bằng silhouette lớn nhất trong khoảng k-min..k-max (hòa thì k nhỏ); mọi k đều được ghi lại. Chi tiết và lý do ở day_clusters.py.
Ra: /co/output/day_clusters (Parquet: date, cluster, mean_co, after_2025_07_01, dayofweek, shape[24], raw[24]) và /co/output/day_clusters.json (bảng chọn k, tâm cụm, thống kê cụm).
Bước `cluster-and-model` chỉ xong khi cả 6b (mô hình dự đoán) xong, nên script này ghi trạng thái "partial" kèm chú thích phần nào đã xong. Đã có đầu ra thì dừng, chỉ ghi đè khi có --overwrite.
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as c  # noqa: E402
import day_clusters as dc  # noqa: E402

PARQUET = f"{c.OUTPUT_DIR}/day_clusters"
JSON_OUT = f"{c.OUTPUT_DIR}/day_clusters.json"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--k-min", type=int, default=dc.K_MIN)
    ap.add_argument("--k-max", type=int, default=dc.K_MAX)
    a = ap.parse_args()
    if (c.hdfs_exists(PARQUET) or c.hdfs_exists(JSON_OUT)) and not a.overwrite:
        print(f"[06a_cluster_days] {PARQUET} hoặc {JSON_OUT} đã có; chạy lại với --overwrite nếu chắc chắn (hỏi người dùng trước).", file=sys.stderr)
        return 2

    from pyspark.sql import functions as F

    with c.step("cluster-and-model", final_state="partial", merge=True) as r:
        spark = c.spark_session("HCMC_CO_06a_cluster_days")
        df = spark.read.parquet(f"{c.HDFS_URI}{c.CLEAN_DIR}")
        days, excluded, total_days = dc.day_table(spark, df)
        used = days.count()
        rng = days.agg(F.min("date").alias("a"), F.max("date").alias("b")).first()
        table, k, model, assigned = dc.select_k(spark, days, a.k_min, a.k_max)
        sil = next(t["silhouette"] for t in table if t["k"] == k)
        stats = dc.cluster_stats(assigned, model.clusterCenters(), k)
        label = dc.structure_label(sil)

        out = assigned.select("date", "cluster", F.round("mean_co", 2).alias("mean_co"), "after_2025_07_01", "dayofweek",
                              F.array(*dc.SHAPE_COLS).alias("shape"), F.array(*dc.RAW_COLS).alias("raw"))
        out.coalesce(1).write.mode("overwrite" if a.overwrite else "errorifexists").parquet(f"{c.HDFS_URI}{PARQUET}")
        back = spark.read.parquet(f"{c.HDFS_URI}{PARQUET}")
        if back.count() != used or back.select("cluster").distinct().count() != k:
            raise RuntimeError("Parquet ghi ra không khớp số ngày hoặc số cụm")
        doc = {"generatedAt": c.now_iso(), "source": c.CLEAN_DIR, "mode": "shape (CO từng giờ chia cho CO trung bình của ngày)",
               "criterion": f"silhouette lớn nhất (squaredEuclidean) trong k={a.k_min}..{a.k_max}; hòa thì k nhỏ hơn", "seed": dc.SEED, "maxIter": dc.MAX_ITER,
               "daysUsed": used, "daysExcludedIncomplete": excluded, "daysTotal": total_days, "firstDay": rng["a"], "lastDay": rng["b"],
               "k": k, "silhouette": sil, "structure": label, "selection": table, "clusters": stats,
               "notes": ["Hình dạng đã loại mức CO, nên cụm không chỉ là 'giai đoạn trước/sau 01/07/2025'; afterBreakShare cho biết cụm nghiêng về giai đoạn nào.",
                         "Giờ tính theo +07; ngày đầu (01/04/2022) và ngày cuối (01/04/2026) thiếu giờ nên bị loại.",
                         f"Silhouette {sil} nghĩa là cấu trúc cụm {label} (quy ước Kaufman và Rousseeuw 1990)."]}
        c.hdfs_write_text(JSON_OUT, json.dumps(doc, ensure_ascii=False, indent=2) + "\n")

        sizes = ", ".join(f"cụm {s['cluster']}: {s['days']} ngày" for s in stats)
        shape_txt = "; ".join(f"cụm {s['cluster']} cao nhất {s['peakHour']}h, thấp nhất {s['troughHour']}h, {int(s['afterBreakShare'] * 100)}% sau điểm gãy, CO ngày TB {s['meanDailyCo']}" for s in stats)
        r.summary = [c.item("Số ngày phân cụm", used, "ngày"), c.item("Ngày bị loại (thiếu giờ)", excluded, "ngày"), c.item("Số cụm chọn (k)", k, "cụm"),
                     c.item("Silhouette của k đã chọn", sil, None), c.item("Cấu trúc cụm theo silhouette", label, None),
                     c.item("Silhouette theo k", ", ".join(f"k={t['k']}: {t['silhouette']}" for t in table), None), c.item("Kích thước các cụm", sizes, None)]
        r.detail = (f"Mới xong phần 6a (phân cụm ngày); phần 6b (mô hình dự đoán CO) CHƯA chạy nên bước này chưa xong. K-Means MLlib trên hình dạng 24 giờ (CO từng giờ / CO trung bình ngày), "
                    f"k chọn bằng silhouette lớn nhất trong {a.k_min}..{a.k_max}. {shape_txt}. Kết quả: {PARQUET} (nhãn từng ngày) và {JSON_OUT} (bảng chọn k, tâm cụm).")
        spark.stop()
    print(f"[06a_cluster_days] xong: {used} ngày (loại {excluded}), k={k}, silhouette={sil} ({label}); " + sizes)
    for t in table:
        print(f"    k={t['k']} silhouette={t['silhouette']} wssse={t['wssse']} sizes={t['sizes']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
