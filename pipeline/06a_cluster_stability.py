"""Bước 6a (bổ sung): độ ổn định của phân cụm ngày theo seed, và kết quả k=3 để so với k=2.

  spark-submit --driver-memory 1g pipeline/06a_cluster_stability.py [--overwrite]

Chạy K-Means cho k = 2..8 với 6 seed (42 là seed của 06a, cộng 5 seed thêm: 1, 7, 13, 21, 99). Báo:
  - k nào được chọn (silhouette lớn nhất) ở MỖI seed;
  - với k = 2 và k = 3: silhouette theo seed, chỉ số Rand điều chỉnh (ARI) giữa các lần gán nhãn (1 = giống hệt), kích thước cụm theo seed;
  - chi tiết k=3 (seed 42): kích thước, mức CO, tỷ lệ sau điểm gãy, giờ cao/thấp nhất của tâm cụm. Nhãn k=3 ghi Parquet /co/output/day_clusters_k3.
Không ghi đè đầu ra của 06a (day_clusters, day_clusters.json): đầu ra riêng là day_clusters_stability.json và day_clusters_k3. Đã có thì dừng, chỉ ghi đè khi có --overwrite.
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as c  # noqa: E402
import day_clusters as dc  # noqa: E402
import stability as st  # noqa: E402

JSON_OUT = f"{c.OUTPUT_DIR}/day_clusters_stability.json"
K3_PARQUET = f"{c.OUTPUT_DIR}/day_clusters_k3"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--overwrite", action="store_true")
    a = ap.parse_args()
    if (c.hdfs_exists(JSON_OUT) or c.hdfs_exists(K3_PARQUET)) and not a.overwrite:
        print(f"[06a_cluster_stability] {JSON_OUT} hoặc {K3_PARQUET} đã có; chạy lại với --overwrite nếu chắc chắn (hỏi người dùng trước).", file=sys.stderr)
        return 2

    with c.step("cluster-and-model", final_state="partial", merge=True) as r:
        spark = c.spark_session("HCMC_CO_06a_cluster_stability")
        days, _excluded, _total = dc.day_table(spark, spark.read.parquet(f"{c.HDFS_URI}{c.CLEAN_DIR}"))
        data = st.assemble(days)
        n_days = data.count()
        dates = [row["date"] for row in data.select("date").orderBy("date").collect()]

        per_seed, chosen, tables, k3_model, k3_out = {}, {}, {}, None, None
        for seed in st.STABILITY_SEEDS:
            per_seed[seed], tables[seed] = {}, []
            for k in range(dc.K_MIN, dc.K_MAX + 1):
                sil, model, out = st.fit(data, k, seed)
                tables[seed].append({"k": k, "silhouette": round(sil, 4)})
                if k in st.STABILITY_KS:
                    rows = {row["date"]: row["cluster"] for row in out.select("date", "cluster").collect()}
                    counts = {}
                    for v in rows.values():
                        counts[v] = counts.get(v, 0) + 1
                    per_seed[seed][k] = {"silhouette": round(sil, 4), "sizes": sorted(counts.values(), reverse=True), "labels": [rows[d] for d in dates]}
                    if k == 3 and seed == dc.SEED:
                        k3_model, k3_out = model, out
            chosen[seed] = st.best_k(tables[seed])

        stability = st.summarize(per_seed)
        k3_stats = dc.cluster_stats(k3_out, k3_model.clusterCenters(), 3)
        (k3_out.select("date", "cluster", "mean_co", "after_2025_07_01", "dayofweek").coalesce(1)
         .write.mode("overwrite" if a.overwrite else "errorifexists").parquet(f"{c.HDFS_URI}{K3_PARQUET}"))
        doc = {"generatedAt": c.now_iso(), "source": c.CLEAN_DIR, "seeds": st.STABILITY_SEEDS, "baseSeed": dc.SEED, "daysUsed": n_days,
               "chosenKBySeed": {str(s): k for s, k in chosen.items()}, "silhouetteTableBySeed": {str(s): t for s, t in tables.items()},
               "stability": stability, "k3": {"silhouette": per_seed[dc.SEED][3]["silhouette"], "clusters": k3_stats},
               "notes": ["ARI (chỉ số Rand điều chỉnh) so hai cách gán nhãn bất kể cách đánh số cụm: 1 = giống hệt, gần 0 = như ngẫu nhiên.",
                         "k=3 chỉ ghi nhãn cho seed 42 (day_clusters_k3); các seed khác chỉ dùng để đo độ ổn định."]}
        c.hdfs_write_text(JSON_OUT, json.dumps(doc, ensure_ascii=False, indent=2) + "\n")

        s2, s3 = stability["2"], stability["3"]
        all_k2 = all(k == 2 for k in chosen.values())
        r.summary = [c.item("Số seed đã thử", len(st.STABILITY_SEEDS), "seed"), c.item("k được chọn theo seed", ", ".join(f"{s}: k={k}" for s, k in chosen.items()), None),
                     c.item("Silhouette k=2 (nhỏ nhất / lớn nhất)", f"{s2['silhouetteMin']} / {s2['silhouetteMax']}", None),
                     c.item("Silhouette k=3 (nhỏ nhất / lớn nhất)", f"{s3['silhouetteMin']} / {s3['silhouetteMax']}", None),
                     c.item("ARI k=2 giữa các seed (nhỏ nhất)", s2["ariPairwiseMin"], None), c.item("ARI k=3 giữa các seed (nhỏ nhất)", s3["ariPairwiseMin"], None)]
        r.detail = (f"Phần 6a (phân cụm ngày) và độ ổn định theo {len(st.STABILITY_SEEDS)} seed đã xong; phần 6b (mô hình dự đoán CO) CHƯA chạy nên bước này chưa xong. "
                    f"Tiêu chí silhouette lớn nhất chọn {'k=2 ở mọi seed' if all_k2 else 'k khác nhau giữa các seed (xem chosenKBySeed)'}. "
                    f"Chi tiết ở {JSON_OUT} và {K3_PARQUET} (nhãn k=3).")
        spark.stop()
    print(f"[06a_cluster_stability] xong: {n_days} ngày; k chọn theo seed: {chosen}")
    for k in ("2", "3"):
        s = stability[k]
        print(f"    k={k}: silhouette {s['silhouetteMin']}..{s['silhouetteMax']} (TB {s['silhouetteMean']}); ARI giữa các cặp seed: nhỏ nhất {s['ariPairwiseMin']}, TB {s['ariPairwiseMean']}; "
              f"ARI so seed 42: {s['ariVsBaseSeed']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
