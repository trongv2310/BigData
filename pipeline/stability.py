"""Độ ổn định của phân cụm ngày theo seed (bước 6a bổ sung). Phần Spark nằm ở fit(); phần còn lại là Python thuần để kiểm được (backend/tests/test_a3b.py)."""
from collections import Counter

import day_clusters as dc

STABILITY_SEEDS = [dc.SEED, 1, 7, 13, 21, 99]  # seed gốc của 06a + 5 seed thêm
STABILITY_KS = (2, 3)


def assemble(days):
    from pyspark.ml.feature import VectorAssembler
    return VectorAssembler(inputCols=dc.SHAPE_COLS, outputCol="features").transform(days).cache()


def fit(data, k, seed):
    """K-Means cho một (k, seed): trả (silhouette, mô hình, DataFrame có cột cluster)."""
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.evaluation import ClusteringEvaluator

    model = KMeans(k=k, seed=seed, maxIter=dc.MAX_ITER, featuresCol="features", predictionCol="cluster").fit(data)
    out = model.transform(data)
    sil = float(ClusteringEvaluator(featuresCol="features", predictionCol="cluster", metricName="silhouette", distanceMeasure="squaredEuclidean").evaluate(out))
    return sil, model, out


def adjusted_rand_index(a: list, b: list) -> float:
    """Chỉ số Rand điều chỉnh giữa hai cách gán nhãn của cùng các điểm (không phụ thuộc việc đánh số cụm): 1 = giống hệt, quanh 0 = như ngẫu nhiên."""
    if len(a) != len(b) or not a:
        raise ValueError("hai cách gán nhãn phải cùng độ dài và không rỗng")
    comb2 = lambda x: x * (x - 1) // 2  # noqa: E731
    sum_ij = sum(comb2(v) for v in Counter(zip(a, b)).values())
    sum_a, sum_b = sum(comb2(v) for v in Counter(a).values()), sum(comb2(v) for v in Counter(b).values())
    total = comb2(len(a))
    expected = sum_a * sum_b / total if total else 0.0
    max_index = (sum_a + sum_b) / 2
    return 1.0 if max_index == expected else (sum_ij - expected) / (max_index - expected)


def best_k(table: list[dict]) -> int:
    """Đúng tiêu chí của 06a: silhouette lớn nhất, hòa thì k nhỏ hơn."""
    return max(table, key=lambda t: (t["silhouette"], -t["k"]))["k"]


def summarize(per_seed: dict, base_seed: int = dc.SEED) -> dict:
    """per_seed: {seed: {k: {"silhouette", "sizes", "labels"}}} -> thống kê ổn định cho từng k (silhouette, ARI so với seed gốc và giữa các cặp, kích thước cụm)."""
    out = {}
    seeds = list(per_seed)
    for k in sorted({k for v in per_seed.values() for k in v}):
        sils = [per_seed[s][k]["silhouette"] for s in seeds]
        vs_base = {str(s): round(adjusted_rand_index(per_seed[base_seed][k]["labels"], per_seed[s][k]["labels"]), 4) for s in seeds if s != base_seed}
        pairs = [adjusted_rand_index(per_seed[a][k]["labels"], per_seed[b][k]["labels"]) for i, a in enumerate(seeds) for b in seeds[i + 1:]]
        out[str(k)] = {"silhouetteBySeed": {str(s): per_seed[s][k]["silhouette"] for s in seeds},
                       "silhouetteMin": round(min(sils), 4), "silhouetteMean": round(sum(sils) / len(sils), 4), "silhouetteMax": round(max(sils), 4),
                       "ariVsBaseSeed": vs_base, "ariPairwiseMin": round(min(pairs), 4), "ariPairwiseMean": round(sum(pairs) / len(pairs), 4),
                       "sizesBySeed": {str(s): per_seed[s][k]["sizes"] for s in seeds}}
    return out
