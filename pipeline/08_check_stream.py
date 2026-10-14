"""Bước 8 (Kafka ingest và dự đoán trên luồng sống): KIỂM những gì job Spark streaming đã ghi vào /live/co_stream, rồi ghi kết quả vào step_summary.json (bước kafka-ingest).

  spark-submit --driver-memory 1g pipeline/08_check_stream.py

Chỉ ĐỌC /live/co_stream và /co/output; không ghi gì ngoài step_summary.json. Không có dòng nào đã được dự đoán thì bước `failed` (không bịa trạng thái đạt).
Báo, theo từng nguồn (provider):
  - số dòng có dự đoán (co_predicted không null) và số dòng không có (thiếu đầu vào cho mô hình);
  - RMSE/MAE của dự đoán so với co_actual, và RMSE của mốc "CO giờ trước" trên CHÍNH các dòng đó (so sánh công bằng);
  - phát lại: bao nhiêu dòng nằm NGOÀI/TRONG mẫu huấn luyện (theo originalDatetime so với đầu tập kiểm tra), và dự đoán trên luồng có trùng dự đoán offline đã lưu
    (/co/output/model_test_predictions) hay không, cho các dòng thuộc tập kiểm tra.
Số mẫu trên luồng nhỏ (nhất là dữ liệu thật: tối đa một dòng mỗi giờ) nên RMSE ở đây chỉ là bằng chứng chuỗi chạy đúng, không thay cho chỉ số trên tập kiểm tra ở model_metrics.json.
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as c  # noqa: E402

LIVE = f"{c.HDFS_URI}/live/co_stream"


def main() -> int:
    from pyspark.sql import functions as F

    with c.step("kafka-ingest") as r:
        spark = c.spark_session("HCMC_CO_08_check_stream")
        metrics = json.loads(c.hdfs_read_text(f"{c.OUTPUT_DIR}/model_metrics.json"))
        test_from = metrics["testSet"]["from"][:19].replace("T", " ")
        best = next(m for m in metrics["models"] if m["isBest"])
        df = (spark.read.option("mergeSchema", "true").parquet(LIVE).dropDuplicates(["partition", "offset"]))
        if "co_predicted" not in df.columns:
            raise RuntimeError("/live/co_stream chưa có cột co_predicted: job Spark chưa chạy bản có mô hình (Bước A5)")
        total = df.count()
        ok_all = df.where(F.col("co_predicted").isNotNull() & F.col("co_actual").isNotNull())
        # phát lại chạy theo vòng lặp nên cùng một giờ gốc có thể xuất hiện nhiều lần: giữ một dòng cho mỗi originalDatetime để không đếm trùng
        ok = (ok_all.where(F.col("provider") != "replay-dataset")
              .unionByName(ok_all.where(F.col("provider") == "replay-dataset").dropDuplicates(["originalDatetime"]))).cache()
        if ok.count() == 0:
            raise RuntimeError("không có dòng nào đã được dự đoán trong /live/co_stream")
        per = {}
        for row in ok.groupBy("provider").agg(
                F.count("*").alias("n"), F.sqrt(F.avg(F.pow(F.col("co_predicted") - F.col("co_actual"), 2))).alias("rmse"), F.avg(F.abs(F.col("co_predicted") - F.col("co_actual"))).alias("mae"),
                F.sqrt(F.avg(F.pow(F.col("model_inputs.coLag1h") - F.col("co_actual"), 2))).alias("rmse_prev"), F.min("model_version").alias("v1"), F.max("model_version").alias("v2")).collect():
            per[row["provider"]] = row.asDict()
        no_pred = {x["provider"]: x["n"] for x in df.where(F.col("co_predicted").isNull()).groupBy("provider").agg(F.count("*").alias("n")).collect()}
        counts = {x["provider"]: x["n"] for x in df.groupBy("provider").agg(F.count("*").alias("n")).collect()}

        rep = ok.where(F.col("provider") == "replay-dataset")  # đã loại trùng theo originalDatetime
        in_test = rep.where(F.col("originalDatetime") >= F.lit(test_from)).cache()
        n_rep, n_test = rep.count(), in_test.count()
        match = None
        if n_test:
            off = spark.read.parquet(f"{c.HDFS_URI}{c.OUTPUT_DIR}/model_test_predictions").select(F.col("datetime_vn").alias("originalDatetime"), F.col("predicted").alias("offline"))
            j = in_test.dropDuplicates(["originalDatetime"]).join(off, "originalDatetime")
            agg = j.agg(F.count("*").alias("n"), F.max(F.abs(F.col("co_predicted") - F.col("offline"))).alias("maxdiff")).first()
            match = (agg["n"], float(agg["maxdiff"]) if agg["maxdiff"] is not None else None)

        fmt = lambda d: f"n={d['n']}, RMSE {d['rmse']:.1f}, MAE {d['mae']:.1f}, RMSE mốc CO giờ trước {d['rmse_prev']:.1f} (µg/m³)"  # noqa: E731
        items = [c.item("Dòng trong /live/co_stream (đã loại trùng)", total, "dòng"), c.item("Dòng đã có dự đoán (phát lại đã loại trùng theo originalDatetime)", ok.count(), "dòng"),
                 c.item("Dòng chưa có dự đoán (thiếu đầu vào hoặc ghi trước A5)", sum(no_pred.values()), "dòng"),
                 c.item("Phiên bản mô hình trên luồng", ", ".join(sorted({v for d in per.values() for v in (d["v1"], d["v2"])})), None),
                 c.item("Phiên bản mô hình đã lưu", best["version"], None)]
        for prov, d in sorted(per.items()):
            items.append(c.item(f"Sai số dự đoán trên luồng: {prov}", fmt(d), None))
        items.append(c.item("Phát lại: dòng ngoài mẫu huấn luyện / tổng dòng có dự đoán", f"{n_test} / {n_rep}", None))
        if match is not None:
            items.append(c.item("Phát lại: dòng khớp dự đoán offline / chênh lệch lớn nhất", f"{match[0]} / {match[1]}", "µg/m³"))
        r.summary = items
        r.detail = ("Đã đọc lại /live/co_stream: " + "; ".join(f"{p}: {n} dòng" for p, n in sorted(counts.items())) + ". "
                    + " ".join(f"{prov}: {fmt(d)}." for prov, d in sorted(per.items())) + " "
                    + (f"Luồng phát lại: {n_test}/{n_rep} dòng có dự đoán nằm trong tập kiểm tra của mô hình (ngoài mẫu huấn luyện); dự đoán trên luồng lệch dự đoán offline tối đa {match[1]} µg/m³ ở {match[0]} dòng đối chiếu được. "
                       if match else "")
                    + "Luồng phát lại là MÔ PHỎNG từ bộ lịch sử, dữ liệu thật chỉ có tối đa một dòng mỗi giờ nên số mẫu nhỏ: đây là bằng chứng chuỗi chạy đúng chứ không thay chỉ số của model_metrics.json. "
                      "Dữ liệu thật dùng nhiệt độ và gió của quan sát hiện tại thay cho giờ trước (API lịch sử không có thời tiết).")
        spark.stop()
    print("[08_check_stream] xong:", f"tổng {total} dòng; có dự đoán: " + ", ".join(f"{p}={d['n']} (RMSE {d['rmse']:.1f} vs mốc {d['rmse_prev']:.1f})" for p, d in sorted(per.items())),
          f"| khớp offline: {match}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
