"""Bước 6b (mô hình dự đoán CO) và bước 7 (lưu kết quả).

  spark-submit --driver-memory 2g pipeline/06b_train.py [--overwrite]

Đầu vào /co/processed/aqi_clean. Làm theo quyết định D6 của người dùng (docs/TASKS.md):
  A (chính)  huấn luyện và kiểm tra CHỈ trong giai đoạn sau chuyển tiếp, chia 80/20 theo thời gian;
  B          chia 80/20 theo thời gian trên toàn bộ dữ liệu (yêu cầu gốc), để so sánh;
  C          huấn luyện trên toàn bộ trước tập kiểm tra của A, có biến đánh dấu chế độ `regime`, kiểm tra đúng tập kiểm tra của A.
Ranh giới chuyển tiếp xác định theo DỮ LIỆU (changepoint.py), các ngày chuyển tiếp và 24 giờ sau đó bị loại khỏi tập huấn luyện (A và C).
Ba mô hình (hồi quy tuyến tính, Random Forest, GBT) và hai mốc (CO giờ trước; trung bình theo giờ trong ngày) trên cùng tập kiểm tra của từng phương án.
Ra: /co/output/model_metrics.json, /co/output/model_best (Spark PipelineModel của mô hình tốt nhất ở phương án A), /co/output/model_test_predictions (Parquet),
/co/output/model_inputs_reference.json (trung vị đặc trưng huấn luyện, để backend điền đầu vào thiếu).
Nguyên nhân điểm gãy dữ liệu CHƯA xác định; script này không kết luận CO ngoài đời giảm thật. Đã có đầu ra thì dừng, chỉ ghi đè khi có --overwrite.
"""
import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import changepoint as cp  # noqa: E402
import common as c  # noqa: E402
import day_clusters as dc  # noqa: E402
import modeling as m  # noqa: E402

METRICS_JSON = f"{c.OUTPUT_DIR}/model_metrics.json"
MODEL_DIR = f"{c.OUTPUT_DIR}/model_best"
PRED_DIR = f"{c.OUTPUT_DIR}/model_test_predictions"
REF_JSON = f"{c.OUTPUT_DIR}/model_inputs_reference.json"  # trung vị đặc trưng của tập huấn luyện: backend dùng điền đầu vào form bỏ trống (và báo ở filledInputs)
TARGET = ("CO (µg/m³) của giờ t, dự đoán từ thông tin đến giờ t-1: nhiệt độ, gió và nồng độ các chất khác của giờ t-1; CO của các giờ t-1, t-2, t-3 và t-24; "
          "giờ và tháng của t (giờ +07). Khi dùng cho form /api/predict, temperatureC và windSpeedKmh là của giờ ngay trước đó.")
CAUSE = ("CHƯA XÁC ĐỊNH. Điểm gãy có trong cả bộ Kaggle lẫn bản thô OpenWeather tự cào (cùng nhà cung cấp), với hình dạng gần như y hệt, nên không phải do khâu xử lý của Kaggle. "
         "Dữ liệu này không phân biệt được thay đổi ở nguồn (cách tính, hiệu chuẩn, đầu vào mô hình khí quyển) với thay đổi thật ngoài đời. Báo cáo KHÔNG kết luận CO ngoài đời giảm thật.")


def iso(ts: str) -> str:
    """'2025-07-12 00:00:00' (giờ +07) -> '2025-07-12T00:00:00+07:00'."""
    return ts.replace(" ", "T")[:19] + "+07:00"


def span(df) -> dict:
    from pyspark.sql import functions as F
    r = df.agg(F.date_format(F.min("datetime_vn"), "yyyy-MM-dd HH:mm:ss").alias("a"), F.date_format(F.max("datetime_vn"), "yyyy-MM-dd HH:mm:ss").alias("b"), F.count("*").alias("n")).first()
    return {"from": iso(r["a"]), "to": iso(r["b"]), "rows": r["n"]}


def conclusion_text(res: dict, test_rows: int) -> str:
    models, base, best = res["models"], res["baselines"], res["best"]
    prev, hm, b = base["previous-hour"]["rmse"], base["hourly-mean"]["rmse"], models[best]["test"]["rmse"]
    wins = [m.NAMES[k] for k in m.KINDS if models[k]["test"]["rmse"] < prev]
    parts = [f"Mô hình chọn theo đoạn kiểm định: {m.NAMES[best]}, RMSE {b:.1f} µg/m³ trên {test_rows} giờ kiểm tra.",
             f"So với mốc CO giờ trước (RMSE {prev:.1f}): {'THẮNG' if b < prev else 'KHÔNG thắng'} ({(b - prev) / prev * 100:+.1f}%).",
             f"So với mốc trung bình theo giờ trong ngày (RMSE {hm:.1f}): {'thắng' if b < hm else 'không thắng'} ({(b - hm) / hm * 100:+.1f}%).",
             "Mô hình có RMSE thấp hơn mốc CO giờ trước: " + (", ".join(wins) if wins else "không có mô hình nào") + "."]
    if not wins:
        parts.append("Với dữ liệu này, chỉ lặp lại CO của giờ trước đã tốt hơn mọi mô hình; báo đúng như vậy.")
    return " ".join(parts)


def models_json(res: dict, version_for) -> list:
    out = []
    prev, hm = res["baselines"]["previous-hour"]["rmse"], res["baselines"]["hourly-mean"]["rmse"]
    for k in m.KINDS:
        t = res["models"][k]["test"]
        out.append({"name": m.NAMES[k], "kind": k, "version": version_for(k), "isBest": k == res["best"], "test": m.round_metrics(t),
                    "beatsPreviousHour": t["rmse"] < prev, "beatsHourlyMean": t["rmse"] < hm,
                    "params": m.MODEL_PARAMS[k] + f"; RMSE kiểm định {res['models'][k]['val']:.1f}"})
    return out


def baselines_json(res: dict, hm_description: str) -> list:
    b = res["baselines"]
    return [{"id": "previous-hour", "name": "CO giờ trước", "description": "Dự đoán CO giờ t bằng đúng CO giờ t-1.", "test": m.round_metrics(b["previous-hour"])},
            {"id": "hourly-mean", "name": "Trung bình theo giờ trong ngày", "description": hm_description, "test": m.round_metrics(b["hourly-mean"])}]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--overwrite", action="store_true")
    a = ap.parse_args()
    if any(c.hdfs_exists(p) for p in (METRICS_JSON, MODEL_DIR, PRED_DIR, REF_JSON)) and not a.overwrite:
        print(f"[06b_train] {METRICS_JSON}, {MODEL_DIR}, {PRED_DIR} hoặc {REF_JSON} đã có; chạy lại với --overwrite nếu chắc chắn (hỏi người dùng trước).", file=sys.stderr)
        return 2

    from pyspark.ml import PipelineModel
    from pyspark.sql import functions as F

    today = datetime.now(c.VN).strftime("%Y%m%d")
    version_for = lambda kind: f"{kind}-{today}"  # noqa: E731

    with c.step("cluster-and-model", final_state="done", merge=True) as r6:
        spark = c.spark_session("HCMC_CO_06b_train")
        df = spark.read.parquet(f"{c.HDFS_URI}{c.CLEAN_DIR}")
        gaps = m.check_hourly_continuity(df)
        if gaps:
            raise RuntimeError(f"{gaps} khoảng cách giữa hai dòng liên tiếp khác 1 giờ; lag theo dòng sẽ sai")

        # 1. ranh giới chuyển tiếp theo dữ liệu
        days_df, _, _ = dc.day_table(spark, df)
        rows = days_df.select("date", "mean_co").orderBy("date").collect()
        days = [date.fromisoformat(x["date"]) for x in rows]
        trans = cp.detect(days, [float(x["mean_co"]) for x in rows])
        t_from, t_to = date.fromisoformat(trans["main"]["from"]), date.fromisoformat(trans["main"]["to"])
        post_start = datetime.combine(t_to + timedelta(days=1), datetime.min.time())
        post_start_s = post_start.strftime("%Y-%m-%d %H:%M:%S")
        lag_clear_s = (post_start + timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")  # từ đây mọi độ trễ 24 giờ nằm trong giai đoạn sau
        trans_from_s = datetime.combine(t_from, datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S")

        feats = m.build_features(df).cache()
        ts = lambda s: F.lit(s).cast("timestamp")  # noqa: E731

        # 2. ba phương án chia dữ liệu
        a_rows = feats.where(F.col("datetime_vn") >= ts(lag_clear_s))
        a_train, a_test, _ = m.time_split(a_rows)
        a_test_start = a_test.agg(F.date_format(F.min("datetime_vn"), "yyyy-MM-dd HH:mm:ss")).first()[0]
        b_train, b_test, _ = m.time_split(feats)
        c_train = m.add_regime(feats.where((F.col("datetime_vn") < ts(a_test_start)) & ~((F.col("datetime_vn") >= ts(trans_from_s)) & (F.col("datetime_vn") < ts(lag_clear_s)))), post_start_s)
        c_test = m.add_regime(a_test, post_start_s)

        res_a = m.run_setup(a_train, a_test, m.BASE_FEATURES)
        res_b = m.run_setup(b_train, b_test, m.BASE_FEATURES)
        res_c = m.run_setup(c_train, c_test, m.BASE_FEATURES + ["regime"])
        # mốc "trung bình theo giờ" của C lấy từ các dòng huấn luyện cùng chế độ với tập kiểm tra (regime = 1): so với mốc trộn hai chế độ thì quá dễ thắng
        c_regime1 = c_train.where(F.col("regime") == 1)
        res_c["baselines"]["hourly-mean"] = m.metrics(m.hourly_mean_predictions(c_regime1, c_test).where(F.col("prediction").isNotNull()))

        # 3. độ nhạy của A với lựa chọn điểm bắt đầu giai đoạn sau (cùng tập kiểm tra, chỉ đổi đầu của tập huấn luyện)
        sens = []
        starts = {"mốc quy ước 01/07/2025": datetime(2025, 7, 1), "ranh giới chính (ramp ±60 ngày)": post_start}
        w90 = next(x for x in trans["windows"] if x["halfWidthDays"] == 90)
        starts["kết thúc chuyển tiếp theo cửa sổ ±90 ngày"] = datetime.combine(date.fromisoformat(w90["to"]) + timedelta(days=1), datetime.min.time())
        best_kind = res_a["best"]
        for label, st in sorted(starts.items(), key=lambda kv: kv[1]):
            tr = feats.where((F.col("datetime_vn") >= ts((st + timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S"))) & (F.col("datetime_vn") < ts(a_test_start)))
            mt = m.metrics(m.fit(best_kind, tr, m.BASE_FEATURES).transform(a_test))
            sens.append({"label": label, "postStart": st.strftime("%Y-%m-%d"), "trainRows": tr.count(), "kind": best_kind, "test": m.round_metrics(mt)})

        # 4. lắp JSON theo hợp đồng
        span_a_tr, span_a_te = span(a_train), span(a_test)
        span_b_tr, span_b_te = span(b_train), span(b_test)
        span_c_tr = span(c_train)
        b_test_post = b_test.where(F.col("datetime_vn") >= ts(post_start_s)).count()
        b_train_trans = b_train.where((F.col("datetime_vn") >= ts(trans_from_s))).count()
        excluded_days = (t_to - t_from).days + 1
        a_json = {"models": models_json(res_a, version_for)}
        hm_desc_a = "Dự đoán bằng CO trung bình của cùng giờ +07 trong tập huấn luyện của phương án."
        effect = (f"Cùng tập kiểm tra của phương án chính ({span_a_te['rows']} giờ), mô hình tốt nhất của mỗi cách huấn luyện (RMSE µg/m³): "
                  f"chỉ giai đoạn sau chuyển tiếp {res_a['models'][best_kind]['test']['rmse']:.1f} ({m.NAMES[best_kind]}); "
                  f"toàn bộ có biến chế độ {res_c['models'][res_c['best']]['test']['rmse']:.1f} ({m.NAMES[res_c['best']]}); "
                  f"mốc CO giờ trước {res_a['baselines']['previous-hour']['rmse']:.1f}. "
                  "Độ nhạy với điểm bắt đầu giai đoạn sau: " + "; ".join(f"{s['label']} ({s['postStart']}): {s['test']['rmse']:.1f}" for s in sens) + ".")
        metrics_doc = {
            "generatedAt": c.now_iso(), "target": TARGET, "horizonHours": 1, "features": m.BASE_FEATURES,
            "trainedOn": (f"Bộ Kaggle (OpenWeather) đã làm sạch (/co/processed/aqi_clean), CHỈ giai đoạn sau chuyển tiếp: từ {span_a_tr['from']} đến {span_a_tr['to']} ({span_a_tr['rows']} giờ), "
                          f"đã loại 24 giờ đầu của giai đoạn (độ trễ 24 giờ cần dữ liệu trong chính giai đoạn). Kiểm tra trên {span_a_te['from']} đến {span_a_te['to']} ({span_a_te['rows']} giờ), "
                          "chỉ phủ khoảng 2 tháng cuối (mùa vụ khác với phần huấn luyện). Dữ liệu gốc đã tiền xử lý từ trước (o3 điền ở 14,9% dòng, độ ẩm/nhiệt độ có dấu hiệu nội suy)."),
            "trainSet": span_a_tr, "testSet": span_a_te,
            "splitMethod": "Theo thời gian: 80% đầu của giai đoạn sau chuyển tiếp để huấn luyện, 20% cuối để kiểm tra, không xáo ngẫu nhiên. Mô hình tốt nhất chọn theo RMSE trên 20% cuối của tập huấn luyện (kiểm định), không chọn trên tập kiểm tra.",
            "models": a_json["models"], "baselines": baselines_json(res_a, hm_desc_a), "conclusion": conclusion_text(res_a, span_a_te["rows"]),
            "breakpoint": {"date": iso(post_start_s), "strategy": "separate-training",
                           "description": (f"Phương án chính huấn luyện riêng cho giai đoạn từ {post_start.strftime('%d/%m/%Y')} (ngay sau chuyển tiếp {t_from.strftime('%d/%m/%Y')} đến {t_to.strftime('%d/%m/%Y')}, xác định theo dữ liệu, không theo mốc 01/07). "
                                           f"{excluded_days} ngày chuyển tiếp và 24 giờ sau đó bị loại khỏi tập huấn luyện. Điểm gãy là chuyển tiếp kéo dài chứ không phải một ngày; ranh giới thay đổi nhiều theo cửa sổ khớp (xem transition.windows)."),
                           "effect": effect, "cause": CAUSE},
            "transition": {**trans, "excludedDays": excluded_days, "excludedFrom": t_from.isoformat(), "excludedTo": t_to.isoformat(), "postStart": post_start.strftime("%Y-%m-%d"),
                           "sensitivity": sens,
                           "note": "Ranh giới không duy nhất: các cửa sổ khớp khác nhau cho khoảng khác nhau (windows). Khoảng 18 đến 30/06/2025 là bước giảm nhanh nhất, nhưng sau khi khử mùa vụ, CO giảm dần từ tháng 4 đến tháng 9/2025 (monthlyResidual)."},
            "alternatives": [
                {"id": "full-80-20", "title": "Chia 80/20 theo thời gian trên toàn bộ dữ liệu (yêu cầu gốc)",
                 "description": (f"Huấn luyện {span_b_tr['from']} đến {span_b_tr['to']} ({span_b_tr['rows']} giờ), kiểm tra {span_b_te['from']} đến {span_b_te['to']} ({span_b_te['rows']} giờ). "
                                 f"Tập kiểm tra có {b_test_post} giờ ở giai đoạn sau chuyển tiếp ({b_test_post / span_b_te['rows'] * 100:.0f}%) và gồm cả các ngày chuyển tiếp; tập huấn luyện có {b_train_trans} giờ từ ngày bắt đầu chuyển tiếp trở đi. "
                                 "Mốc trung bình theo giờ tính trên tập huấn luyện toàn mức cao nên không tương xứng với tập kiểm tra mức thấp."),
                 "features": m.BASE_FEATURES, "trainSet": span_b_tr, "testSet": span_b_te,
                 "splitMethod": "Theo thời gian: 80% dòng đầu của toàn bộ dữ liệu để huấn luyện, 20% cuối để kiểm tra; không loại ngày chuyển tiếp (đúng yêu cầu gốc).",
                 "models": models_json(res_b, version_for), "baselines": baselines_json(res_b, "CO trung bình của cùng giờ +07 trong tập huấn luyện (toàn mức cao, trước chuyển tiếp)."),
                 "conclusion": conclusion_text(res_b, span_b_te["rows"])},
                {"id": "full-with-regime", "title": "Huấn luyện toàn bộ có biến chế độ, kiểm tra trên tập kiểm tra của phương án chính",
                 "description": (f"Huấn luyện {span_c_tr['from']} đến {span_c_tr['to']} ({span_c_tr['rows']} giờ) gồm cả trước chuyển tiếp (regime=0) và sau (regime=1), đã loại {excluded_days} ngày chuyển tiếp và 24 giờ sau đó; "
                                 "kiểm tra đúng tập kiểm tra của phương án chính. Chia 80/20 trên toàn bộ dữ liệu thì tập huấn luyện không có dòng nào ở chế độ sau (regime hằng số) nên không đo được tác dụng của biến chế độ; "
                                 "vì vậy phương án này dùng tập kiểm tra của phương án chính."),
                 "features": m.BASE_FEATURES + ["regime"], "trainSet": span_c_tr, "testSet": span_a_te,
                 "splitMethod": "Theo thời gian: mọi giờ trước tập kiểm tra của phương án chính (trừ ngày chuyển tiếp) để huấn luyện; 20% cuối của giai đoạn sau để kiểm tra.",
                 "models": models_json(res_c, version_for), "baselines": baselines_json(res_c, "CO trung bình của cùng giờ +07 trong các dòng huấn luyện cùng chế độ với tập kiểm tra (regime=1)."),
                 "conclusion": conclusion_text(res_c, span_a_te["rows"])}]}

        # 5. lưu mô hình, dự đoán trên tập kiểm tra, metrics (bước 7)
        best_model = res_a["fitted"][best_kind]
        writer = best_model.write()
        (writer.overwrite() if a.overwrite else writer).save(f"{c.HDFS_URI}{MODEL_DIR}")
        hm_df = m.hourly_mean_predictions(a_train, a_test).select("datetime_utc", F.col("prediction").alias("hourlyMean"))
        preds = (best_model.transform(a_test).select("datetime_utc", F.date_format("datetime_vn", "yyyy-MM-dd HH:mm:ss").alias("datetime_vn"), F.col("co").alias("actual"),
                                                     F.round("prediction", 2).alias("predicted"), F.col("coLag1h").alias("previousHour"))
                 .join(hm_df, "datetime_utc", "left").select("datetime_vn", "actual", "predicted", "previousHour", F.round("hourlyMean", 2).alias("hourlyMean")).orderBy("datetime_vn"))
        preds.coalesce(1).write.mode("overwrite" if a.overwrite else "errorifexists").parquet(f"{c.HDFS_URI}{PRED_DIR}")
        c.hdfs_write_text(METRICS_JSON, json.dumps(metrics_doc, ensure_ascii=False, indent=2) + "\n")
        med = a_train.agg(*[F.percentile_approx(f, 0.5).alias(f) for f in m.BASE_FEATURES]).first().asDict()
        ref = {"generatedAt": c.now_iso(), "modelVersion": version_for(best_kind), "features": m.BASE_FEATURES,
               "note": "Trung vị của từng đặc trưng trên tập huấn luyện của phương án chính; backend điền vào đầu vào mà form /api/predict bỏ trống và liệt kê ở filledInputs.",
               "median": {k: round(float(v), 4) for k, v in med.items()}}
        c.hdfs_write_text(REF_JSON, json.dumps(ref, ensure_ascii=False, indent=2) + "\n")

        A, bm = metrics_doc, next(x for x in metrics_doc["models"] if x["isBest"])
        r6.summary = [c.item("Mô hình tốt nhất (phương án chính, theo kiểm định)", f"{bm['name']} ({bm['version']})", None),
                      c.item("RMSE / MAE / R² trên tập kiểm tra", f"{bm['test']['rmse']} / {bm['test']['mae']} / {bm['test']['r2']}", "µg/m³"),
                      c.item("RMSE mốc CO giờ trước / trung bình theo giờ", f"{A['baselines'][0]['test']['rmse']} / {A['baselines'][1]['test']['rmse']}", "µg/m³"),
                      c.item("Thắng mốc CO giờ trước", "có" if bm["beatsPreviousHour"] else "KHÔNG", None),
                      c.item("Chuyển tiếp theo dữ liệu (cửa sổ ±60 ngày)", f"{trans['main']['from']} đến {trans['main']['to']}", None),
                      c.item("Tập huấn luyện / kiểm tra (phương án chính)", f"{span_a_tr['rows']} / {span_a_te['rows']}", "giờ")]
        r6.detail = ("Bước đã xong cả 6a (phân cụm ngày, độ ổn định theo seed) và 6b (mô hình dự đoán CO). " + A["conclusion"] + " Nguyên nhân điểm gãy dữ liệu chưa xác định; "
                     "không kết luận CO ngoài đời giảm thật. Chi tiết, hai phương án so sánh và độ nhạy ở /co/output/model_metrics.json.")

        # kiểm lại những gì đã lưu: nạp mô hình từ HDFS, dự đoán lại 200 dòng kiểm tra, so với bản trong bộ nhớ
        loaded = PipelineModel.load(f"{c.HDFS_URI}{MODEL_DIR}")
        probe = a_test.orderBy("datetime_utc").limit(200)
        want = [round(x["prediction"], 6) for x in best_model.transform(probe).orderBy("datetime_utc").select("prediction").collect()]
        got = [round(x["prediction"], 6) for x in loaded.transform(probe).orderBy("datetime_utc").select("prediction").collect()]
        if want != got:
            raise RuntimeError("mô hình nạp lại từ HDFS cho dự đoán khác bản trong bộ nhớ")
        n_pred = spark.read.parquet(f"{c.HDFS_URI}{PRED_DIR}").count()
        if n_pred != span_a_te["rows"]:
            raise RuntimeError("số dòng dự đoán đã lưu không khớp tập kiểm tra")
        sizes = {p: int(c.hdfs("-du", "-s", p).stdout.split()[0]) for p in (MODEL_DIR, PRED_DIR, METRICS_JSON, REF_JSON)}
        saved = (loaded, n_pred, sizes, want)

    with c.step("save-results") as r7:
        loaded, n_pred, sizes, want = saved
        r7.summary = [c.item("Mô hình đã lưu", MODEL_DIR, None), c.item("Dung lượng mô hình", sizes[MODEL_DIR], "byte"), c.item("Phiên bản mô hình", bm["version"], None),
                      c.item("Dự đoán trên tập kiểm tra đã lưu", n_pred, "dòng"), c.item("Dung lượng dự đoán", sizes[PRED_DIR], "byte"),
                      c.item("Chỉ số mô hình", METRICS_JSON, None), c.item("Dung lượng chỉ số", sizes[METRICS_JSON], "byte"),
                      c.item("Trung vị đặc trưng (điền đầu vào thiếu)", REF_JSON, None)]
        r7.detail = (f"Đã lưu mô hình {bm['name']} ({bm['version']}) dạng Spark PipelineModel, dự đoán trên {n_pred} giờ kiểm tra (Parquet) và model_metrics.json trong {c.OUTPUT_DIR}. "
                     "Đã nạp lại mô hình từ HDFS và dự đoán lại 200 giờ kiểm tra: kết quả trùng bản trong bộ nhớ. Kết quả các bước 5, 6a ở cùng thư mục (sql_analysis.json, day_clusters*).")
    spark.stop()
    print(f"[06b_train] xong: chuyển tiếp {trans['main']['from']}..{trans['main']['to']}; phương án chính train {span_a_tr['rows']} / test {span_a_te['rows']}; tốt nhất {bm['name']} RMSE {bm['test']['rmse']} "
          f"(mốc CO giờ trước {A['baselines'][0]['test']['rmse']}, trung bình theo giờ {A['baselines'][1]['test']['rmse']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
