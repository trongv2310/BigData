"""Bước 4 (làm sạch): /co/raw/Dataset.csv -> Parquet /co/processed/aqi_clean.

  spark-submit --driver-memory 1g pipeline/04_clean.py [--overwrite]

Quy tắc (mỗi quy tắc đếm số dòng/ô nó thật sự đụng tới, ghi vào step_summary.json; không quy tắc nào điền số thay thế). Phần biến đổi nằm ở cleaning.py:
  1. datetime đọc theo dd-MM-yy H:mm là giờ UTC (đã xác nhận ở 02_check_raw: cột hour/month/dayofweek của file khớp UTC); thêm datetime_vn = UTC + 7 giờ.
     Dòng không đọc được datetime bị bỏ.
  2. Dòng có CO thiếu, không phải số hoặc âm bị bỏ (CO là đại lượng cần dự đoán); thiếu ở cột khác thì giữ null.
  3. datetime trùng: giữ một dòng.
  4. Ô không phải số -> null. Ô ngoài miền của data_dictionary.csv -> null, TRỪ độ ẩm > 100 (bão hòa, nằm thành chuỗi giờ liên tiếp trông như nội suy vượt): cắt về 100
     và đánh cờ humidity_clipped = 1 để còn truy được. Không nội suy, không điền trung bình.
  5. Ép kiểu; humidity_% đổi tên thành humidity_pct.
  6. Thay hour, month, dayofweek (file tính theo UTC) bằng giá trị theo giờ +07 (dayofweek: 0 = Thứ Hai); thêm year và after_2025_07_01
     (= 1 khi datetime_vn từ 01/07/2025 00:00, điểm gãy dữ liệu: CO giảm mạnh).
Không có dòng nào được "tạo thêm": số dòng ra <= số dòng vào. Đã có /co/processed/aqi_clean thì dừng, chỉ ghi đè khi có --overwrite (hỏi người dùng trước).
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as c  # noqa: E402
import cleaning  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--overwrite", action="store_true", help="ghi đè /co/processed/aqi_clean nếu đã có")
    a = ap.parse_args()
    if c.hdfs_exists(c.CLEAN_DIR) and not a.overwrite:
        print(f"[04_clean] {c.CLEAN_DIR} đã có; chạy lại với --overwrite nếu chắc chắn muốn ghi đè (hỏi người dùng trước).", file=sys.stderr)
        return 2

    from pyspark.sql import functions as F

    with c.step("clean") as r:
        spark = c.spark_session("HCMC_CO_04_clean")
        raw = spark.read.option("header", True).option("inferSchema", False).csv(f"{c.HDFS_URI}{c.RAW_FILE}")
        df, st = cleaning.clean(raw)
        df.coalesce(1).write.mode("overwrite" if a.overwrite else "errorifexists").parquet(f"{c.HDFS_URI}{c.CLEAN_DIR}")

        # đọc lại từ HDFS để kiểm, không tin biến trong bộ nhớ
        back = spark.read.parquet(f"{c.HDFS_URI}{c.CLEAN_DIR}")
        chk = back.agg(F.count("*").alias("rows"), F.date_format(F.min("datetime_vn"), "yyyy-MM-dd HH:mm").alias("vn_min"),
                       F.date_format(F.max("datetime_vn"), "yyyy-MM-dd HH:mm").alias("vn_max"),
                       F.sum("after_2025_07_01").alias("after"), F.sum("humidity_clipped").alias("clipped"),
                       F.count(F.when(F.col("co").isNull(), 1)).alias("co_null"), F.max("humidity_pct").alias("hum_max"),
                       F.min("hour").alias("h_min"), F.max("hour").alias("h_max"), F.min("dayofweek").alias("d_min"), F.max("dayofweek").alias("d_max")).first().asDict()
        n_in, n_out = st["rowsIn"], chk["rows"]
        expected = n_in - st["droppedBadDatetime"] - st["droppedBadCo"] - st["droppedDuplicates"]
        if n_out != expected:
            raise RuntimeError(f"số dòng ghi ra ({n_out}) không khớp phép tính ({expected})")
        if chk["co_null"] or (chk["hum_max"] or 0) > 100 or chk["clipped"] != st["humidityClipped"]:
            raise RuntimeError(f"kiểm sau khi ghi thất bại: {chk}")
        size = int(c.hdfs("-du", "-s", c.CLEAN_DIR).stdout.split()[0])
        after = int(chk["after"])
        r.summary = [c.item("Dòng vào", n_in, "dòng"), c.item("Dòng ra", n_out, "dòng"),
                     c.item("Bỏ vì datetime không đọc được", st["droppedBadDatetime"], "dòng"), c.item("Bỏ vì CO thiếu hoặc không hợp lệ", st["droppedBadCo"], "dòng"),
                     c.item("Bỏ vì datetime trùng", st["droppedDuplicates"], "dòng"),
                     c.item("Ô chuyển thành null", st["nulledTotal"], "ô"), c.item("Độ ẩm > 100 cắt về 100", st["humidityClipped"], "ô"),
                     c.item("Trước 01/07/2025 (giờ +07)", n_out - after, "dòng"), c.item("Từ 01/07/2025 (giờ +07)", after, "dòng"),
                     c.item("Từ (giờ +07)", chk["vn_min"], None), c.item("Đến (giờ +07)", chk["vn_max"], None), c.item("Dung lượng Parquet", size, "byte")]
        r.detail = (f"Ghi {c.CLEAN_DIR} ({len(cleaning.OUTPUT_COLUMNS)} cột). Kiểm sau khi đọc lại từ HDFS: không còn CO null, độ ẩm lớn nhất {chk['hum_max']}, "
                    f"hour {chk['h_min']}..{chk['h_max']}, dayofweek {chk['d_min']}..{chk['d_max']}. Ô null theo cột: {st['nulledByColumn'] or 'không có'}. "
                    "datetime_vn = datetime_utc + 7 giờ (phiên Spark đặt UTC nên đồng hồ hiển thị là giờ Việt Nam, như view aqi của /api/sql); "
                    "hour, month, dayofweek, year tính theo datetime_vn (cột cùng tên của file gốc tính theo UTC nên bị thay). "
                    "Dữ liệu gốc đã qua tiền xử lý từ trước: o3 được điền ở o3_was_missing=1 (giữ nguyên cờ này), và độ ẩm/nhiệt độ có nhiều giá trị lẻ trông như đã nội suy; "
                    "pipeline này không biết chính xác nội suy ở đâu.")
        spark.stop()
    print(f"[04_clean] xong: {n_in} -> {n_out} dòng; null {st['nulledTotal']} ô; độ ẩm cắt {st['humidityClipped']}; trước/sau 01/07/2025: {n_out - after}/{after}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
