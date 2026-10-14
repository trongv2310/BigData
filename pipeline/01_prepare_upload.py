"""Bước 1 (chuẩn bị HDFS) và bước 2 (upload Dataset.csv).

  python3 pipeline/01_prepare_upload.py [--local /mnt/f/HK1_Nam04/BigData/Data/hcmc_aqi_dataset_after_preprocess_final.csv] [--force]

- Tạo /co/raw, /co/processed, /co/output (mkdir -p, chỉ thêm thư mục mới).
- Sao chép file CSV cục bộ vào /co/raw/Dataset.csv. KHÔNG đụng /raw/Dataset.csv cũ. Nếu /co/raw/Dataset.csv đã có và khác file cục bộ thì dừng,
  chỉ ghi đè khi có --force (ghi đè là việc không đảo ngược, hãy hỏi người dùng trước).
- Kiểm lại: kích thước, số dòng dữ liệu, số cột, md5 của bản trên HDFS phải khớp bản cục bộ.
Không cần Spark. Kết quả thật ghi vào /co/output/step_summary.json (bước prepare-hdfs và upload-raw).
"""
import argparse
import hashlib
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as c  # noqa: E402

DEFAULT_LOCAL = "/mnt/f/HK1_Nam04/BigData/Data/hcmc_aqi_dataset_after_preprocess_final.csv"


def local_stats(path: str) -> dict:
    md5, size, lines, header = hashlib.md5(), 0, 0, None
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.readline(), b""):
            md5.update(chunk)
            size += len(chunk)
            lines += 1
            header = header or chunk.decode("utf-8-sig").strip()
    return {"md5": md5.hexdigest(), "bytes": size, "rows": lines - 1, "columns": header.split(","), "header": header}


def hdfs_stats(path: str) -> dict:
    p = subprocess.Popen([c.HDFS_BIN, "dfs", "-cat", path], stdout=subprocess.PIPE)
    md5, size, lines, header = hashlib.md5(), 0, 0, None
    for chunk in iter(p.stdout.readline, b""):
        md5.update(chunk)
        size += len(chunk)
        lines += 1
        header = header or chunk.decode("utf-8-sig").strip()
    if p.wait() != 0:
        raise RuntimeError(f"không đọc được {path} trên HDFS")
    return {"md5": md5.hexdigest(), "bytes": size, "rows": lines - 1, "columns": header.split(","), "header": header}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--local", default=DEFAULT_LOCAL)
    ap.add_argument("--force", action="store_true", help="ghi đè /co/raw/Dataset.csv nếu đã có bản khác")
    a = ap.parse_args()

    # Đếm TRƯỚC khi bước bắt đầu: ghi step_summary.json ở bước "running" đã tự tạo /co/output rồi.
    before = [d for d in (c.RAW_DIR, c.PROCESSED_DIR, c.OUTPUT_DIR) if c.hdfs_exists(d)]
    with c.step("prepare-hdfs") as r:
        c.hdfs("-mkdir", "-p", c.RAW_DIR, c.PROCESSED_DIR, c.OUTPUT_DIR)
        missing = [d for d in (c.RAW_DIR, c.PROCESSED_DIR, c.OUTPUT_DIR) if not c.hdfs_exists(d)]
        if missing:
            raise RuntimeError(f"không tạo được: {missing}")
        r.summary = [c.item("Thư mục của dự án", 3, "thư mục"), c.item("Tạo mới ở lần chạy này", 3 - len(before), "thư mục")]
        r.detail = f"{c.RAW_DIR} (dữ liệu gốc), {c.PROCESSED_DIR} (đã làm sạch), {c.OUTPUT_DIR} (kết quả và step_summary.json). Chỉ thêm thư mục, không sửa gì ở /raw, /stream_input, /live."

    with c.step("upload-raw") as r:
        local = local_stats(a.local)
        if c.hdfs_exists(c.RAW_FILE):
            remote = hdfs_stats(c.RAW_FILE)
            if remote["md5"] == local["md5"]:
                action = "đã có bản giống hệt, không ghi lại"
            elif a.force:
                c.hdfs("-put", "-f", a.local, c.RAW_FILE)
                action = "đã ghi đè (--force)"
            else:
                raise RuntimeError(f"{c.RAW_FILE} đã có và KHÁC file cục bộ (md5 {remote['md5'][:8]} và {local['md5'][:8]}); chạy lại với --force nếu chắc chắn ghi đè")
        else:
            c.hdfs("-put", a.local, c.RAW_FILE)
            action = "đã upload"
        remote = hdfs_stats(c.RAW_FILE)
        if remote["md5"] != local["md5"] or remote["bytes"] != local["bytes"]:
            raise RuntimeError("bản trên HDFS không khớp file cục bộ sau khi upload")
        if remote["columns"] != c.RAW_COLUMNS:
            raise RuntimeError(f"cột trong file khác kỳ vọng của data_dictionary.csv: {remote['columns']}")
        r.summary = [c.item("Số dòng dữ liệu", remote["rows"], "dòng"), c.item("Số cột", len(remote["columns"]), "cột"),
                     c.item("Dung lượng", remote["bytes"], "byte"), c.item("md5 (12 ký tự đầu)", remote["md5"][:12], None)]
        r.detail = (f"{action}: {a.local} -> {c.RAW_FILE}. Số dòng, số cột, kích thước và md5 đọc lại từ HDFS khớp file cục bộ. "
                    "Lưu ý: datetime trong file có dạng dd-MM-yy H:mm (không phải M/D/Y như data_dictionary.csv ghi) và là giờ UTC.")
    print(f"[01_prepare_upload] xong: {remote['rows']} dòng, {len(remote['columns'])} cột, {remote['bytes']} byte ({action})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
