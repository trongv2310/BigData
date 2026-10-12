"""Kiểm câu SQL trước khi đưa cho Spark: đúng một câu SELECT (hoặc WITH ... SELECT), không có hàm/từ khóa nguy hiểm.

Đây là lớp chặn thứ nhất. Lớp thứ hai nằm ở cấu hình SparkSession (sql_engine.py): catalog trong bộ nhớ chỉ có view `aqi`,
`spark.sql.runSQLOnFiles=false` (không đọc file tùy ý bằng csv.`/đường/dẫn`), không UI.
"""
import re

MAX_LENGTH = 5000

# Từ khóa của câu lệnh ghi/đổi trạng thái, và các hàm chạy mã JVM tùy ý (reflect...).
_FORBIDDEN = {
    "insert", "update", "delete", "drop", "create", "alter", "truncate", "merge", "grant", "revoke", "set", "reset", "use",
    "cache", "uncache", "refresh", "load", "copy", "export", "add", "analyze", "msck", "repair", "call", "explain",
    "reflect", "try_reflect", "java_method", "input_file_name", "input_file_block_start", "input_file_block_length",
}


class SqlNotAllowed(ValueError):
    pass


def _strip_comments_and_literals(sql: str) -> str:
    """Trả về câu SQL với comment bị bỏ và nội dung chuỗi '...' / "..." thay bằng khoảng trắng (để dò từ khóa và dấu ;)."""
    out, i, n = [], 0, len(sql)
    while i < n:
        c = sql[i]
        if sql.startswith("--", i):
            j = sql.find("\n", i)
            i = n if j < 0 else j
        elif sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            if j < 0:
                raise SqlNotAllowed("Comment /* chưa đóng")
            i = j + 2
            out.append(" ")
        elif c in ("'", '"'):
            j = i + 1
            while j < n:
                if sql[j] == "\\":
                    j += 2
                    continue
                if sql[j] == c:
                    if j + 1 < n and sql[j + 1] == c:  # '' là dấu nháy trong chuỗi
                        j += 2
                        continue
                    break
                j += 1
            if j >= n:
                raise SqlNotAllowed("Chuỗi ký tự chưa đóng")
            out.append(" ")
            i = j + 1
        else:
            out.append(c)
            i += 1
    return "".join(out)


def check(query: str) -> str:
    """Trả về câu đã chuẩn hóa (bỏ dấu ; cuối). Ném SqlNotAllowed nếu không được phép."""
    if len(query) > MAX_LENGTH:
        raise SqlNotAllowed(f"Câu lệnh dài quá {MAX_LENGTH} ký tự")
    if "`" in query:
        raise SqlNotAllowed("Không dùng dấu backtick; tên cột và bảng chỉ gồm chữ, số, gạch dưới")
    plain = _strip_comments_and_literals(query).strip()
    while plain.endswith(";"):
        plain = plain[:-1].rstrip()
    if not plain:
        raise SqlNotAllowed("Câu lệnh rỗng")
    if ";" in plain:
        raise SqlNotAllowed("Chỉ chấp nhận một câu lệnh")
    words = re.findall(r"[A-Za-z_][A-Za-z_0-9]*", plain.lower())
    if words[0] not in ("select", "with"):
        raise SqlNotAllowed("Chỉ chấp nhận câu SELECT (hoặc WITH ... SELECT)")
    bad = sorted({w for w in words if w in _FORBIDDEN})
    if bad:
        raise SqlNotAllowed(f"Không cho phép: {', '.join(bad)}")
    # Giữ nguyên câu gốc (kể cả comment/chuỗi) nhưng bỏ ; cuối
    q = query.strip()
    while q.endswith(";"):
        q = q[:-1].rstrip()
    return q
