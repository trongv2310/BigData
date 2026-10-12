"""Đọc log NameNode (chỉ đọc file) để biết thao tác ghi HDFS thật.

Mức INFO mặc định của NameNode có sẵn:
  BLOCK* allocate blk_X_gs, replicas=ip:port[, ...] for /path        -> addBlock
  DIR* completeFile: /path is closed by DFSClient_...                -> close
Create/mkdir/rename/delete chỉ có trong audit log (hdfs-audit.log), mặc định TẮT (hdfs.audit.logger=INFO,NullAppender).
Nếu người dùng bật audit và file tồn tại, ta đọc thêm nó. Backend KHÔNG tự đổi cấu hình Hadoop.
"""
import glob
import os
import re
from dataclasses import dataclass, field
from datetime import datetime

from . import config

_TS = r"(?P<ts>\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3})"
_ALLOCATE = re.compile(_TS + r" INFO \S*StateChange: BLOCK\* allocate (?P<block>blk_\d+)_\d+, replicas=(?P<replicas>\S+(?:, \S+)*) for (?P<path>.+)$")
_COMPLETE = re.compile(_TS + r" INFO \S*StateChange: DIR\* completeFile: (?P<path>.+) is closed by ")
_AUDIT = re.compile(_TS + r" INFO \S*FSNamesystem\.audit: allowed=true\s.*?\bcmd=(?P<cmd>\w+)\s+src=(?P<src>\S+)\s+dst=(?P<dst>\S+)")
_AUDIT_OPS = {"create": "create", "mkdirs": "mkdir", "rename": "rename", "delete": "delete"}


@dataclass
class LogEvent:
    kind: str  # allocate | complete | create | mkdir | rename | delete
    ts: datetime  # giờ của máy chạy NameNode (WSL = +07)
    path: str
    block: str | None = None
    replicas: list[str] = field(default_factory=list)  # "ip:xferPort"
    dst: str | None = None


def _ts(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S,%f")


def parse_line(line: str) -> LogEvent | None:
    m = _ALLOCATE.match(line)
    if m:
        return LogEvent("allocate", _ts(m["ts"]), m["path"].strip(), block=m["block"], replicas=m["replicas"].split(", "))
    m = _COMPLETE.match(line)
    if m:
        return LogEvent("complete", _ts(m["ts"]), m["path"].strip())
    m = _AUDIT.match(line)
    if m and m["cmd"] in _AUDIT_OPS:
        dst = None if m["dst"] == "null" else m["dst"]
        return LogEvent(_AUDIT_OPS[m["cmd"]], _ts(m["ts"]), m["src"], dst=dst)
    return None


class Tailer:
    """Đọc phần mới được nối vào file có tên khớp `pattern`. Lần đầu bắt đầu từ cuối file (không phát lại lịch sử)."""

    def __init__(self, pattern: str):
        self.pattern = pattern
        self._inode: int | None = None
        self._pos = 0
        self._rest = b""
        self._primed = False  # đã gặp file ít nhất một lần chưa

    def exists(self) -> bool:
        return bool(glob.glob(self.pattern))

    def read_new(self) -> list[str]:
        files = glob.glob(self.pattern)
        if not files:
            self._primed, self._inode, self._pos, self._rest = True, None, 0, b""  # file xuất hiện sau thì đọc từ đầu
            return []
        path = max(files, key=os.path.getmtime)
        st = os.stat(path)
        if not self._primed:
            self._primed, self._inode, self._pos = True, st.st_ino, st.st_size
            return []
        if st.st_ino != self._inode or st.st_size < self._pos:  # xoay vòng hoặc bị cắt: đọc lại từ đầu
            self._inode, self._pos, self._rest = st.st_ino, 0, b""
        if st.st_size == self._pos:
            return []
        with open(path, "rb") as f:
            f.seek(self._pos)
            data = self._rest + f.read(st.st_size - self._pos)
        self._pos = st.st_size
        *lines, self._rest = data.split(b"\n")  # phần cuối chưa có xuống dòng thì giữ lại lượt sau
        return [l.decode("utf-8", "replace").rstrip("\r") for l in lines]


def namenode_tailer() -> Tailer:
    return Tailer(os.path.join(config.HADOOP_LOG_DIR, "hadoop-*-namenode-*.log"))


def audit_tailer() -> Tailer:
    return Tailer(os.path.join(config.HADOOP_LOG_DIR, "hdfs-audit.log"))
