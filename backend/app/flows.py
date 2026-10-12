"""Định nghĩa các luồng để frontend vẽ. Phải trùng mock/flows.json và mock/flow-*.json (có test giữ cho hai bên không lệch)."""

SUMMARIES = [
    {"id": "hdfs-write", "title": "HDFS ghi file", "status": "available",
     "description": "Client xin NameNode block, đẩy dữ liệu xuống DataNode, rồi đóng file."},
    {"id": "hdfs-read", "title": "HDFS đọc file", "status": "planned",
     "description": "Client hỏi NameNode vị trí block rồi đọc thẳng từ DataNode."},
    {"id": "kafka-produce", "title": "Kafka nhận bản ghi", "status": "planned",
     "description": "Producer gửi bản ghi vào topic hcmc_co_stream."},
    {"id": "spark-streaming", "title": "Spark Structured Streaming", "status": "planned",
     "description": "Đọc luồng, phân loại mức CO, ghi kết quả."},
    {"id": "sparksql", "title": "SparkSQL", "status": "planned",
     "description": "Truy vấn dữ liệu trên HDFS bằng SQL."},
]

# Số bước của hdfs-write (khớp steps[].order bên dưới), dùng cho flow.step
STEP_CREATE, STEP_ADD_BLOCK, STEP_LOCATIONS, STEP_SEND, STEP_ACK, STEP_RECEIVED, STEP_COMPLETE, STEP_RENAME = 1, 2, 3, 4, 5, 6, 7, 8


def _step(order, frm, to, label, channel, repeats):
    return {"order": order, "from": frm, "to": to, "label": label, "channel": channel, "repeats": repeats}


DEFINITIONS = {
    "hdfs-write": {
        "id": "hdfs-write",
        "title": "HDFS ghi file",
        "components": [
            {"id": "client", "label": "Client (hdfs dfs -put)", "kind": "client", "note": "Chương trình ghi file"},
            {"id": "namenode", "label": "NameNode", "kind": "namenode", "note": "Giữ metadata, không giữ dữ liệu"},
            {"id": "datanode-1", "label": "DataNode", "kind": "datanode",
             "note": "Giữ block thật. Đây là DataNode đầu pipeline; tên thật có trong hdfs.op.datanode"},
            {"id": "secondary", "label": "Secondary NameNode", "kind": "secondary-namenode", "note": "Checkpoint định kỳ"},
        ],
        "steps": [
            _step(1, "client", "namenode", "create(): xin tạo file, NameNode ghi vào edit log", "control", None),
            _step(2, "client", "namenode", "addBlock(): xin block mới và danh sách DataNode", "control", "mỗi block"),
            _step(3, "namenode", "client", "Trả về blockId và vị trí DataNode", "control", "mỗi block"),
            _step(4, "client", "datanode-1", "Gửi dữ liệu theo packet 64 KB (cổng truyền dữ liệu của DataNode, ví dụ 9866)", "data", "mỗi block"),
            _step(5, "datanode-1", "client", "ACK cho từng packet", "data", "mỗi packet"),
            _step(6, "datanode-1", "namenode", "blockReceived: báo đã nhận đủ block", "control", "mỗi block"),
            _step(7, "client", "namenode", "complete(): đóng file", "control", None),
            _step(8, "client", "namenode", "rename: bỏ hậu tố ._COPYING_", "control", None),
            _step(9, "secondary", "namenode", "Checkpoint gộp edit log vào fsimage (mỗi giờ)", "background", None),
        ],
    },
}
