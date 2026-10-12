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


# Luồng kafka-produce và spark-streaming: chỉ công bố khi config.FLOWS_EXTRA (xem config.py). Số bước dùng cho flow.step.
KAFKA_STEP_SEND, KAFKA_STEP_ACK = 1, 2
SPARK_STEP_READ, SPARK_STEP_COMPUTE, SPARK_STEP_WRITE, SPARK_STEP_CHECKPOINT = 1, 2, 3, 4

EXTRA_DEFINITIONS = {
    "kafka-produce": {
        "id": "kafka-produce",
        "title": "Kafka nhận bản ghi",
        "components": [
            {"id": "producer", "label": "Producer (live_producer.py)", "kind": "client",
             "note": "Lấy CO và thời tiết từ Open-Meteo, chỉ gửi khi có quan sát mới"},
            {"id": "broker", "label": "Kafka broker", "kind": "kafka-broker", "note": "Topic hcmc_co_stream, 1 partition; ghi vào log trên đĩa"},
        ],
        "steps": [
            _step(1, "producer", "broker", "send(): gửi bản ghi JSON vào topic (acks=all)", "data", "mỗi bản ghi"),
            _step(2, "broker", "producer", "ACK kèm offset sau khi bản ghi đã nằm trong log", "control", "mỗi bản ghi"),
        ],
    },
    "spark-streaming": {
        "id": "spark-streaming",
        "title": "Spark Structured Streaming",
        "components": [
            {"id": "broker", "label": "Kafka broker", "kind": "kafka-broker", "note": "Nguồn: topic hcmc_co_stream"},
            {"id": "spark", "label": "Spark (HCMC_CO_Kafka_Streaming)", "kind": "spark", "note": "local[*], micro-batch; không dùng consumer group"},
            {"id": "hdfs", "label": "HDFS /live", "kind": "storage", "note": "Kết quả Parquet và checkpoint"},
        ],
        "steps": [
            _step(1, "spark", "broker", "Đọc các bản ghi mới của topic theo offset", "data", "mỗi micro-batch"),
            _step(2, "spark", "spark", "Tính co_accumulation_index và mức cảnh báo (500/1000, quyết định D3 tạm)", "data", "mỗi micro-batch"),
            _step(3, "spark", "hdfs", "Ghi file Parquet vào /live/co_stream", "data", "mỗi micro-batch"),
            _step(4, "spark", "hdfs", "Ghi checkpoint: offset đã xử lý (commits/N)", "control", "mỗi micro-batch"),
        ],
    },
}


def _steps(flow_id: str, run_id: str, orders: list[int]) -> list[dict]:
    return [{"flowId": flow_id, "runId": run_id, "step": n, "state": "done"} for n in orders]


def kafka_produce_steps(partition: int, offset: int) -> list[dict]:
    """Bản ghi ở (partition, offset) đã nằm trong log của topic: bước 2 (ACK kèm offset) là quan sát thật, bước 1 (gửi) suy ra từ đó."""
    return _steps("kafka-produce", f"kafka-p{partition}-o{offset}", [KAFKA_STEP_SEND, KAFKA_STEP_ACK])


def spark_batch_steps(batch_id: int) -> list[dict]:
    """Micro-batch đã xong theo Spark REST (job SUCCEEDED): cả 4 bước đều SUY RA từ việc job thành công (REST không lộ từng bước)."""
    return _steps("spark-streaming", f"spark-batch-{batch_id}",
                  [SPARK_STEP_READ, SPARK_STEP_COMPUTE, SPARK_STEP_WRITE, SPARK_STEP_CHECKPOINT])


def summaries() -> list[dict]:
    """Danh sách luồng; kafka-produce và spark-streaming là `available` chỉ khi bật FLOWS_EXTRA."""
    from . import config
    if not config.FLOWS_EXTRA:
        return SUMMARIES
    return [{**s, "status": "available"} if s["id"] in EXTRA_DEFINITIONS else s for s in SUMMARIES]


def definition(flow_id: str) -> dict | None:
    from . import config
    return DEFINITIONS.get(flow_id) or (EXTRA_DEFINITIONS.get(flow_id) if config.FLOWS_EXTRA else None)


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
