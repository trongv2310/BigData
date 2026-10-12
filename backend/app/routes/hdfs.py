"""/api/hdfs/*: NameNode JMX + WebHDFS, chỉ đọc."""
import posixpath

from fastapi import APIRouter, Query

from .. import timeutil
from ..clients import hdfs
from ..errors import ApiError

router = APIRouter()

NN_INFO = "Hadoop:service=NameNode,name=NameNodeInfo"
NN_FS = "Hadoop:service=NameNode,name=FSNamesystem"


def _unreachable(e: Exception) -> ApiError:
    return ApiError(503, "HDFS_UNREACHABLE", f"Không đọc được HDFS: {e}")


def _norm_path(path: str) -> str:
    if not path.startswith("/") or any(p == ".." for p in path.split("/")):
        raise ApiError(400, "BAD_PATH", "path phải là đường dẫn tuyệt đối HDFS, bắt đầu bằng / và không chứa ..")
    n = posixpath.normpath(path)
    return "/" if n in (".", "//") else n


@router.get("/api/hdfs/summary")
async def hdfs_summary():
    try:
        info = await hdfs.jmx(NN_INFO)
        fs = await hdfs.jmx(NN_FS)
        defaults = await hdfs.server_defaults()
    except (hdfs.HdfsUnreachable, hdfs.HdfsError) as e:
        raise _unreachable(e)
    return {
        "clusterId": info["ClusterId"],
        "version": info["Version"].split(",")[0].strip(),
        "safeMode": bool(info.get("Safemode")),
        "capacityTotalBytes": int(fs["CapacityTotal"]),
        "capacityUsedBytes": int(fs["CapacityUsed"]),
        "capacityRemainingBytes": int(fs["CapacityRemaining"]),
        "liveDataNodes": len(hdfs.parse_json_bean_field(info, "LiveNodes")),
        "deadDataNodes": len(hdfs.parse_json_bean_field(info, "DeadNodes")),
        "filesTotal": int(fs["FilesTotal"]),
        "blocksTotal": int(fs["BlocksTotal"]),
        "underReplicatedBlocks": int(fs["UnderReplicatedBlocks"]),
        "missingBlocks": int(fs["MissingBlocks"]),
        "defaultBlockSizeBytes": int(defaults["blockSize"]),
        "defaultReplication": int(defaults["replication"]),
        "updatedAt": timeutil.now_iso(),
    }


def _node_state(admin_state: str) -> str:
    # NameNodeInfo.LiveNodes[].adminState: "In Service", "Decommission In Progress", "Decommissioned", "Entering Maintenance", "In Maintenance"
    return "live" if admin_state == "In Service" else "decommissioning"


@router.get("/api/hdfs/datanodes")
async def hdfs_datanodes():
    try:
        info = await hdfs.jmx(NN_INFO)
        hb = int(await hdfs.conf_value("dfs.heartbeat.interval", "3"))
        recheck_ms = int(await hdfs.conf_value("dfs.namenode.heartbeat.recheck-interval", "300000"))
    except (hdfs.HdfsUnreachable, hdfs.HdfsError) as e:
        raise _unreachable(e)
    nodes = []
    for name, n in hdfs.parse_json_bean_field(info, "LiveNodes").items():
        nodes.append({
            "name": name, "address": n["xferaddr"], "state": _node_state(n.get("adminState", "In Service")),
            "lastContactSeconds": int(n["lastContact"]), "capacityBytes": int(n["capacity"]), "usedBytes": int(n["usedSpace"]),
            "blockCount": int(n["numBlocks"]), "version": n.get("version", ""),
        })
    for name, n in hdfs.parse_json_bean_field(info, "DeadNodes").items():
        # DataNode đã chết: NameNode không còn số liệu dung lượng/block của nó
        nodes.append({
            "name": name, "address": n.get("xferaddr", name), "state": "dead", "lastContactSeconds": int(n.get("lastContact", 0)),
            "capacityBytes": 0, "usedBytes": 0, "blockCount": 0, "version": n.get("version", ""),
        })
    nodes.sort(key=lambda x: x["name"])
    return {"heartbeatIntervalSeconds": hb, "deadAfterSeconds": 2 * recheck_ms // 1000 + 10 * hb, "dataNodes": nodes}


@router.get("/api/hdfs/files")
async def hdfs_files(path: str = Query("/")):
    p = _norm_path(path)
    try:
        raw = await hdfs.list_status(p)
    except hdfs.HdfsNotFound:
        raise ApiError(404, "NOT_FOUND", f"Không có đường dẫn {p} trên HDFS")
    except (hdfs.HdfsUnreachable, hdfs.HdfsError) as e:
        raise _unreachable(e)
    if len(raw) == 1 and raw[0]["pathSuffix"] == "":
        raise ApiError(400, "NOT_A_DIRECTORY", f"{p} là file, không phải thư mục (xem /api/hdfs/blocks)")
    entries = [{
        "name": e["pathSuffix"], "type": "directory" if e["type"] == "DIRECTORY" else "file",
        "lengthBytes": int(e["length"]), "replication": int(e["replication"]), "blockSizeBytes": int(e["blockSize"]),
        "modifiedAt": timeutil.from_ms(e["modificationTime"]), "owner": e["owner"],
    } for e in raw]
    return {"path": p, "entries": entries}


@router.get("/api/hdfs/blocks")
async def hdfs_blocks(path: str = Query(...)):
    p = _norm_path(path)
    try:
        st = await hdfs.file_status(p)
        if st["type"] != "FILE":
            raise ApiError(400, "NOT_A_FILE", f"{p} là thư mục, không có block")
        located = await hdfs.block_locations(p)
    except hdfs.HdfsNotFound:
        raise ApiError(404, "NOT_FOUND", f"Không có file {p} trên HDFS")
    except (hdfs.HdfsUnreachable, hdfs.HdfsError) as e:
        raise _unreachable(e)
    blocks = [{
        "index": i,
        "blockId": f"blk_{b['block']['blockId']}",
        "lengthBytes": int(b["block"]["numBytes"]),
        # trùng khóa của /api/hdfs/datanodes: "<hostName>:<xferPort>"
        "locations": [f"{loc['hostName']}:{loc['xferPort']}" for loc in b["locations"]],
    } for i, b in enumerate(located)]
    return {
        "path": p, "lengthBytes": int(st["length"]), "blockSizeBytes": max(int(st["blockSize"]), 1),
        "replication": max(int(st["replication"]), 1), "blockCount": len(blocks), "blocks": blocks,
    }
