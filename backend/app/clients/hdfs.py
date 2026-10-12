"""Client HDFS CHỈ ĐỌC: NameNode JMX và WebHDFS (chỉ GET, chỉ các op đọc).

Không có hàm ghi. Nếu sau này cần ghi, phải đi qua app.writeguard.
"""
import json
from urllib.parse import quote

import httpx

from .. import config

READ_OPS = {"LISTSTATUS", "GETFILESTATUS", "GET_BLOCK_LOCATIONS", "GETFILEBLOCKLOCATIONS", "GETSERVERDEFAULTS", "OPEN"}

_client: httpx.AsyncClient | None = None


def client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=httpx.Timeout(5.0, connect=config.PROBE_TIMEOUT), follow_redirects=True)
    return _client


async def aclose() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


class HdfsUnreachable(Exception):
    pass


class HdfsNotFound(Exception):
    pass


class HdfsError(Exception):
    pass


async def _get(url: str, params: dict) -> httpx.Response:
    try:
        return await client().get(url, params=params)
    except httpx.HTTPError as e:
        raise HdfsUnreachable(f"Không nối được NameNode {config.NAMENODE_HTTP}: {type(e).__name__}") from e


async def jmx(bean: str) -> dict:
    """Trả về bean đầu tiên khớp, ví dụ 'Hadoop:service=NameNode,name=NameNodeInfo'."""
    r = await _get(f"{config.NAMENODE_HTTP}/jmx", {"qry": bean})
    if r.status_code != 200:
        raise HdfsError(f"JMX trả {r.status_code}")
    beans = r.json().get("beans", [])
    if not beans:
        raise HdfsError(f"JMX không có bean {bean}")
    return beans[0]


async def webhdfs(path: str, op: str) -> httpx.Response:
    if op not in READ_OPS:
        raise HdfsError(f"op WebHDFS không được phép (chỉ đọc): {op}")
    r = await _get(f"{config.NAMENODE_HTTP}/webhdfs/v1{quote(path)}", {"op": op})
    if r.status_code == 404:
        raise HdfsNotFound(path)
    if r.status_code != 200:
        raise HdfsError(f"WebHDFS {op} {path} trả {r.status_code}: {r.text[:200]}")
    return r


async def list_status(path: str) -> list[dict]:
    return (await webhdfs(path, "LISTSTATUS")).json()["FileStatuses"]["FileStatus"]


async def file_status(path: str) -> dict:
    return (await webhdfs(path, "GETFILESTATUS")).json()["FileStatus"]


async def block_locations(path: str) -> list[dict]:
    """Danh sách block: mỗi phần tử có block.blockId, block.numBytes, locations[].hostName/xferPort."""
    r = await webhdfs(path, "GET_BLOCK_LOCATIONS")
    return r.json()["LocatedBlocks"]["locatedBlocks"]


async def server_defaults() -> dict:
    return (await webhdfs("/", "GETSERVERDEFAULTS")).json()["FsServerDefaults"]


async def read_text(path: str) -> str:
    return (await webhdfs(path, "OPEN")).text


def parse_json_bean_field(bean: dict, field: str) -> dict:
    """LiveNodes, DeadNodes... trong NameNodeInfo là chuỗi JSON lồng trong JSON."""
    v = bean.get(field) or "{}"
    return json.loads(v) if isinstance(v, str) else v
