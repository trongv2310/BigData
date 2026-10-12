#!/usr/bin/env python3
"""In các tin của /ws/stream ra màn hình (để thử khi chưa có giao diện).

  python3 backend/tools/watch_stream.py [ws://127.0.0.1:8000/ws/stream]
Ở cửa sổ khác trong WSL: hdfs dfs -put -f somefile.json /stream_input/   rồi xem hdfs.op và flow.step hiện ra. Ctrl+C để dừng.
"""
import asyncio
import json
import sys

import websockets

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


async def main(url):
    async with websockets.connect(url) as ws:
        async for m in ws:
            e = json.loads(m)
            print(e["ts"][11:19], f"#{e['seq']}", e["type"], json.dumps(e["data"], ensure_ascii=False)[:150], flush=True)


try:
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "ws://127.0.0.1:8000/ws/stream"))
except KeyboardInterrupt:
    pass
