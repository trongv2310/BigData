"""Tìm và nghe thử giọng nữ tiếng Việt trên ElevenLabs.

    .venv/Scripts/python tools/voices.py list            # giọng trong thư viện chung, tiếng Việt, nữ
    .venv/Scripts/python tools/voices.py mine            # giọng đã có trong tài khoản
    .venv/Scripts/python tools/voices.py add <public_owner_id> <voice_id> <tên>
    .venv/Scripts/python tools/voices.py sample <voice_id> [<voice_id> …]

"sample" đọc một đoạn khoảng 20 giây của video 02 (nhiều thuật ngữ) bằng từng
giọng, lưu ở out/voice_samples/ để nghe và chọn. Mỗi mẫu tốn khoảng 330 ký tự
trong hạn mức ElevenLabs. Chọn xong, ghi ELEVENLABS_VOICE_ID vào video/.env.
Giọng trong thư viện chung thường phải "add" vào tài khoản trước khi dùng qua API.
"""

from __future__ import annotations

import base64
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tts import ROOT, load_env  # noqa: E402

API = "https://api.elevenlabs.io"
SAMPLE = (
    "HDFS cắt file thành những mẩu cố định, gọi là block. Mặc định, mỗi block tối đa một trăm hai mươi tám mebibyte. "
    "NameNode giữ sổ: block nào nằm ở DataNode nào. Mỗi DataNode gửi một nhịp tim, ba giây một lần. "
    "Client gửi dữ liệu thành từng gói tới DataNode thứ nhất, rồi checksum được kiểm khi đọc lại. "
    "Với replication một, đường ống chỉ có một DataNode."
)


def call(method: str, path: str, body: dict | None = None) -> dict:
    key = os.environ.get("ELEVENLABS_API_KEY")
    if not key:
        sys.exit("Thiếu ELEVENLABS_API_KEY trong video/.env")
    req = urllib.request.Request(
        API + path,
        data=json.dumps(body).encode("utf-8") if body is not None else None,
        headers={"xi-api-key": key, "Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "replace")
        try:
            msg = json.loads(detail)["detail"]["message"]
        except (ValueError, KeyError, TypeError):
            msg = detail[:300]
        sys.exit(f"ElevenLabs trả lỗi {e.code} ở {path.split('?')[0]}: {msg}")


def main() -> None:
    load_env()
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    cmd = sys.argv[1]
    if cmd == "list":
        q = urllib.parse.urlencode({"language": "vi", "gender": "female", "page_size": 30, "sort": "usage_character_count_1y"})
        for v in call("GET", f"/v1/shared-voices?{q}").get("voices", []):
            print(f"{v['voice_id']}  owner={v.get('public_owner_id')}  {v.get('name')}  | {v.get('accent')} | {v.get('age')} | {v.get('use_case')}")
            if v.get("preview_url"):
                print(f"    nghe thử: {v['preview_url']}")
    elif cmd == "mine":
        for v in call("GET", "/v1/voices").get("voices", []):
            labels = v.get("labels") or {}
            print(f"{v['voice_id']}  {v.get('name')}  | {labels.get('language', '')} {labels.get('gender', '')} {labels.get('accent', '')}")
    elif cmd == "add" and len(sys.argv) == 5:
        res = call("POST", f"/v1/voices/add/{sys.argv[2]}/{sys.argv[3]}", {"new_name": sys.argv[4]})
        print(f"Đã thêm, voice_id dùng được: {res.get('voice_id')}")
    elif cmd == "sample" and len(sys.argv) >= 3:
        # Model đổi bằng biến ELEVENLABS_MODEL. eleven_multilingual_v2 KHÔNG có tiếng Việt;
        # tiếng Việt có ở eleven_turbo_v2_5, eleven_flash_v2_5, eleven_v3.
        model = os.environ.get("ELEVENLABS_MODEL", "eleven_turbo_v2_5")
        out = ROOT / "out" / "voice_samples"
        out.mkdir(parents=True, exist_ok=True)
        for vid in sys.argv[2:]:
            body = {
                "text": SAMPLE,
                "model_id": model,
                "voice_settings": {"stability": 0.5, "similarity_boost": 0.8, "style": 0.2, "use_speaker_boost": True},
            }
            if model != "eleven_v3":
                body["language_code"] = "vi"
            res = call("POST", f"/v1/text-to-speech/{vid}/with-timestamps?output_format=mp3_44100_128", body)
            path = out / f"{vid}_{model}.mp3"
            path.write_bytes(base64.b64decode(res["audio_base64"]))
            print(f"Đã lưu {path}")
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
