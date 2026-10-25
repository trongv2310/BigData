"""Sinh giọng đọc, manifest thời gian và phụ đề .srt cho một video.

    .venv/Scripts/python tools/tts.py v02

Đọc  src/videos/<id>/narration.json
Ghi  public/audio/<id>/<scene>.mp3   (một file mỗi cảnh)
     src/videos/<id>/manifest.json   (khung hình của từng câu, Remotion đọc)
     out/<video>.srt                 (cùng mốc thời gian với phụ đề trong video)

Hai bộ đọc, chọn bằng trường "engine" trong narration.json:

- "elevenlabs" (mặc định): đọc LIỀN cả cảnh trong một lần gọi để giữ ngữ điệu
  giữa các câu; API trả mốc thời gian từng ký tự, từ đó biết câu nào bắt đầu
  lúc nào. Cần ELEVENLABS_API_KEY trong video/.env (không commit).
- "edge": edge-tts, đọc từng câu rồi ghép với khoảng lặng cố định (cách cũ,
  không cần khóa, nhưng ngữ điệu bị đứt giữa các câu).

Kết quả đọc được lưu ở .cache/tts theo nội dung: sửa một cảnh chỉ đọc lại cảnh
đó (ElevenLabs tính tiền theo số ký tự gửi đi).
"""

from __future__ import annotations

import asyncio
import base64
import glob
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FPS = 30
WIDTH, HEIGHT = 1920, 1080
LEAD_SEC = 0.6  # lặng đầu cảnh, để chữ tiêu đề kịp hiện
GAP_SEC = 0.35  # lặng giữa hai câu (chỉ bộ đọc edge)
TAIL_SEC = 0.9  # lặng cuối cảnh, cho hoạt hình kết thúc
MAX_CUE_CHARS = 105  # tối đa khoảng hai dòng phụ đề
SRT_LINE_CHARS = 52

ELEVEN_URL = "https://api.elevenlabs.io/v1/text-to-speech/{voice}/with-timestamps?output_format=mp3_44100_128"


def load_env() -> None:
    """Đọc video/.env (KEY=giá trị) vào biến môi trường nếu chưa có."""
    env = ROOT / ".env"
    if not env.exists():
        return
    for line in env.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def find_tool(name: str) -> str:
    """ffmpeg/ffprobe đi kèm Remotion, nếu không có thì lấy trong PATH."""
    hits = glob.glob(str(ROOT / "node_modules/@remotion/compositor-*" / f"{name}*"))
    hits = [h for h in hits if Path(h).stem == name]
    if hits:
        return hits[0]
    found = shutil.which(name)
    if not found:
        sys.exit(f"Không tìm thấy {name}. Chạy npm install trong thư mục video/ trước.")
    return found


FFMPEG = find_tool("ffmpeg")
FFPROBE = find_tool("ffprobe")


def duration_of(path: Path) -> float:
    out = subprocess.run(
        [FFPROBE, "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(path)],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    return float(out)


def pad_audio(parts: list[Path], dest: Path, gap: float, tempo: float = 1.0) -> None:
    """Ghép các đoạn: lặng đầu, đoạn, gap, đoạn, …, lặng cuối; mã hóa mp3.
    tempo khác 1 thì tăng/giảm tốc lời đọc (atempo giữ nguyên cao độ), trước khi chèn lặng."""
    args = [FFMPEG, "-y", "-v", "error"]
    for p in parts:
        args += ["-i", str(p)]
    chains, labels = [], []
    speed = f",atempo={tempo}" if abs(tempo - 1.0) > 1e-6 else ""
    for i in range(len(parts)):
        pad = TAIL_SEC if i == len(parts) - 1 else gap
        delay = f",adelay={int(LEAD_SEC * 1000)}" if i == 0 else ""
        chains.append(f"[{i}:a]aresample=44100,aformat=channel_layouts=mono{speed}{delay},apad=pad_dur={pad}[a{i}]")
        labels.append(f"[a{i}]")
    graph = ";".join(chains) + f";{''.join(labels)}concat=n={len(parts)}:v=0:a=1[out]"
    args += ["-filter_complex", graph, "-map", "[out]", "-c:a", "libmp3lame", "-b:a", "128k", str(dest)]
    subprocess.run(args, check=True)


# ---------------------------------------------------------------- ElevenLabs


def eleven_request(text: str, cfg: dict, prev_text: str, next_text: str, seed: int | None = None) -> dict:
    key = os.environ.get("ELEVENLABS_API_KEY")
    if not key:
        sys.exit("Thiếu ELEVENLABS_API_KEY. Tạo file video/.env theo video/.env.example.")
    voice = os.environ.get("ELEVENLABS_VOICE_ID") or cfg.get("voiceId")
    if not voice:
        sys.exit("Chưa chọn giọng: đặt ELEVENLABS_VOICE_ID trong video/.env (xem tools/voices.py).")
    body = {
        "text": text,
        "model_id": cfg.get("model", "eleven_v3"),
        "voice_settings": cfg.get("voiceSettings", {}),
    }
    # eleven_v3 chưa nhận previous_text/next_text (API trả 400); các model khác thì gửi
    # để ngữ điệu nối liền giữa các cảnh.
    if body["model_id"] != "eleven_v3":
        body["previous_text"] = prev_text or None
        body["next_text"] = next_text or None
    if cfg.get("languageCode"):
        body["language_code"] = cfg["languageCode"]
    if seed is not None:
        body["seed"] = seed
    req = urllib.request.Request(
        ELEVEN_URL.format(voice=voice),
        data=json.dumps(body).encode("utf-8"),
        headers={"xi-api-key": key, "Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=180) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", "replace")[:400]
            if e.code in (429, 500, 502, 503) and attempt < 2:
                continue
            sys.exit(f"ElevenLabs trả lỗi {e.code}: {detail}")
        except urllib.error.URLError as e:
            if attempt == 2:
                sys.exit(f"Không gọi được ElevenLabs: {e}")
    raise RuntimeError("không tới được đây")


def synth_scene_eleven(scene: dict, cfg: dict, cache: Path, prev_text: str, next_text: str) -> tuple[Path, list[tuple[float, float]]]:
    """Đọc cả cảnh một lần. Trả về file mp3 thô và (bắt đầu, kết thúc) của từng câu, tính bằng giây."""
    says = [s.get("say", s["text"]).strip() for s in scene["sentences"]]
    text = " ".join(says)
    voice = os.environ.get("ELEVENLABS_VOICE_ID") or cfg.get("voiceId", "")
    # "seeds" trong narration.json: đổi số của một cảnh để model đọc lại theo biến thể khác
    # (eleven_v3 mỗi lần đọc một khác; dùng khi một cảnh bị đọc sai thuật ngữ).
    seed = cfg.get("seeds", {}).get(scene["id"])
    key_parts = [voice, cfg.get("model"), cfg.get("voiceSettings"), text] + ([seed] if seed is not None else [])
    key = hashlib.sha1(json.dumps(key_parts, ensure_ascii=False).encode("utf-8")).hexdigest()[:16]
    mp3 = cache / f"el_{key}.mp3"
    meta = cache / f"el_{key}.json"
    if not (mp3.exists() and meta.exists()):
        res = eleven_request(text, cfg, prev_text, next_text, seed)
        mp3.write_bytes(base64.b64decode(res["audio_base64"]))
        meta.write_text(json.dumps(res["alignment"], ensure_ascii=False), encoding="utf-8")
        print(f"  đã đọc {scene['id']} ({len(text)} ký tự)")
    al = json.loads(meta.read_text(encoding="utf-8"))
    starts, ends = al["character_start_times_seconds"], al["character_end_times_seconds"]
    chars = al["characters"]
    if "".join(chars) != text:
        # API có thể chuẩn hóa vài ký tự; khi đó căn theo tỉ lệ độ dài để không lệch câu.
        print(f"  cảnh báo: mốc ký tự của {scene['id']} không khớp nguyên văn, căn theo tỉ lệ")
    n = len(starts)
    times, pos = [], 0
    for s in says:
        a = round(pos * (n - 1) / max(1, len(text) - 1)) if "".join(chars) != text else pos
        b_idx = pos + len(s) - 1
        b = round(b_idx * (n - 1) / max(1, len(text) - 1)) if "".join(chars) != text else b_idx
        times.append((starts[min(a, n - 1)], ends[min(b, n - 1)]))
        pos += len(s) + 1
    return mp3, times


# ---------------------------------------------------------------- edge-tts


async def _edge_one(text: str, voice: str, rate: str, dest: Path, sem: asyncio.Semaphore) -> None:
    import edge_tts

    if dest.exists() and dest.stat().st_size > 0:
        return
    async with sem:
        tmp = dest.with_suffix(".part")
        for attempt in range(4):  # dịch vụ thỉnh thoảng không trả audio: thử lại
            try:
                await edge_tts.Communicate(text, voice, rate=rate).save(str(tmp))
                tmp.replace(dest)
                return
            except Exception:
                if attempt == 3:
                    raise
                await asyncio.sleep(2 * (attempt + 1))


def synth_scene_edge(scene: dict, cfg: dict, cache: Path) -> tuple[list[Path], list[tuple[float, float]]]:
    voice, rate = cfg.get("voice", "vi-VN-HoaiMyNeural"), cfg.get("rate", "+0%")
    files = []
    for s in scene["sentences"]:
        say = s.get("say", s["text"])
        k = hashlib.sha1(f"{voice}|{rate}|{say}".encode("utf-8")).hexdigest()[:16]
        files.append((say, cache / f"{k}.mp3"))
    sem = asyncio.Semaphore(4)

    async def run() -> None:
        await asyncio.gather(*[_edge_one(t, voice, rate, f, sem) for t, f in files])

    asyncio.run(run())
    times, t = [], 0.0
    for i, (_, f) in enumerate(files):
        d = duration_of(f)
        times.append((t, t + d))
        t += d + (GAP_SEC if i < len(files) - 1 else 0)
    return [f for _, f in files], times


# ---------------------------------------------------------------- phụ đề


def split_cue(text: str) -> list[str]:
    """Chia câu dài thành các đoạn phụ đề ngắn, ưu tiên cắt ở dấu câu."""
    if len(text) <= MAX_CUE_CHARS:
        return [text]
    parts = re.split(r"(?<=[;:,])\s+", text)
    chunks: list[str] = []
    cur = ""
    for p in parts:
        if cur and len(cur) + 1 + len(p) > MAX_CUE_CHARS:
            chunks.append(cur)
            cur = p
        else:
            cur = f"{cur} {p}".strip()
    if cur:
        chunks.append(cur)
    out: list[str] = []
    for c in chunks:
        while len(c) > MAX_CUE_CHARS:
            cut = c.rfind(" ", 0, MAX_CUE_CHARS)
            cut = cut if cut > 0 else MAX_CUE_CHARS
            out.append(c[:cut])
            c = c[cut:].strip()
        out.append(c)
    return out


def wrap_srt(text: str) -> str:
    words, lines, cur = text.split(), [], ""
    for w in words:
        if cur and len(cur) + 1 + len(w) > SRT_LINE_CHARS:
            lines.append(cur)
            cur = w
        else:
            cur = f"{cur} {w}".strip()
    lines.append(cur)
    return "\n".join(lines)


def srt_time(frame: int) -> str:
    ms = round(frame * 1000 / FPS)
    h, ms = divmod(ms, 3_600_000)
    m, ms = divmod(ms, 60_000)
    s, ms = divmod(ms, 1000)
    return f"{h:02}:{m:02}:{s:02},{ms:03}"


def main(vid: str) -> None:
    load_env()
    src_dir = ROOT / "src" / "videos" / vid
    narration = json.loads((src_dir / "narration.json").read_text(encoding="utf-8"))
    cfg = narration.get("tts", {})
    # TTS_ENGINE=edge chạy thử miễn phí mà không sửa narration.json.
    engine = os.environ.get("TTS_ENGINE") or cfg.get("engine", "elevenlabs")
    cache = ROOT / ".cache" / "tts"
    cache.mkdir(parents=True, exist_ok=True)
    audio_dir = ROOT / "public" / "audio" / vid
    audio_dir.mkdir(parents=True, exist_ok=True)

    scenes = narration["scenes"]
    scenes_out, cues = [], []
    offset = 0
    for idx, scene in enumerate(scenes):
        dest = audio_dir / f"{scene['id']}.mp3"
        if engine == "elevenlabs":
            # Câu cuối cảnh trước và câu đầu cảnh sau giúp ngữ điệu nối liền giữa các cảnh.
            prev_t = scenes[idx - 1]["sentences"][-1].get("say", scenes[idx - 1]["sentences"][-1]["text"]) if idx else ""
            next_t = scenes[idx + 1]["sentences"][0].get("say", scenes[idx + 1]["sentences"][0]["text"]) if idx + 1 < len(scenes) else ""
            raw, times = synth_scene_eleven(scene, cfg, cache, prev_t, next_t)
            # Tăng tốc bằng ffmpeg không tốn ký tự; mốc thời gian co lại theo đúng tỉ lệ.
            tempo = float(cfg.get("tempo", 1.0))
            pad_audio([raw], dest, 0, tempo)
            times = [(a / tempo, b / tempo) for a, b in times]
        else:
            parts, times = synth_scene_edge(scene, cfg, cache)
            pad_audio(parts, dest, GAP_SEC)

        sentences = [
            {"start": round((LEAD_SEC + a) * FPS), "end": round((LEAD_SEC + b) * FPS), "text": s["text"]}
            for s, (a, b) in zip(scene["sentences"], times)
        ]
        length = math.ceil(duration_of(dest) * FPS)

        # Phụ đề: giữ chữ tới đầu câu sau để không nhấp nháy.
        for i, s in enumerate(sentences):
            hold = sentences[i + 1]["start"] if i + 1 < len(sentences) else min(length, s["end"] + round(TAIL_SEC * FPS * 0.6))
            chunks = split_cue(s["text"])
            total = sum(len(c) for c in chunks)
            cs = s["start"]
            for j, c in enumerate(chunks):
                ce = hold if j == len(chunks) - 1 else cs + round((s["end"] - s["start"]) * len(c) / total)
                cues.append({"start": offset + cs, "end": offset + ce, "text": c})
                cs = ce

        scenes_out.append(
            {
                "id": scene["id"],
                "title": scene["title"],
                "from": offset,
                "durationInFrames": length,
                "audio": f"audio/{vid}/{scene['id']}.mp3",
                "sentences": sentences,
            }
        )
        print(f"{scene['id']}: {len(sentences)} câu, {length / FPS:.1f} s")
        offset += length

    manifest = {
        "video": narration["video"],
        "fps": FPS,
        "width": WIDTH,
        "height": HEIGHT,
        "totalFrames": offset,
        "scenes": scenes_out,
        "cues": cues,
    }
    (src_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    blocks = [f"{n}\n{srt_time(c['start'])} --> {srt_time(c['end'])}\n{wrap_srt(c['text'])}\n" for n, c in enumerate(cues, 1)]
    out_dir = ROOT / "out"
    out_dir.mkdir(exist_ok=True)
    (out_dir / f"{narration['video']}.srt").write_text("\n".join(blocks), encoding="utf-8")
    print(f"Tổng: {offset / FPS:.1f} s ({offset} khung hình), {len(cues)} dòng phụ đề, bộ đọc {engine}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Cách dùng: python tools/tts.py v02")
    main(sys.argv[1])
