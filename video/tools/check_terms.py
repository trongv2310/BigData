"""Nghe lại giọng đọc bằng Whisper để tìm thuật ngữ kỹ thuật bị đọc sai.

    .venv/Scripts/python tools/check_terms.py v02

Với mỗi câu có thuật ngữ (danh sách "terms" trong narration.json), Whisper chép
lại đoạn audio của câu đó. Nếu bản chép không có thuật ngữ viết đúng chính tả
tiếng Anh (bỏ qua hoa thường, khoảng trắng, gạch nối), câu đó bị đánh dấu.
Đây là phép thử gần đúng: Whisper có thể nghe nhầm cả khi giọng đọc đúng, nên
danh sách là "cần người nghe lại", không phải kết luận. Báo cáo ghi ở
out/<video>_term_check.md.

Lần đầu chạy tải mô hình Whisper từ Hugging Face (medium khoảng 1,5 GB).
Đổi mô hình bằng biến WHISPER_MODEL (small, medium, large-v3).
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tts import FFMPEG, FPS, ROOT  # noqa: E402


def load_audio(path: Path):
    """Giải mã bằng ffmpeg sang mono 16 kHz float32 (tránh lỗi PyAV của faster-whisper).
    ffmpeg của Remotion không có muxer s16le nên đi qua file wav tạm."""
    import subprocess
    import wave

    import numpy as np

    tmp = ROOT / ".cache" / "whisper_in.wav"
    subprocess.run(
        [FFMPEG, "-v", "error", "-y", "-i", str(path), "-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le", str(tmp)],
        check=True,
    )
    with wave.open(str(tmp), "rb") as w:
        raw = w.readframes(w.getnframes())
    return np.frombuffer(raw, np.int16).astype(np.float32) / 32768.0


def norm(s: str) -> str:
    return re.sub(r"[\s\-_.·]+", "", s.lower())


def main(vid: str) -> None:
    from faster_whisper import WhisperModel

    src = ROOT / "src" / "videos" / vid
    narration = json.loads((src / "narration.json").read_text(encoding="utf-8"))
    manifest = json.loads((src / "manifest.json").read_text(encoding="utf-8"))
    terms: list[dict] = narration.get("terms", [])
    model = WhisperModel(os.environ.get("WHISPER_MODEL", "medium"), device="cpu", compute_type="int8")

    rows, checked = [], 0
    for scene_n, scene_m in zip(narration["scenes"], manifest["scenes"]):
        audio = ROOT / "public" / scene_m["audio"]
        segments, _ = model.transcribe(load_audio(audio), language="vi", word_timestamps=True, vad_filter=False)
        words = [(w.start, w.end, w.word) for seg in segments for w in (seg.words or [])]
        for i, (sent, timing) in enumerate(zip(scene_n["sentences"], scene_m["sentences"])):
            say = sent.get("say", sent["text"])
            want = [t for t in terms if norm(t["term"]) in norm(say)]
            if not want:
                continue
            a, b = timing["start"] / FPS - 0.3, timing["end"] / FPS + 0.3
            heard = "".join(w for s, e, w in words if e >= a and s <= b).strip()
            for t in want:
                checked += 1
                ok = any(norm(x) in norm(heard) for x in [t["term"], *t.get("accept", [])])
                if not ok:
                    rows.append((scene_m["id"], i, scene_m["from"] / FPS + timing["start"] / FPS, t["term"], heard))
        print(f"{scene_m['id']}: xong")

    out = ROOT / "out" / f"{narration['video']}_term_check.md"
    lines = [
        f"# Kiểm thuật ngữ bằng Whisper ({os.environ.get('WHISPER_MODEL', 'medium')})",
        "",
        f"Đã kiểm {checked} lần xuất hiện thuật ngữ, {len(rows)} lần Whisper không chép ra đúng chữ.",
        "Đây là danh sách cần người nghe lại, không phải kết luận chắc chắn.",
        "",
        "| Cảnh | Câu | Mốc | Thuật ngữ | Whisper nghe được |",
        "|---|---|---|---|---|",
    ]
    for sid, i, t, term, heard in rows:
        m, s = divmod(int(t), 60)
        lines.append(f"| {sid} | {i + 1} | {m:02}:{s:02} | {term} | {heard.replace('|', '/')} |")
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"{len(rows)}/{checked} cần nghe lại. Báo cáo: {out}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Cách dùng: python tools/check_terms.py v02")
    main(sys.argv[1])
