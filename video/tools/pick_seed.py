"""Đọc một cảnh với nhiều seed, để Whisper chấm thuật ngữ, giữ seed tốt nhất.

    .venv/Scripts/python tools/pick_seed.py v02 s3 s5 s7 --seeds 1 2 3 4

eleven_v3 mỗi lần đọc một khác: cùng câu, lần này đọc "NameNode" đúng, lần sau
lại Việt hóa thành "nêm nốt". Công cụ này đọc từng cảnh với các seed cho trước
(mỗi lần tốn số ký tự của cảnh), đếm số thuật ngữ Whisper chép ra đúng, rồi ghi
seed tốt nhất vào "tts.seeds" của narration.json. Sau đó chạy lại tools/tts.py
(dùng bản đã lưu trong .cache, không tốn thêm ký tự) và tools/check_terms.py.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_terms import load_audio, norm  # noqa: E402
from tts import ROOT, load_env, synth_scene_eleven  # noqa: E402


def term_score(say_text: str, heard: str, terms: list[dict]) -> tuple[int, int, list[str]]:
    """(số lần đúng, số lần cần có, thuật ngữ thiếu) cho cả cảnh."""
    ok = total = 0
    missing = []
    h = norm(heard)
    for t in terms:
        need = norm(say_text).count(norm(t["term"]))
        if not need:
            continue
        forms = [norm(x) for x in [t["term"], *t.get("accept", [])]]
        found = max(h.count(f) for f in forms)
        ok += min(need, found)
        total += need
        if found < need:
            missing.append(f"{t['term']} {found}/{need}")
    return ok, total, missing


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("video")
    ap.add_argument("scenes", nargs="+")
    ap.add_argument("--seeds", nargs="+", type=int, default=[1, 2, 3])
    args = ap.parse_args()

    load_env()
    from faster_whisper import WhisperModel

    path = ROOT / "src" / "videos" / args.video / "narration.json"
    narration = json.loads(path.read_text(encoding="utf-8"))
    cfg = narration["tts"]
    terms = narration.get("terms", [])
    cache = ROOT / ".cache" / "tts"
    model = WhisperModel(os.environ.get("WHISPER_MODEL", "medium"), device="cpu", compute_type="int8")

    for sid in args.scenes:
        scene = next(s for s in narration["scenes"] if s["id"] == sid)
        say = " ".join(s.get("say", s["text"]) for s in scene["sentences"])
        best = None
        for seed in args.seeds:
            trial = dict(cfg, seeds={**cfg.get("seeds", {}), sid: seed})
            mp3, _ = synth_scene_eleven(scene, trial, cache, "", "")
            segs, _ = model.transcribe(load_audio(mp3), language="vi")
            heard = " ".join(s.text for s in segs)
            ok, total, missing = term_score(say, heard, terms)
            print(f"{sid} seed {seed}: {ok}/{total} thuật ngữ đúng" + (f"; thiếu {', '.join(missing)}" if missing else ""))
            if best is None or ok > best[1]:
                best = (seed, ok, total)
        cfg.setdefault("seeds", {})[sid] = best[0]
        print(f"=> {sid}: chọn seed {best[0]} ({best[1]}/{best[2]})")

    path.write_text(json.dumps(narration, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
