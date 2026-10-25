# video/ — loạt video hướng dẫn Big Data

Video hoạt hình tiếng Việt dựng bằng Remotion (React + TypeScript), 1920×1080, 30 fps.
Kịch bản ở `../storyboard/`; quy ước màu, số, câu không được nói sai ở `../storyboard/00_quy_uoc.md`.
Hiện mới có video 02 (HDFS), làm thử.

## Chạy (Windows, PowerShell hoặc Git Bash, trong thư mục `video/`)

```bash
npm install
python -m venv .venv
.venv/Scripts/python -m pip install edge-tts faster-whisper
```

Giọng đọc dùng ElevenLabs, model `eleven_v3` (đọc được thuật ngữ tiếng Anh trong câu tiếng Việt). Không dùng `eleven_multilingual_v2`: model này không có tiếng Việt, đọc ra giọng lơ lớ. Giọng trong thư viện cần gói trả phí (Starter trở lên).

1. Chép `.env.example` thành `.env`, điền `ELEVENLABS_API_KEY` (file `.env` không commit).
2. Chọn giọng nữ tiếng Việt: `.venv/Scripts/python tools/voices.py list`, nghe các link `preview`,
   thêm giọng vào tài khoản bằng `tools/voices.py add <owner> <voice_id> <tên>`, hoặc đọc mẫu
   đoạn nhiều thuật ngữ bằng `tools/voices.py sample <voice_id> …` (lưu ở `out/voice_samples/`).
   Ghi `ELEVENLABS_VOICE_ID` vào `.env`.
3. Đọc, kiểm, render:

```bash
.venv/Scripts/python tools/tts.py v02
.venv/Scripts/python tools/check_terms.py v02
npx remotion render Video02Hdfs out/02_hdfs.mp4
```

- `tools/tts.py v02` đọc LIỀN từng cảnh (giữ ngữ điệu giữa các câu), lấy mốc thời gian từng ký tự
  để biết câu nào bắt đầu lúc nào, rồi sinh `public/audio/v02/s1.mp3 … s8.mp3`, `src/videos/v02/manifest.json`
  và `out/02_hdfs.srt`. Cảnh đã đọc được lưu ở `.cache/tts`; sửa một cảnh chỉ đọc lại cảnh đó
  (video 02 khoảng 5 000 ký tự mỗi lần đọc cả bài). `TTS_ENGINE=edge` chạy thử bằng edge-tts miễn phí.
- `tools/check_terms.py v02` cho Whisper nghe lại, liệt kê câu mà thuật ngữ (danh sách `terms` trong
  `narration.json`) không được chép ra đúng chữ: `out/02_hdfs_term_check.md`. Đây là danh sách cần nghe lại,
  không phải kết luận. Lần đầu tải mô hình Whisper (medium khoảng 1,5 GB).
- Thời lượng mỗi cảnh = độ dài audio (cộng 0,6 s lặng đầu, 0,9 s cuối). Hoạt hình bám theo câu bằng
  `useCues()`: `cue(i)` là khung hình câu i bắt đầu.
- Xem và chỉnh trực tiếp: `npx remotion studio`. Một khung hình: `npx remotion still Video02Hdfs out/thu.png --frame=600`.
- Render cần mạng lần đầu (tải Chrome headless và phông Be Vietnam Pro từ Google Fonts).

## Sửa lời đọc

Trong `narration.json`, mỗi câu có `text` (chữ hiện trên phụ đề) và `say` nếu cần (chữ đưa cho giọng đọc).
Giữ nguyên thuật ngữ tiếng Anh (NameNode, checksum, replication); chỉ viết lại số dài thành chữ, cổng thành
từng chữ số ("chín tám bảy không"), đường dẫn và ký hiệu thành lời. Muốn ngắt nghỉ thì thêm dấu phẩy hoặc dấu chấm
trong `say`. Thuật ngữ nào vẫn đọc sai thì viết lại cách đọc trong `say` của câu đó rồi chạy lại `tools/tts.py`.

## Cấu trúc

| Đường dẫn | Nội dung |
|---|---|
| `src/theme/palette.ts` | Bảng màu cố định: HDFS xanh dương, Kafka xanh lá, Spark cam, MLlib tím, Airflow xanh ngọc |
| `src/theme/fonts.ts` | Be Vietnam Pro (vietnamese + latin-ext + latin), JetBrains Mono cho lệnh và đường dẫn |
| `src/theme/format.ts` | Định dạng số kiểu Việt Nam (3 179 004; 38,91). Đổi dấu ngăn nghìn ở đây |
| `src/theme/anim.ts` | `progress`, `appear`, `pop`, `blink` |
| `src/components/` | Dùng chung cho mọi video: `Node`, `Arrow`, `Packet`, `Block`, `Counter`, `Callout`, `Ledger` (sổ ghi), `Terminal`, `SceneShell`, `Subtitles`, `useCues` |
| `src/videos/v02/` | Video HDFS: `narration.json`, `manifest.json` (sinh tự động), `layout.ts`, `scenes/Scene1…8` |
| `tools/tts.py` | Giọng đọc (ElevenLabs hoặc edge-tts), manifest, phụ đề |
| `tools/voices.py` | Tìm, thêm, đọc mẫu giọng ElevenLabs |
| `tools/check_terms.py` | Whisper nghe lại, đánh dấu thuật ngữ có thể bị đọc sai |

Phông mono không có dấu tiếng Việt: `Node`, `Block`, `Ledger` tự chuyển sang Be Vietnam Pro khi chuỗi có ký tự ngoài ASCII.

## Thêm video mới

1. Tạo `src/videos/vNN/narration.json` theo mẫu v02.
2. `.venv/Scripts/python tools/tts.py vNN`.
3. Viết `scenes/`, `VideoNN.tsx`, khai báo `Composition` trong `src/Root.tsx`.
