# AGENTS.md — luật chung cho mọi agent

Dự án: bảng điều khiển thời gian thực cho đồ án Big Data về CO tại TP.HCM. Người dùng (Trọng) là người quyết định; hai agent làm việc song song, mỗi agent một worktree trong Orca.

## 1. Vai trò và quyền sở hữu

| Agent | Vai trò | Được sửa | Không được sửa |
|---|---|---|---|
| Claude | Backend, lead | `backend/`, `docs/`, `mock/`, `infra/`, gốc repo | `frontend/` |
| Antigravity | Frontend | `frontend/`, phần "Frontend" trong `docs/HANDOFF.md` | mọi thứ còn lại |

Cần đổi thứ mình không sở hữu: ghi yêu cầu vào `docs/HANDOFF.md`, không tự sửa.

## 2. Hợp đồng là nguồn sự thật

- `docs/contract/openapi.yaml` và `docs/contract/events.md` quy định mọi dữ liệu giữa backend và frontend.
- Frontend: sinh kiểu từ openapi, làm với `mock/serve.py` (cổng 8010) trước, đổi sang backend thật (cổng 8000) bằng biến môi trường. Thiếu trường nào thì xin trong HANDOFF, đừng tự bịa trường và đừng suy ra từ dữ liệu khác.
- Backend: mọi route trả đúng schema. Làm xong route thì so với file mock cùng route.
- Trước mỗi commit đụng hợp đồng hoặc mock: `python docs/contract/validate_contract.py` phải báo OK.

## 3. Git và Orca

- Nhánh: `main` (ổn định), `feat/backend`, `feat/frontend`. Mỗi agent chỉ commit vào nhánh của mình.
- Commit nhỏ, thông điệp tiếng Việt hoặc Anh đều được, nói rõ làm gì. Rebase lên `main` trước khi mở PR; PR nhỏ, một việc.
- Chỉ lead gộp vào `main`. Frontend mở PR, lead đọc diff rồi gộp.
- Đầu mỗi phiên: `git fetch`, đọc `docs/HANDOFF.md` và `docs/TASKS.md`. Cuối mỗi phiên: ghi HANDOFF.
- Agent không nhắn trực tiếp cho nhau. Mọi trao đổi đi qua file trong repo, PR, hoặc người dùng.

## 4. Môi trường

- Backend chạy trong WSL Ubuntu cùng Hadoop 3.3.6 (`/home/trong/hadoop`), Kafka (`/home/trong/kafka`), Spark `local[*]`. Địa chỉ và cổng xem `README.md` và `infra/README.md`.
- Cổng: backend 8000, mock 8010, frontend 5173.
- Không có Docker. Không cài thêm dịch vụ nặng nếu chưa hỏi người dùng.

## 5. Quy ước dữ liệu (dễ sai nhất)

- Cột `datetime` trong bộ Kaggle là **UTC**; hiển thị phải cộng 7 giờ (giờ Việt Nam). Hợp đồng luôn dùng ISO 8601 có `+07:00`.
- Đơn vị khí: µg/m³. Dung lượng: byte. JSON: camelCase.
- Có điểm gãy dữ liệu ngày 01/07/2025 (CO giảm mạnh), có ở cả bản Kaggle lẫn bản thô OpenWeather. Biểu đồ không được nối mượt qua điểm này như thể không có gì; ghi chú trên biểu đồ.
- Nhãn "trạm" trong luồng replay chỉ là nhãn gán luân phiên, không phải trạm đo thật (`isSimulatedStation: true`). UI phải ghi chú rõ, không vẽ như dữ liệu theo trạm.
- Luồng live đang dùng Open-Meteo, bộ dữ liệu lịch sử là OpenWeather: khác nguồn. Hiển thị `source` và không trộn vào cùng một đường biểu đồ.
- Ngưỡng `SAFE/WARNING/DANGER` theo thang CO của OpenWeather (quyết định D3, chốt 13/10/2026): `WARNING` từ 4400 µg/m³, `DANGER` từ 12400. Chỉ hiển thị `status` backend trả, không tự tính; không gọi là quy chuẩn của Việt Nam hay "theo WHO" (chỉ có thể nói 4400 xấp xỉ hướng dẫn 24 giờ 4 mg/m³ của WHO 2021).

## 6. Bảo mật

- Không commit khóa API. `own_key.txt`, `.env` đã nằm trong `.gitignore`. Chỉ `.env.example` với giá trị trống được commit.
- Cửa sổ lệnh chạy lệnh thật: tuân theo mục bảo mật trong `docs/contract/events.md` (127.0.0.1, kiểm tra Origin, token).
- Backend mặc định chỉ đọc đối với HDFS, Kafka, Spark.

## 7. Thế nào là xong một việc

1. Chạy được ở máy người dùng, chỉ rõ lệnh chạy.
2. Validator hợp đồng báo OK.
3. Với giao diện: đã xem trên trình duyệt, không lỗi console, thu nhỏ vẫn dùng được.
4. Đã cập nhật `docs/TASKS.md` và `docs/HANDOFF.md`.
5. Nói thật phần nào chưa làm hoặc đang giả lập.

## 8. Đừng

- Đừng bịa số liệu, tên dịch vụ đang chạy, hay "thời gian thực" khi thật ra là phát lại. Dữ liệu giả lập phải có nhãn.
- Đừng sửa file ngoài phần mình sở hữu, đừng di chuyển hay xóa script pipeline cũ của người dùng ở thư mục cha.
- Đừng thêm thư viện nặng mà không ghi lý do vào HANDOFF.
