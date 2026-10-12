# CO HCMC Dashboard

Bảng điều khiển thời gian thực cho đồ án Big Data "Phân tích và dự đoán độ ô nhiễm khí CO tại TP.HCM 4/2022 – 3/2026": xem dữ liệu CO, xem trực tiếp luồng hoạt động của NameNode, DataNode, Kafka, Spark, SparkSQL, và có cửa sổ dòng lệnh nhúng.

Hai agent làm chung qua Orca: **Claude** là backend và lead, **Antigravity** là frontend. Hai bên chỉ gặp nhau ở `docs/contract/`.

## Chạy thử không cần Hadoop

```bash
pip install pyyaml jsonschema
python docs/contract/validate_contract.py     # kiểm tra hợp đồng
python mock/serve.py                          # mock API tại http://127.0.0.1:8010
curl http://127.0.0.1:8010/api/services
```

## Cổng

| Thành phần | Cổng |
|---|---|
| Backend thật (trong WSL) | 8000 |
| Mock server | 8010 |
| Frontend (Vite) | 5173 |
| NameNode UI / RPC | 9870 / 9000 |
| DataNode | 9864 |
| YARN ResourceManager | 8088 |
| Kafka | 9092 |

## Cấu trúc

```
AGENTS.md            luật chung cho mọi agent (đọc đầu tiên)
CLAUDE.md            phần riêng của Claude (lead)
docs/contract/       hợp đồng: openapi.yaml, events.md, validate_contract.py
docs/TASKS.md        giai đoạn, người phụ trách, tiêu chí xong, quyết định chờ
docs/HANDOFF.md      sổ bàn giao giữa hai agent
docs/PROMPTS.md      câu lệnh để dán vào từng agent
mock/                dữ liệu mẫu đúng hợp đồng + serve.py
backend/             của Claude
frontend/            của Antigravity
infra/               ghi chú môi trường WSL (Hadoop, Kafka, Spark, Airflow)
```

Bắt đầu: đọc `docs/PROMPTS.md`.
