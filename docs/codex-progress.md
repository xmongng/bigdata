# Audit và tiến độ V2

Repo thực tế nằm trong hai thư mục lồng cùng tên. Checkout ban đầu thiếu .git. Audit đọc toàn bộ handoff, kiểm kê các file ban đầu, không thấy reparse point trong repo. Các file lớn nhất là tài liệu handoff và source import, không phát hiện dữ liệu runtime trong repo.

## Phát hiện bảo mật

| Mức | Phát hiện | Trạng thái |
| --- | --- | --- |
| Cao | Cấu hình và script mẫu có thể hướng người vận hành về tài nguyên V1 | Đã tách cấu hình staging. Deploy scripts chỉ preview, control V1 chỉ status |
| Cao | Nếu POST ghi Bronze rồi mất Kafka ACK, lần thử lại có thể gây trùng hoặc mất dấu sự kiện | V2 ghi SQLite outbox trước khi gửi, giữ khóa Bronze và event ID khi retry. ACK mơ hồ và nhiều replica vẫn cần nghiệm thu |
| Trung bình | Snapshot chứa IP LAN, tên dịch vụ và topology V1 | Cần người sở hữu duyệt trước khi publish private |
| Trung bình | V1 dùng Kafka một broker và endpoint LAN. V2 chưa nghiệm thu TLS, HA, ACL, restore, giám sát và rollback | Còn mở |
| Thông tin | Quét toàn bộ tập tin bằng heuristic không phát hiện secret. Một cảnh báo ban đầu ở dòng ghi tên biến vào file là false positive | Tiếp tục quét staged diff. Không có lịch sử sẵn để đánh giá |

Không có thao tác viết lên VM, thay đổi hay chạy container, gửi dữ liệu hoặc push Git.

## Roadmap và điều kiện hoàn tất

| Phase | Bằng chứng hiện có | Điều kiện còn lại |
| --- | --- | --- |
| P0 | Đã kiểm kê, mở rộng .gitignore, thêm env example, bộ quét redacted và hướng dẫn private Git | Duyệt staged diff và metadata nội bộ. Commit/push sau xác nhận |
| P1 | SHA-256 file local, host VM2 và container khớp. Đã lưu bản V1 trong reference | Kiểm thử build từ checkout và so sánh tài nguyên runtime trước khi chứng nhận tái lập |
| P2 | Hai API, hợp đồng route và payload cơ bản được test bằng mock. Có giới hạn file, timeout, đóng kết nối và outbox retry | Chạy integration với V2 MinIO, Kafka, Trino và đo 202/GET. Không giả định API V1 hay dashboard hiện tại được thay thế |
| P3 | Tách Spark, gộp MERGE theo batch, bảo vệ replay cùng batch ID và giữ tombstone. Unit test pass | Spark/Iceberg MERGE thật, replay sau crash và chính sách xử lý late events/DLQ; giữ checkpoint V2 tương ứng table |
| P4 | Compose staging và VM2 validate, môi trường riêng và Trino V2 template, deploy preview | Cấp resources staging, xác minh phiên bản và OAuth, phục hồi outbox/checkpoint. Chính sách production vẫn chưa nghiệm thu |
| P5 | CI validation workflow, smoke staging, checklist và runbook | CI từ private remote, E2E staging và rollback có bằng chứng |

## Kiểm thử local

- `node scripts/scan-secrets.mjs` — PASS, không có findings.
- `python -B scripts/test_apis.py` — PASS 3 test với FastAPI TestClient và adapter giả.
- `python -B -m unittest discover -s tests -v` trong `pipelines/spark` — PASS 4 test thuần Python và Spark mock.
- Compile Python không sinh cache — PASS 17 file. Bash syntax và script deploy dry-run — PASS.
- `docker compose config --quiet` cho staging và VM2 với env validation — PASS. Lệnh chỉ kiểm tra cấu hình.
- Docker build/start, Spark SQL thật, tích hợp Trino thật, CI remote, live smoke/E2E và restore test — NOT RUN. Cần tài nguyên staging biệt lập và quyền kiểm thử.

Index local trên branch main chứa baseline để review theo [Git private](security.md). Không tạo remote và không commit trước duyệt.
