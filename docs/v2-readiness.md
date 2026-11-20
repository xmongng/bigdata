# Rà soát V2 và kế hoạch nghiệm thu

Baseline local `main` tại `3dc0429`. Hai tài liệu đầu vào là README và handoff. Các mô tả scaffold trong handoff ghi lại thời điểm nhập baseline.

| Thành phần | Hiện có | Còn thiếu bằng chứng |
| --- | --- | --- |
| Spark Standalone VM2 | Inventory ghi Master/Worker/driver client và checksum của script V1. SHA-256 của bản reference tại checkout khớp inventory. V2 có Compose, runner, Spark 3.5.7 và job tách module. | Trạng thái VM2 hiện tại chưa xác minh lại. Cần build từ checkout bằng base image và JAR đã kiểm kê, chạy worker và kiểm thử restart với checkpoint V2. |
| FastAPI | Ingestion có upload, CRUD và SQLite outbox. Query có GET, kết nối Trino, timeout và lọc tombstone. | Cần đối chiếu hợp đồng với bản Windows đang chạy và thử lỗi MinIO/Kafka/Trino thật. |
| Kafka và MinIO | API ghi Bronze trước Kafka, Spark đọc topic và lấy JSON. Compose staging API không cấp Kafka/MinIO riêng. | Cần cấp topic/bucket V2, kiểm thử listener từ VM2, quyền, retry, mất ACK và ordering giữa upload và CRUD. |
| Polaris và PostgreSQL | Provisioning và schema init trong repo là snapshot V1. Spark và Trino V2 có cấu hình REST/OAuth. | Thiếu quy trình bootstrap V2 độc lập có thể lặp lại, kiểm thử OAuth/quyền và phục hồi metadata. |
| Iceberg | Có DDL Silver, MERGE theo micro-batch, tombstone và guard batch revision. | Cần kiểm thử MERGE thật, replay sau commit, sự kiện đến muộn, poison message và DLQ. Batch ID chỉ có ý nghĩa trong vòng đời checkpoint tương ứng. |
| Trino | Compose staging, catalog REST V2 và adapter truy vấn. | Cần chốt image staging và xác minh OAuth, đọc Iceberg qua MinIO và thấy dữ liệu mới sau commit. |
| Gold, production, CI | SQL Gold minh họa, CI validation và deploy preview. | Gold materialization, cấu hình production, bằng chứng CI remote, E2E, rollback và restore còn thiếu. |

## Thứ tự hoàn thiện

1. **P0 Git và security.** Quét working tree và index, duyệt metadata nội bộ, xác minh lịch sử và quyền private trước khi publish. Heuristic có thể bỏ sót bí mật.
2. **P1 VM2 Standalone.** Đối chiếu script, image, JAR và mount chỉ đọc theo runtime inventory. Tiêu chí là khớp dấu vết runtime và build lại được từ nguồn đã chốt.
3. **P2 FastAPI.** Chốt hợp đồng Windows, kiểm thử upload/CRUD, lỗi từng adapter và retry outbox với cùng event ID. Tiêu chí là API trả 202 sau khi giao được sự kiện và truy vấn theo ID hoạt động trên Trino.
4. **P3 Spark và Iceberg.** Chạy trên tài nguyên V2 riêng, kiểm chứng MERGE, tombstone và replay khi driver lỗi sau commit. Chốt ordering theo nghiệp vụ, xử lý poison message và điều kiện recovery.
5. **P4 Hạ tầng và Trino.** Cấp topic, bucket, catalog, warehouse và checkpoint riêng. Kiểm chứng quyền OAuth/S3 và khả năng kết nối giữa host. Docker network cùng tên trên hai host chưa thiết lập kết nối liên host.
6. **P5 Nghiệm thu.** Chạy CI và smoke E2E với một bài tổng hợp, sau đó thử rollback/restore. Cần kết quả quan sát được ở cả POST 202 và GET qua Trino. Production cần đánh giá TLS, ACL, HA, backup và monitoring riêng.

Smoke ghi một bài JSON tổng hợp từ `/upload-files` và poll `/articles?article_id=…&limit=1`. Chế độ mặc định gọi các endpoint đọc. Chỉ chạy `python -B scripts/smoke_staging.py --write-staging` sau khi xác nhận hai cổng staging, tài nguyên cách ly và quyền ghi. Smoke để lại bài tổng hợp phục vụ đối chiếu.
