# V2 staging

API ingestion lưu SQLite outbox trước khi PUT Bronze và gửi Kafka. Trả 202 sau khi Kafka xác nhận; POST có thể trả 503 trong lúc sự kiện vẫn đang chờ phục hồi. API query đọc Trino riêng, ẩn tombstone.

Spark chỉ ghi vào catalog, schema và checkpoint V2. Một micro-batch có một MERGE, và batch revision bảo vệ replay của chính batch đó. Backfill và thứ tự update giữa partition cần thiết kế riêng.

Phạm vi sandbox staging là một Docker host cho driver, Trino và API với network riêng. Các endpoint VM1 staging phải được cung cấp và provision riêng. Kiến trúc nhiều máy chưa được nghiệm thu.
