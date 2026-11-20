# V2 staging và vận hành

| Tài nguyên | V1 | V2 staging |
| --- | --- | --- |
| Compose project | vnnews và vn-news-spark | vnnews-v2-staging, vnnews-v2-spark |
| Network | Mạng V1 | vnnews-v2-staging trên một Docker host |
| Cổng | FastAPI 8000, Trino 8080 | 18000 ingestion, 18001 query, 18080 Trino, bind loopback |
| Kafka, Bronze, Polaris | Topic vn-news-events, bucket vn-news-lake, catalog news_lake | Topic, bucket và catalog riêng, hậu tố -v2 hoặc _v2 |
| Table | lake.news_db.articles | lake_v2.news_db_v2.articles |
| State | Checkpoint V1 | Hai volume riêng cho outbox và checkpoint |

## Điều kiện trước khi chạy staging

1. Dùng broker/bucket/Polaris cấp riêng cho staging. Xác minh endpoint và quyền của credential staging. Chuẩn bị topic, warehouse, catalog và Spark DDL trong `sql/silver/articles-v2.sql`. Không chạy Polaris provisioning snapshot V1.
2. Tạo env riêng từ mẫu, điền `TRINO_HOST=trino-v2` và OAuth/S3 của staging. Chọn bản Trino đã duyệt và cung cấp image Spark 3.5.7 cùng bộ JAR offline. Giữ credential ở ngoài Git.
3. Chạy `bash scripts/check-local.sh`, xem Compose config và hai bản preview deploy. Khi đã duyệt staging mới build image và khởi động các project riêng, đảm bảo Spark driver nối được network. `compose.production.yaml` vẫn dành cho giai đoạn sau.
4. Ghi commit SHA và image ID. Chạy `python scripts/smoke_staging.py` để kiểm tra đọc. Gắn `--write-staging` sau khi được phép ghi dữ liệu giả và quan sát batch, snapshot, GET. Script giữ lại bản ghi thử, nên audit theo ID nó in ra.
5. Nếu Kafka chưa xác nhận, xem event ID và retry từ cùng volume outbox bằng `python -m src.adapters` trong môi trường ingestion V2. Tránh POST lại payload trong lúc chưa đối soát sự kiện cũ. Retry có thể trùng nếu Kafka đã nhận nhưng ACK bị mất.
6. Rollback bằng dừng riêng tiến trình V2 và khôi phục image/config đã duyệt. Với cùng V2 table, phải giữ checkpoint và so độ tương thích code/schema trước khi bật lại. Không khởi động một driver khác cùng checkpoint.

Checklist nghiệm thu: kiểm tra secret và diff, CI, smoke staging, upload/CRUD, khả năng retry/đọc lại, kiểm tra cổng và RAM, restore outbox/checkpoint sau sự cố, và thử rollback. Không chuyển traffic V1 trước khi có bằng chứng kiểm thử.
