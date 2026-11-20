# V1 đang vận hành

Windows FastAPI gửi JSON Bronze sang MinIO và sự kiện sang Kafka trên VM1. Spark Docker Standalone trên VM2 đọc Kafka, tải object Bronze, ghi Iceberg qua Polaris và MinIO. Polaris dùng PostgreSQL. GET đi từ Windows FastAPI qua Trino trên MacBook.

Chi tiết nguồn và checksum trong [inventory](runtime-inventory.md). Snapshot VM1 và Spark V1 chỉ dùng làm đối chiếu.
