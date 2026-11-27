# V1 đang vận hành

FastAPI và Spark Docker Standalone chạy trên VM2. FastAPI gửi JSON Bronze sang MinIO và sự kiện sang Kafka trên VM1. Spark đọc Kafka, tải object Bronze và ghi Iceberg qua Polaris và MinIO. Polaris dùng PostgreSQL trên VM1. Khi có GET, FastAPI gọi Trino trong mạng Docker trên VM2. Windows và MacBook có thể mở dashboard trên VM2 qua cổng `8000`.

Xem [cấu hình triển khai hai VM](v1-two-vms.md). [Inventory](runtime-inventory.md) ghi nguồn và checksum từ kiến trúc bốn máy ban đầu.
