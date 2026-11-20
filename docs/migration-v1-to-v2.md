# Migration V1 → V2

1. Kiểm kê và chọn commit baseline qua quy trình Git private. Đối chiếu source runtime, môi trường và phiên bản.
2. Cấp Kafka/MinIO/Polaris staging và Trino/API độc lập theo bảng cách ly trong operations.md.
3. Chạy CI, kiểm thử API contract, MERGE, replay cùng batch, lỗi Kafka/MinIO, lỗi dữ liệu, query và rollback bằng dữ liệu giả.
4. Có kế hoạch backfill riêng và đối chiếu số ID/giá trị. Checkpoint V1 thuộc V1 và chỉ được giữ cho job V1.
5. Review bảo mật, phân quyền, backup/restore, SLA, monitoring và release. Duyệt cutover riêng và chỉ chuyển traffic khi có bằng chứng.
