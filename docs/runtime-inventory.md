# Inventory V1 và trạng thái migration

| Thành phần | Nguồn local | Bằng chứng và trạng thái |
| --- | --- | --- |
| Spark Streaming V1 | `pipelines/spark/reference/streaming_job.py` | VERIFIED. SHA-256 `09a8e4cb7882e18080271b895222d21e35e54ff9e54f646293c181b6e67690f1` khớp file local, host và container qua SSH chỉ đọc |
| Spark Docker V1 | Báo cáo vận hành ngày 19/11 và kiểm tra container | VERIFIED host `/home/hduser/news-spark/streaming_job.py` và container `/app/streaming_job.py`. Image `vn-news-spark:3.5.7`, Master, Worker, Streaming Up khi kiểm tra. Spark Standalone client, 2 executor cores, JAR Kafka Spark 3.5.7/Scala 2.12 và Iceberg 1.10.1. Checkout V2 chưa tái lập chính xác image V1 |
| VM1 | `infrastructure/docker/compose.yaml`, `infrastructure/polaris/provision_catalog.py` | Snapshot V1. Compose dùng tên `vnnews`, port và volume V1. Không chạy để test V2. Chưa đối chiếu byte-for-byte VM1 trong audit này |
| Windows | `../../news-api/app.py` tính từ root repo thật | Nguồn đối chiếu hợp đồng V1. Ingestion V2 ban đầu chỉ có placeholder, query V2 chưa có route |
| Trino | `trino/catalog/lake.properties` | Snapshot trỏ tới VM1, credential tham chiếu env. Runtime Mac chưa xác minh trong lượt audit này |
| Spark V2 | `pipelines/spark/src/`, `run.sh` | Ban đầu có 5 module scaffold và runner bị khóa. Cấu hình Docker V2 chỉ dùng tên và kho trạng thái riêng |
| Production / CI | `compose.production.yaml`, `.github/workflows/` | Ban đầu là placeholder. Mọi kiểm tra staging/production phụ thuộc vào nghiệm thu sau khi cấp quyền |
| SQL | `sql/gold/topic_stats.sql` | Truy vấn minh họa chỉ đọc. Chưa có Gold table |

Checkpoint V1 nằm dưới `/home/hduser/news-spark/checkpoints/main` và được mount cho Docker driver. Không dùng lại trong V2. Khi đối chiếu trên VM2, chỉ đọc: `docker ps -a --filter name=spark`, `sha256sum ~/news-spark/streaming_job.py` và `docker exec spark-streaming sha256sum /app/streaming_job.py`.
