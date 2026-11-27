# Cấu hình topic Kafka V1

[`topic-v1.env`](topic-v1.env) khai báo topic `vn-news-events`, số partition và số bản sao. Dòng `KAFKA_TOPIC_PARTITIONS=5` đặt 5 partition khi tạo topic. [`create-v1-topic.sh`](create-v1-topic.sh) đọc file này. [Compose VM1](../docker/compose.yaml) cấu hình broker và listener `kafka:19092` trong mạng Docker.

Khi chuẩn bị một môi trường V1 mới, chạy lệnh sau từ thư mục Compose trên VM1 (ví dụ `~/vn-news-core`) sau khi broker Kafka đã sẵn sàng. Dùng đường dẫn tới file script trong bản checkout trên VM1.

```bash
bash /path/to/bigdata/infrastructure/kafka/create-v1-topic.sh
```

Script dùng `--create --if-not-exists` để tạo topic nếu cần, rồi in `PartitionCount` và `ReplicationFactor` thực tế. Với topic đã có, script chỉ xem cấu hình hiện tại ở bước `--describe`. Môi trường thử V2 dùng topic riêng.
