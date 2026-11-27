# Chạy Lakehouse V1 trên hai VM

`bigdata` trên Windows là nguồn chính của mã, Compose và cấu hình mẫu. Đồng bộ bản sẽ chạy lên VM, sửa từ Windows cho các lần triển khai tiếp theo.

| Máy | Dịch vụ | Nguồn trong `bigdata` |
| --- | --- | --- |
| VM1 `192.168.1.195` | Kafka, MinIO, Polaris, PostgreSQL | [Compose](../infrastructure/docker/compose.yaml), [SQL bootstrap](../infrastructure/docker/postgres-init/001-schema.sql), [provision Polaris](../infrastructure/polaris/provision_catalog.py), [topic Kafka](../infrastructure/kafka/topic-v1.env) |
| VM2 `192.168.1.197` | Spark V1 | [Compose offline](../pipelines/spark/v1/compose.yml), [Dockerfile](../pipelines/spark/v1/Dockerfile), [launcher](../pipelines/spark/v1/launch-offline.sh), [streaming job](../pipelines/spark/reference/streaming_job.py) |
| VM2 `192.168.1.197` | FastAPI, dashboard, Trino | [Compose](../infrastructure/docker/compose.v1-vm2.yaml), [FastAPI và dashboard](../services/news-api-v1/app.py), [catalog Trino](../trino/catalog/lake.properties), [30 bài mẫu](../examples/v1/articles) |

FastAPI và Spark kết nối tới Kafka và MinIO trên VM1. Spark commit Iceberg qua Polaris. FastAPI gọi `trino:8080` trong Docker để đọc bảng `lake.news_db.articles`. Dashboard mở qua cổng `8000` của VM2. Spark UI tiếp tục dùng cổng host `8080`.

## Cấu hình và dữ liệu triển khai

- **VM1:** [vm1.env.example](../configs/v1/vm1.env.example) khai báo các biến root MinIO, PostgreSQL và Polaris. Trên VM1 hiện hữu, dùng lại `~/vn-news-core/.env`, `client.env` và các volume V1. Khi dựng VM1 trống, tạo bucket `vn-news-lake` bằng client S3 có quyền tạo bucket, dùng [script Kafka](../infrastructure/kafka/create-v1-topic.sh) để tạo topic, rồi chạy `provision_catalog.py` với `POLARIS_ADMIN_SECRET` để tạo catalog và cấp client. Provisioning ghi `client.env` trong thư mục chạy.
- **Spark:** [spark.env.example](../configs/v1/spark.env.example) khai báo endpoint, OAuth, đường dẫn JAR và checkpoint. VM2 hiện hữu có thể tiếp tục dùng `~/news-spark/.env`, `~/.ivy2/jars` và `~/news-spark/checkpoints`. Nếu dựng mới, chuẩn bị image `vn-news-spark:3.5.7` và sáu JAR đã đối chiếu. Xác minh [tệp Spark](../pipelines/spark/v1/spark-dist.sha256) và [các JAR](../pipelines/spark/v1/jars.sha256) bằng `sha256sum -c` trong thư mục chứa tài nguyên. Để build image, dùng `docker build -t vn-news-spark:3.5.7 -f <repo>/pipelines/spark/v1/Dockerfile <thư-mục-chứa-spark-tgz>`.
- **FastAPI và Trino:** [v1-api.env.example](../infrastructure/docker/v1-api.env.example) khai báo image Trino 484, IP bind, MinIO, Kafka và credential Polaris. `POLARIS_OAUTH_CREDENTIAL` có dạng `client_id:client_secret`. Compose gắn mã dashboard, dữ liệu mẫu và catalog trực tiếp từ cây `bigdata`.

Các file mẫu để trống giá trị bí mật. Bản sao riêng trên Windows nằm cạnh `bigdata` trong `.env.v1-private`. Bộ cài Spark và sáu JAR được lưu riêng cạnh repo trong `v1-runtime-assets`. Khi đồng bộ mã, bỏ qua file bí mật, Spark checkpoint, dữ liệu Kafka/MinIO/PostgreSQL và image/JAR. Đóng gói bằng danh sách file cần chuyển hoặc Git từ nguồn chính.

## Chạy từ bản triển khai của bigdata

Trong các ví dụ dưới đây, `$repo` là thư mục `bigdata` đã đồng bộ lên VM.

Trên **VM1**, kiểm tra cấu hình repo với file môi trường hiện có. Compose đã đối chiếu trùng nội dung với bản đang chạy. Giữ project `vnnews` để dùng lại các volume.

```bash
docker compose --env-file "$HOME/vn-news-core/.env" \
  -f "$repo/infrastructure/docker/compose.yaml" config --quiet
```

Trên **VM2**, chọn cấu hình và checkpoint của Spark hiện có. `SPARK_V1_CHECKPOINTS_DIR` trỏ tới thư mục cha chứa `main`.

```bash
export SPARK_V1_ENV_FILE="$HOME/news-spark/.env"
export SPARK_V1_JARS_DIR="$HOME/.ivy2/jars"
export SPARK_V1_CHECKPOINTS_DIR="$HOME/news-spark/checkpoints"
docker compose -f "$repo/pipelines/spark/v1/compose.yml" config --quiet
```

Khi chuyển driver đang chạy sang cấu hình repo, dùng cùng project `vn-news-spark` và cùng checkpoint. Chạy `docker compose -f "$repo/pipelines/spark/v1/compose.yml" up -d --no-deps spark-streaming` để thay đúng driver. Khi dựng pipeline mới hoàn toàn, chỉ bật `SPARK_V1_ALLOW_NEW_CHECKPOINT=1` tại lần chạy đầu với thư mục checkpoint cha ghi được và chưa có `main`.

Khởi động FastAPI và Trino trên VM2 bằng file môi trường đã cấp:

```bash
docker compose --env-file "$HOME/lakehouse-v1-two-vm/.env.v1-vm2" \
  -f "$repo/infrastructure/docker/compose.v1-vm2.yaml" up -d --build --wait
```

## Kiểm tra Spark gần thời gian thực

Theo dõi log driver trên VM2:

```bash
docker logs --follow --timestamps --tail=50 spark-streaming
```

Sau đó chạy phép thử từ bản `bigdata`. Lệnh ghi một bài tổng hợp với ID `smoke-two-vm-…` vào V1 và giữ bài để đối chiếu.

```bash
docker compose --env-file "$HOME/lakehouse-v1-two-vm/.env.v1-vm2" \
  -f "$repo/infrastructure/docker/compose.v1-vm2.yaml" \
  exec -T api python - < "$repo/scripts/smoke_v1_two_vm.py"
```

Đối chiếu `s3_key` trong phản hồi HTTP `202` với `FILE_OK` và `BATCH_OK` trong log Spark. Phép thử đợi Trino trả đúng nội dung bài và đo độ trễ từ lúc API trả `202`. Nhịp micro-batch V1 là 5 giây, còn kết quả thực tế phụ thuộc backlog, thời gian ghi Iceberg và thời điểm kiểm tra Trino.

### Kết quả nghiệm thu trên hai VM

Ngày 26/11/2026, hệ thống thực hiện một lượt kiểm thử toàn luồng với **một file JSON chứa một bài tổng hợp** trên bản triển khai `v1-82a1bffb5d48`. Phép thử diễn ra sau khi chuyển dịch vụ sang bản đồng bộ từ `bigdata`. Spark tiếp tục xử lý từ checkpoint V1 sau batch 33.

| Hạng mục | Cách đo hoặc đối chiếu | Kết quả |
| --- | --- | --- |
| Tiếp nhận upload | Từ lúc gửi yêu cầu đến lúc FastAPI trả HTTP `202`. API đã lưu file vào MinIO và nhận xác nhận giao event từ Kafka tại mốc này. | **0,03 giây** |
| Xử lý sự kiện | Đối chiếu `s3_key` từ phản hồi upload với log Spark. | `FILE_OK ... ROWS 1` và `BATCH_OK 34 EVENTS 1` |
| Dữ liệu xuất hiện qua truy vấn | Từ lúc API trả `202` đến khi phép thử nhận được phản hồi đầu tiên có đúng bài qua Trino. | **23,50 giây**, đọc được **1/1 bài** |

Khoảng 23,50 giây bao gồm thời gian chờ micro-batch, đọc Kafka và MinIO, thực hiện `MERGE` và commit Iceberg, cùng thời gian truy vấn và kiểm tra lặp lại. Cấu hình 5 giây quy định nhịp kích hoạt micro-batch. Phép thử đo hai khoảng thời gian nêu trong bảng, nên chưa xác định được độ trễ của từng công đoạn.

**Kết luận nghiệm thu:** Đạt yêu cầu kiểm thử chức năng end-to-end trên hai VM với một bài mới. Số đo 23,50 giây áp dụng cho lượt thử sau triển khai này. Đánh giá độ trễ khi hệ thống chạy ổn định cần thêm các lượt đo lặp lại.
