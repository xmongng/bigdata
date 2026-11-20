# Spark Docker

Bản đối chiếu V1 có checksum trong docs/runtime-inventory.md. Driver V1 đọc code từ bind mount; image không chứa bản V2. V2 đóng gói src vào image driver riêng và chỉ dùng JAR offline theo run.sh. Compose V2 khai báo container Master/Worker riêng và giới hạn tổng 4,5 GiB. Xác minh tài nguyên đủ cho cả V1 và V2 trước khi dùng chung một host.

Kiểm tra JAR bằng checksum của bộ artifact đã duyệt khi chuẩn bị staging. Không commit file JAR, tarball Spark hoặc checkpoint.
