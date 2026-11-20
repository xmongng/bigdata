# Kiểm tra trước khi publish private Git

- Checkout này chưa có .git khi audit. Không có index, commit, lịch sử hay remote để xác minh.
- Chạy `node scripts/scan-secrets.mjs` trước khi stage. Bộ quét nội bộ báo đường dẫn, dòng và tên quy tắc. Đây là heuristic, cần đọc diff và quét lịch sử bằng công cụ chuyên dụng trước khi đưa repo hiện có lên remote.
- Khi đã khởi tạo Git và chọn file stage, chạy `node scripts/scan-secrets.mjs --staged`, `git diff --cached --check`, `git diff --cached --stat` và `git diff --cached --name-only`. Chỉ mở diff ở môi trường riêng. Nếu từng lộ secret, ngừng publish, thu hồi/rotate qua kênh an toàn và review lịch sử.
- Loại khỏi Git credentials, local env, archive, checkpoint, log, database, backup, JAR cục bộ và import thô. Dữ liệu thật đi qua hệ thống runtime riêng.
- Snapshot chứa IP LAN, tên tài nguyên và topology V1. Đây là metadata nội bộ cần người sở hữu review trước khi publish private.
- Staging chưa có TLS, ACL đầy đủ, RBAC API, HA hay kiểm thử sao lưu phục hồi. Chỉ dùng tài nguyên cô lập trong mạng đã được phép.
