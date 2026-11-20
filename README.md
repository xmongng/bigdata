# Vietnamese News Lakehouse

V1 đang chạy trên VM1/VM2, MacBook và Windows. Workspace này chứa cả snapshot V1 phục vụ đối chiếu lẫn code V2 staging.

- [Kiến trúc V1](docs/architecture-current.md) và [inventory đã đối chiếu](docs/runtime-inventory.md)
- [Kiến trúc V2](docs/architecture-target.md), [vận hành staging](docs/operations.md), [kế hoạch chuyển đổi](docs/migration-v1-to-v2.md)
- [Audit bảo mật](docs/security.md) và [rà soát mức độ hoàn thiện V2](docs/v2-readiness.md)

## Kiểm tra local trên Mac hoặc Windows

Tại root thực tế chứa `CODEX_LAKEHOUSE_HANDOFF.md`, dùng Python 3.12 và Node 22 trở lên.

```text
node scripts/scan-secrets.mjs
python -B scripts/test_apis.py
```

Từ `pipelines/spark`, chạy `python -B -m unittest discover -s tests`.
Python tests API cần dependency trong `requirements-test.txt`; các adapter hạ tầng được mock. Chạy `bash scripts/check-local.sh` bằng Bash để kiểm tra syntax shell, Compose và preview. Trên Windows có thể dùng Git Bash hoặc chạy `docker compose config --quiet` theo các biến trong script. CI dùng cùng bộ test này.

## Git private

Đọc audit và review toàn bộ file. Khi đã duyệt baseline, nếu chưa có Git thì chạy `git init -b main`, stage từng file hoặc nhóm đã review, rồi chạy `node scripts/scan-secrets.mjs --staged`, `git diff --cached --check`, `git diff --cached --stat` và `git diff --cached --name-only`. Commit sau khi diff sạch và được duyệt.

Sau đó tạo private repo trống trên nền tảng được chọn, kiểm tra trạng thái Private và URL chính thức, chạy `git remote add origin <PRIVATE_REPOSITORY_URL>` nếu chưa có origin. Chỉ sau xác nhận mới chạy `git push -u origin main`.

`infrastructure/docker/compose.yaml`, `infrastructure/polaris/provision_catalog.py` và `trino/catalog/lake.properties` là snapshot V1. `compose.staging.yaml` và `compose.vm2.yaml` là cấu hình V2, cần môi trường đã cách ly và nghiệm thu. `compose.production.yaml` còn để trống.
