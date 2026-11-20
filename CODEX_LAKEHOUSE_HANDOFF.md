# CODEX PROJECT HANDOFF — Vietnamese News Lakehouse

> **Mục đích:** Đặt file này tại **thư mục gốc** của repository `vietnamese-news-lakehouse/`, sau đó mở repository trong Codex (Codex CLI, Codex trên Zed hoặc Codex app) và dán prompt tại phần **A**. Đây là tài liệu bàn giao công việc cho Codex, **không phải tập lệnh để chạy tự động**.
>
> **Trạng thái:** V2 đang trong quá trình migration từ V1; **chưa phải production-ready**. Ưu tiên số 1 là kiểm tra mã nguồn thực tế, bí mật và khả năng tái lập môi trường trước khi push Git.

## A. PROMPT KHỞI ĐỘNG — Dán nguyên đoạn này vào Codex

```text
Bạn là Senior Software Engineer, Data Platform Engineer và DevOps reviewer cho dự án Vietnamese News Lakehouse.

Hãy đọc toàn bộ CODEX_LAKEHOUSE_HANDOFF.md trong thư mục gốc, sau đó đọc README.md, .gitignore, docs/, scripts/, infrastructure/, pipelines/, services/, trino/, configs/, sql/ và trạng thái Git hiện tại.

Mục tiêu của phiên làm việc:
1. Audit source code và cấu hình đang có; phân biệt V1 thực tế, snapshot import, file placeholder và V2 chưa hoàn thiện.
2. Kiểm tra an toàn trước khi publish Git: .env, client.env, tokens, credentials, private keys, hardcoded secrets, logs, Docker data, checkpoint, dữ liệu cá nhân và thông tin nhạy cảm; KHÔNG in giá trị bí mật ra chat/log.
3. Xác minh sự khác biệt giữa Spark V1 đã import và Spark đang chạy bằng Docker Standalone trên VM2. Nếu không truy cập VM2, nêu đúng lệnh read-only cần tôi chạy và ghi trạng thái UNVERIFIED. Không tự kết luận rằng file pipelines/spark/src/main.py đang là file container thực thi.
4. Sửa những vấn đề an toàn, cục bộ, có thể kiểm chứng: .gitignore, cấu trúc package, tài liệu setup, script kiểm tra, lint/tests cơ bản và hướng dẫn push Git private. Tuyệt đối không thay đổi runtime V1, không SSH hoặc deploy lên máy từ xa nếu chưa được tôi cho phép.
5. Lập và cập nhật kế hoạch theo thứ tự P0 Security/Git → P1 Runtime inventory → P2 API → P3 Spark → P4 Docker/Infrastructure/Trino → P5 CI/CD & E2E. Triển khai các thay đổi LOCAL có phạm vi rõ, chạy test sau mỗi bước; không đánh dấu hoàn tất khi chưa có bằng chứng.
6. Hỗ trợ đưa repository lên PRIVATE Git remote sau khi tôi duyệt Git diff và secret scan. KHÔNG tự tạo repository public, KHÔNG tự git push/force-push/rewrite history nếu chưa có xác nhận.

BẮT ĐẦU BẰNG: chạy kiểm tra read-only; tạo bảng inventory file / trạng thái / nguồn tham chiếu; phát hiện blockers P0; đề xuất những thay đổi đầu tiên. Nếu an toàn, thực hiện refactor cục bộ theo từng commit nhỏ và báo rõ những gì đã sửa, test nào đã chạy, test nào chưa chạy. Hỏi tôi trước mọi hành động thay đổi máy ảo, Docker volume, Kafka topic, Polaris catalog, Iceberg table, Spark checkpoint, credential hoặc Git remote.

Yêu cầu đầu ra: (a) tình trạng repo, (b) security findings theo mức độ, (c) roadmap có acceptance criteria, (d) diff tóm tắt, (e) lệnh test và push Git an toàn, (f) các vấn đề còn cần xác minh trên VM1/VM2/Windows.

Dùng tiếng Việt, tên file/code và thuật ngữ kỹ thuật có thể dùng tiếng Anh.
```

## B. Bối cảnh thực tế đã xác nhận

### B1. Các máy và trách nhiệm

| Máy | Endpoint / truy cập | Trách nhiệm | Nguồn đã biết |
|---|---|---|---|
| **VM1 – master** | `hduser@192.168.1.195` | Docker: Kafka 3.9.1, MinIO-compatible `pgsty/silo`, PostgreSQL 16, Polaris 1.8.0 | `~/vn-news-core` |
| **VM2 – slave02** | `hduser@192.168.1.197` | **Spark Docker Standalone**, Spark Master/Worker/streaming; runtime thực tế cần kiểm kê | `/app/streaming_job.py` bên trong container, đường dẫn source trên host **chưa xác minh** |
| **MacBook** | Local Zed/Codex | Repository phát triển, Trino Docker (container `vn-trino`) | `~/bigdata/vietnamese-news-lakehouse`, `~/vn-news-trino` |
| **Windows laptop** | Local PowerShell/Codex | FastAPI hiện tại, phát triển và đồng bộ Git | `D:\Downloads\cty\lakehouse\news-api` |

**Ghi chú IP:** Địa chỉ trên là trạng thái từng quan sát được, có thể thay đổi khi chuyển mạng. Không hardcode IP trong code mới khi có thể dùng biến môi trường/config riêng của môi trường.

### B2. Đã quan sát được ở thời điểm bàn giao

- MacBook đã import `infrastructure/docker/compose.yaml`, `infrastructure/polaris/provision_catalog.py` (nguồn VM1 là `provision_catalog_v2.py`), `infrastructure/docker/postgres-init/001-schema.sql`, `pipelines/spark/src/main.py` và `trino/catalog/lake.properties`.
- `scripts/control-v1.sh vm1 status` đã trả các container `vnnews-kafka-1`, `vnnews-minio-1`, `vnnews-postgres-1`, `vnnews-polaris-1` ở trạng thái **Up**; `vnnews-polaris-bootstrap-1` **Exited (0)** là trạng thái hoàn thành bình thường.
- `scripts/control-v1.sh vm2 status` thấy Spark Submit đang chạy với `--master spark://spark-master:7077`, `--deploy-mode client`, `--executor-cores 2`, `--total-executor-cores 2`, và Python `/app/streaming_job.py`.
- File Spark V1 import có `normalize`, `upsert`, `process_batch`, `spark.readStream`, `MERGE INTO` nhưng **chưa được đối chiếu** với `/app/streaming_job.py` đang chạy trong Docker trên VM2.
- `services/ingestion-api/src/main.py` còn chứa `BOOTSTRAP_PLACEHOLDER` tại lần kiểm tra gần nhất; FastAPI `app.py` Windows chưa được nhập vào repo vào thời điểm đó.
- Các module `pipelines/spark/src/{config,ingestion,transformations,validation,iceberg_writer}.py`, `services/query-api/` và `compose.production.yaml` có thể là scaffold; cần đọc thực tế, **không giả định đã implement**.

### B3. Semantics luồng dữ liệu V1 cần bảo toàn khi refactor

```text
Windows Ingestion API
  ├─ Upload raw JSON -> VM1 MinIO, bucket vn-news-lake, prefix bronze_raw/
  └─ Kafka Producer -> topic vn-news-events (5 partitions)
         -> VM2 Spark Structured Streaming
         -> validate/normalize/upsert/delete
         -> Apache Iceberg: catalog lake/news_lake, schema news_db, table articles
         -> files trong MinIO, catalog qua Polaris, metadata persistence PostgreSQL

Windows GET /articles -> Trino trên MacBook -> Polaris + MinIO -> response
```

- Sự kiện hiện có: `INGEST_FILE`, `UPSERT`, `DELETE`. Khoá bản ghi: `article_id`.
- API `202 Accepted` chỉ xác nhận đã nhận/xếp hàng, **không đồng nghĩa Iceberg đã commit**.
- Trong code được import từ hướng dẫn V1, `foreachBatch` dùng `collect()` rồi xử lý từng event; cần cải tiến theo batch, kiểm tra idempotency và xử lý sự kiện có thứ tự. Không khẳng định cấu hình trigger/offset V1 giống Docker runtime mới.
- Checkpoint và volume là dữ liệu trạng thái, **không thuộc Git**.

## C. QUY TẮC AN TOÀN BẮT BUỘC CHO CODEX

1. **Không đụng runtime V1 nếu chưa được xác nhận cụ thể:** không `docker compose up/down`, `docker run/stop/rm`, `kubectl apply`, deploy SSH, sửa file remote, khởi động lại process hay ghi DB chỉ để thử code.
2. **Tuyệt đối cấm thao tác phá dữ liệu:** `docker compose down -v`, `docker volume prune`, `docker system prune --volumes`, xóa `checkpoints`, xóa object MinIO, reset Kafka offsets, drop Iceberg table, re-bootstrap Polaris, rerun provisioning V1, sinh lại secret đang dùng.
3. **Không dùng V2 chung tên resource với V1 khi thử nghiệm:** tách Docker project name, tên container, cổng, network, Kafka topic, bucket/prefix, Polaris catalog/warehouse, checkpoint, application ID và credentials khi phù hợp. Tránh sửa `name: vnnews` của V1 rồi chạy trực tiếp tại V2.
4. **Không tự đưa source code và dữ liệu ra ngoài:** không Git public, không đăng toàn bộ `.env`/`client.env`, không upload log chứa credential, không commit `legacy-import/`, checkpoint, dữ liệu MinIO, backups, khóa SSH/private key.
5. **Không tin placeholder:** file/README ghi có một thành phần không có nghĩa thành phần đó tồn tại trong runtime. Kiểm tra code thật và bằng chứng test.
6. **Không đưa mật khẩu vào log/chat:** báo tên file/dòng, loại nguy cơ, cách khắc phục; nếu cần thay credential, yêu cầu người vận hành tự thực hiện qua kênh an toàn.
7. **Giữ tương thích V1** trong khi phát triển V2. Chưa xóa file/endpoint/khoá dữ liệu đang dùng cho đến khi có tests và migration plan.
8. **Kiểm thử trước triển khai:** tests, linter, formatting, Compose validation, syntax check, rồi review diff; nếu không chạy được vì thiếu Docker/network, ghi rõ `NOT RUN` và lý do.
9. **Trước Git remote mutation:** show summary của `git status`, `git diff --cached --stat`, danh sách file staged, kết quả quét secrets; cần xác nhận trước khi `git push`, tạo repo, thay quyền chia sẻ, force-push hoặc rewrite history.
10. **Không đưa ra nhãn production-ready** khi hạ tầng vẫn là Kafka 1 broker, Trino/FastAPI trên laptop, plaintext LAN, chưa có HA/TLS/backup-restore/monitoring/CI-CD đầy đủ.

## D. PHASED TASK LIST — việc Codex cần hoàn thiện

### P0 — Git readiness và secrets (BLOCKING trước khi push)

- Kiểm kê `.gitignore`, `.env.example`, tracked/untracked/ignored files, symlinks và kích thước dữ liệu; phát hiện credential hardcode trong YAML/Python/SQL/Markdown/shell và Git history nếu có.
- Bảo đảm exclude `.env`, `client.env`, SSH key, certificates, `.venv`, `node_modules`, runtime data, Kafka/Spark checkpoints, `legacy-import/`, event logs, database dumps và credentials. `.env.example` chỉ chứa placeholder an toàn.
- Không mặc định `.gitignore` sẽ bảo vệ file **đã từng commit**. Nếu phát hiện secret trong history, dừng publish và đề xuất quy trình rotate credential + clean history có xác nhận.
- Chuẩn hoá README: sơ đồ kiến trúc thực tế, file nào đã import, file nào placeholder, hướng dẫn Mac/Windows, lệnh chỉ-đọc kiểm tra VM, cách chạy test local.
- Thêm pre-commit hoặc CI secret scanning phù hợp (`gitleaks` hoặc công cụ tương đương nếu sẵn có), không yêu cầu xuất secret vào output.
- Chuẩn bị Git remote **private**; ghi hướng dẫn nhưng chưa tự push.

**Done khi:** working tree được kiểm kê; không có secrets trong staged diff; tài liệu đã phân biệt source V1 và V2; người dùng đã duyệt commit/push.

### P1 — Xác minh Spark Docker runtime VM2 (PHẢI LÀM trước khi refactor sâu)

- Xác định container Spark master/worker/streaming, Docker Compose project và đường dẫn mount host <-> container.
- Kiểm tra Dockerfile, Compose, Spark config, package JAR versions, môi trường Python, driver host, checkpoint, network, resource limits, startup command.
- Đối chiếu `/app/streaming_job.py` trong container với `pipelines/spark/src/main.py` đã import (cùng checksum, diff, nơi code được build/mount).
- Nếu được cho phép, chỉ chạy **lệnh read-only** trên VM2. Không stop/restart/change container.
- Đưa cấu hình đã xác minh vào `infrastructure/docker/compose.vm2.yaml`, `infrastructure/spark/` và code tương ứng, nhưng **không copy credential và không bật V2**.
- Ghi `docs/runtime-inventory.md`: file -> host -> container -> ứng dụng đang chạy; đánh dấu `VERIFIED` / `UNVERIFIED`.

**Done khi:** source từ Git có thể build ra image tương ứng với code và dependency đã xác minh, hoặc những sai lệch đã được liệt kê minh bạch.

### P2 — Chuẩn hoá FastAPI

- Khi file Windows `D:\Downloads\cty\lakehouse\news-api\app.py` được import, xác minh `/health`, `POST /upload-files`, `POST/PUT/DELETE /articles`, `GET /articles` giữ hợp đồng API V1.
- Tách `services/ingestion-api` và `services/query-api` hợp lý; tách router, schema, settings, Kafka producer, MinIO client, Trino client, error handling.
- Dùng pydantic settings/typed configuration phù hợp; secrets chỉ từ env/secret provider.
- Thêm timeout, connection cleanup, input validation, size limits, error responses, retry/backoff có giới hạn. Xử lý rủi ro MinIO upload thành công nhưng Kafka publish thất bại (reconciliation/outbox/idempotency, không im lặng mất event).
- Thêm unit tests dùng mocks; không yêu cầu Kafka/MinIO/Polaris live để test nhanh.

**Done khi:** API tests chạy local, OpenAPI rõ và các endpoint quan trọng còn tương thích với V1 hoặc được version hoá có tài liệu.

### P3 — Refactor Spark ETL / Iceberg

- Tách code thành `config.py`, `ingestion.py`, `validation.py`, `transformations.py`, `iceberg_writer.py`, `main.py`; loại hardcoded VM IP khỏi source.
- Hạn chế `collect()` về Driver và MERGE một bài mỗi lần; đề xuất xử lý DataFrame theo micro-batch, MERGE theo batch, tránh O(N) commit cho file N bài.
- Thiết kế chính sách rõ cho UPSERT/DELETE/INGEST_FILE, event duplicates, event ordering, retry, poison messages và DLQ/quarantine.
- Bảo toàn checkpoint và cách quản lý Kafka offset; **không quảng cáo exactly-once end-to-end** nếu chưa chứng minh idempotent sink và quy trình recovery.
- Thêm tests chuẩn hoá JSON (một object/list/`articles`), null/blank `article_id`, UTF-8/BOM, schema, duplicate IDs, batch replay, late events, MERGE.
- Tạo build image Spark Docker có dependency pin, runbook restart và smoke tests; không triển khai vào V1 khi chưa duyệt.

**Done khi:** tests có thể chạy và runtime V2 biệt lập chạy được một end-to-end scenario không ảnh hưởng V1.

### P4 — Infrastructure / Trino / môi trường

- Chuẩn hoá `infrastructure/docker/compose.yaml`, `compose.vm2.yaml`, `compose.production.yaml`; phân biệt rõ actual/V1 vs target/V2.
- Cấu hình env theo môi trường bằng template không secrets; `development.yaml`, `staging.yaml`, `production.yaml` không chứa password.
- Health checks, resource limits, restart policy, internal networks, named volumes, logging, readiness; backup/restore plans.
- Trino REST catalog + Polaris OAuth + MinIO S3 endpoints qua cấu hình; không embed credential; thay đổi Trino config cần restart có kiểm soát.
- Đánh giá Kafka retention, replication, topic management và thay đổi catalog/DDL bằng migration idempotent, không rerun `provision_catalog.py` V1.
- Kiểm tra giới hạn production: cần host luôn bật cho Trino/API, TLS, ACL, HA, backups. Ghi gap analysis, không ngụy tạo đã xử lý.

**Done khi:** Compose config check pass, hướng dẫn deploy/rollback tường minh và staging được cách ly.

### P5 — CI/CD, chất lượng và nghiệm thu

- Tạo `pyproject.toml`/dependency lock phù hợp cho API và Spark; lint/type checks nơi khả thi; tests và CI workflow.
- Viết `docs/architecture-current.md`, `docs/architecture-target.md`, `docs/operations.md`, `docs/migration-v1-to-v2.md`, `docs/security.md`.
- Thêm sample JSON không nhạy cảm, smoke test của Kafka->MinIO->Spark->Iceberg->Trino->FastAPI, read/write test theo từng máy (chỉ chạy live sau khi được phép).
- Script deploy hỗ trợ `--dry-run` hoặc preview mặc định, tách SSH host settings, không dùng `--delete` với runtime data, không tự restart V1. Ghi commit SHA / image tag khi deploy.
- Đưa ra release checklist: tests, secret scan, deploy staging, rollback, restore test, health, logging, eventual consistency và dữ liệu cuối pipeline.

**Done khi:** CI pass, E2E staging có bằng chứng, thủ tục deploy/rollback lặp lại được; production gaps được ghi rõ.

## E. Quy trình Git — dành cho người vận hành và Codex

**Không chạy `git push` trước khi P0 được xác minh.** Chỉ thực hiện trên bản repository local ở MacBook hoặc Windows; không thao tác trong VM1/VM2.

### E1. MacBook: kiểm tra trước khi commit

```bash
cd ~/bigdata/vietnamese-news-lakehouse
pwd

git status --short
cat .gitignore

# Kiểm tra các file được track (nếu đã khởi tạo Git)
git ls-files

# Đảm bảo source import thực tế, không phải placeholder
rg -n 'BOOTSTRAP_PLACEHOLDER|TODO|FIXME' infrastructure pipelines services trino configs || true

# Chạy audit nội dung bí mật trong repo bằng công cụ scan phù hợp.
# ĐỪNG paste kết quả có giá trị credential lên chat.
```

Nếu chưa có `.git/`, khởi tạo một lần:

```bash
git init -b main
```

**Yêu cầu Codex:** xử lý phát hiện trước khi stage; dùng `git add <file-or-directory>` với những file đã kiểm tra thay vì stage bừa bãi, và review staging:

```bash
git diff --cached --check
git diff --cached --stat
git diff --cached --name-only
git diff --cached
```

Nếu an toàn và có xác nhận:

```bash
git commit -m "chore: import audited lakehouse baseline"
```

### E2. Tạo Git repository riêng tư

Trên GitHub/GitLab, tạo repository **Private**, trống (không thêm README tự động nếu local đã có). Lấy URL HTTPS/SSH của repository thật; không đoán URL.

```bash
# Chỉ ví dụ: thay URL bằng repo private bạn vừa tạo.
git remote add origin <PRIVATE_REPOSITORY_URL>
git remote -v

# Chỉ chạy sau khi đã được duyệt nội dung và có commit sạch.
git push -u origin main
```

Nếu `origin` đã tồn tại, **không tự ghi đè**; dùng `git remote -v` để kiểm tra và yêu cầu xác nhận cách sửa.

### E3. Windows: clone và nhập FastAPI

```powershell
New-Item -ItemType Directory -Force D:\Projects | Out-Null
Set-Location D:\Projects
git clone <PRIVATE_REPOSITORY_URL> vietnamese-news-lakehouse
Set-Location D:\Projects\vietnamese-news-lakehouse

# Chạy file .ps1 trong Windows PowerShell, KHÔNG trong zsh của MacBook.
.\scripts\windows\import-api.ps1

# Hoặc copy code Python thủ công, không copy .env.
# Copy-Item 'D:\Downloads\cty\lakehouse\news-api\app.py' '.\services\ingestion-api\src\main.py'
```

Sau khi Codex review code API và secrets:

```powershell
git add services/ingestion-api/src/main.py
git diff --cached --check
git diff --cached --stat
git commit -m "feat: import reviewed FastAPI baseline"
git push
```

Trên MacBook:

```bash
cd ~/bigdata/vietnamese-news-lakehouse
git pull --ff-only
```

**Nguyên tắc:** Mac/Windows đồng bộ source qua commit/push/pull; VM1/VM2 nhận phiên bản build/deploy có kiểm thử, không phải hai bản sửa độc lập.

## F. Các prompt tiếp nối cho Codex

### F1. Khi Codex hoàn thành audit/P0

```text
Hãy dùng inventory và security report đã tạo. Triển khai PHASE P1 chỉ trên local repo: chuẩn hoá tài liệu runtime Spark Docker, tìm và đánh dấu mọi mâu thuẫn giữa code VM2 import và Docker runtime. Được phép tạo file cấu hình mẫu và smoke tests; không tự truy cập SSH hay thay đổi VM. Nếu cần thông tin VM2, đưa ra tối đa 5 lệnh chỉ đọc để tôi chạy và dán output đã che thông tin nhạy cảm. Chỉ kết luận điều đã xác minh.
```

### F2. Khi đã xác minh Spark Docker và nhập FastAPI

```text
Tiếp tục P2 và P3 theo roadmap của CODEX_LAKEHOUSE_HANDOFF.md. Trước khi sửa hãy chỉ ra contract V1 phải giữ, testcase sẽ viết và file sẽ sửa. Sau đó triển khai local theo các commit nhỏ; chạy unit tests/linter. Không thay đổi dữ liệu thực tế, checkpoint, topic, Polaris hay Docker đang chạy. Báo đầy đủ test pass/fail, các giả định và file còn placeholder.
```

### F3. Khi muốn chuẩn bị triển khai V2

```text
Hãy review repository cho một bản V2 staging độc lập. Chuẩn bị deploy plan, resource isolation matrix (Docker project, network, ports, MinIO bucket/prefix, Kafka topic, Polaris catalog, Iceberg warehouse, checkpoint, secrets), runbook rollback và các lệnh DRY-RUN. Không deploy, không dùng cùng checkpoint hoặc tài nguyên ghi với V1, không khởi động Docker Compose thật cho đến khi tôi xác nhận.
```

### F4. Khi muốn đánh giá readiness cuối cùng

```text
Đánh giá V2 theo checklist P0–P5: source-of-truth, reproducible images, unit/integration/E2E tests, Kafka/Spark/Iceberg semantics, secrets, authentication, TLS, backups, HA, logging/metrics, CI/CD, migration/rollback. Đưa bảng PASS/FAIL/UNKNOWN với bằng chứng file và lệnh test. Không sử dụng nhãn 'production-ready' khi chưa có bằng chứng đạt những điều kiện vận hành tương ứng.
```

## G. Những việc Codex cần hỏi / chờ xác nhận từ người dùng

- URL repository **Private** và hình thức xác thực Git; tuyệt đối không hỏi mật khẩu/secret qua chat.
- Tình trạng thực tế của Spark Docker trên VM2 và container `/app/streaming_job.py` (nếu chưa được nhập).
- File FastAPI Windows đã được import và có nằm trong Git hay chưa.
- Môi trường V2 staging có thể dùng tài nguyên nào, có host luôn bật cho Trino/API hay không.
- Quyền thao tác read-only SSH và quyền deploy trên VM1/VM2; mặc định **không có quyền thay đổi runtime**.
- Chính sách vận hành thực tế (LAN demo hay public production), yêu cầu HA/TLS/backup/SLA.

## H. Yêu cầu báo cáo sau mỗi phiên Codex

Codex phải kết thúc bằng bản tóm tắt sau (có thể viết trong `docs/codex-progress.md` nếu cần):

1. **Đã kiểm tra:** đường dẫn, commit hiện tại, module, nguồn runtime/placeholder.
2. **Đã thay đổi:** từng file đáng chú ý và lý do.
3. **Kết quả kiểm thử:** lệnh chạy, PASS/FAIL/NOT RUN và môi trường thực thi.
4. **Rủi ro còn tồn tại:** credentials, Spark Docker drift, V1 compatibility, data migration, Kafka/Iceberg consistency.
5. **Đề xuất bước kế tiếp:** phạm vi cụ thể, quyền cần cấp, kết quả kỳ vọng.
6. **Trạng thái Git:** branch, staged changes, commit (nếu có), remote/push đã được người dùng phê duyệt hay chưa.

---

**Điểm bắt đầu khuyến nghị:** Dán prompt **A** vào Codex sau khi đặt file ở repo root. Chỉ khi P0 không còn blockers mới tạo private remote và push baseline. Mọi thay đổi lớn được thực hiện trong branch riêng, chạy test, review rồi mới merge; V1 tiếp tục hoạt động độc lập.
