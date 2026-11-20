# Migration checklist

- [ ] `git status` clean before importing.
- [ ] Inventory V1 VM1 volumes, topic, bucket, catalog and backups.
- [ ] Import VM1 and VM2 source via `bash scripts/import-v1.sh` on Mac.
- [ ] Review imported files for credentials and hardcoded LAN addresses.
- [ ] Import Windows API from Windows cloned repo; Git push and Mac pull.
- [ ] Pin API/Spark dependencies and add unit tests for normalization, schema and Kafka event payloads.
- [ ] Refactor monolith into module files without changing endpoint/event contracts initially.
- [ ] Plan V2 isolation: Docker project name, bound ports, named volumes, topic, Polaris catalog, Iceberg namespace/table, Spark checkpoint, API port.
- [ ] Test V2 end-to-end and recovery/rollback. Enable V2 run scripts only after isolation.
- [ ] Plan security: new service credentials, secret management, encryption, monitoring, backup/restore.
