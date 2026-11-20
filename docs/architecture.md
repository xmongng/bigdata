# Current V1 and intended V2

```mermaid
flowchart TD
  API[Windows FastAPI] -->|upload| MINIO[VM1 MinIO Bronze]
  API -->|Kafka events| KAFKA[VM1 Kafka]
  KAFKA --> SPARK[VM2 Spark Structured Streaming]
  SPARK -->|read raw| MINIO
  SPARK -->|Iceberg MERGE| MINIO
  SPARK -->|REST catalog| POLARIS[VM1 Polaris]
  POLARIS --> POSTGRES[VM1 PostgreSQL]
  API -->|SQL| TRINO[Mac Trino]
  TRINO --> POLARIS
  TRINO --> MINIO
```

This starter repository lives on MacBook and Windows (Git); runtime V1 remains on remote machines. V2 deployments will require unique resources, security policies and tests before cutting over.
