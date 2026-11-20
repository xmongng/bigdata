# Spark V2

`reference/streaming_job.py` preserves the verified V1 baseline. V2 starts with `src/submit.py` and the offline launcher in `run.sh`.

Each micro-batch sorts Kafka events by partition and offset, handles at most 20 Kafka offsets, normalizes at most 10,000 article mutations and 20 MiB of normalized text, then executes one Iceberg MERGE. Duplicate keys inside a batch use the last observed operation. The same batch ID replay cannot overwrite an already committed row or removal marker. DELETE records a tombstone, and the Query API filters it out. A fresh checkpoint with an existing V2 table is unsafe because batch IDs restart. Kafka producer ambiguity or retries across batches can still repeat an event. Events on separate Kafka partitions do not have a global arrival order. An invalid event fails the batch with its checkpoint intact. Quarantine and controlled resume remain operational gaps.

Provision the isolated bucket/topic/catalog and create the table using `sql/silver/articles-v2.sql` before starting. Drivers and executors are validated only after staged integration tests.
