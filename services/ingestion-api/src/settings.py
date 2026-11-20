from dataclasses import dataclass
from pathlib import Path
import os


@dataclass(frozen=True)
class Settings:
    endpoint: str
    bucket: str
    bootstrap: str
    topic: str
    outbox: Path

    @classmethod
    def load(cls):
        bucket = os.environ["MINIO_BUCKET"]
        topic = os.environ["KAFKA_TOPIC"]
        if not bucket.endswith("-v2") or not topic.endswith("-v2"):
            raise ValueError("Use isolated V2 bucket and topic names ending in -v2")
        return cls(os.environ["MINIO_ENDPOINT"], bucket, os.environ["KAFKA_BOOTSTRAP"],
                   topic, Path(os.environ["OUTBOX_DB"]))
