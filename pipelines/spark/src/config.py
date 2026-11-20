import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    bucket: str
    topic: str
    warehouse: str
    checkpoint: str
    table: str = "lake_v2.news_db_v2.articles"

    @classmethod
    def load(cls):
        values = cls(os.environ["MINIO_BUCKET"], os.environ["KAFKA_TOPIC"],
                     os.environ["POLARIS_WAREHOUSE"], os.environ["SPARK_CHECKPOINT"])
        if (not values.bucket.endswith("-v2") or not values.topic.endswith("-v2")
                or not values.warehouse.endswith("_v2")
                or not values.checkpoint.startswith("file:///state/v2/")
                or ".." in values.checkpoint.split("/")):
            raise ValueError("Spark requires isolated V2 resources")
        return values
