import json
import os

from .outbox import Outbox
from .settings import Settings


def create_outbox():
    import boto3
    from botocore.config import Config
    from confluent_kafka import Producer

    settings = Settings.load()
    s3 = boto3.client("s3", endpoint_url=settings.endpoint, region_name="us-east-1",
                      aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
                      aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
                      config=Config(connect_timeout=3, read_timeout=5,
                                    retries={"total_max_attempts": 2}))
    producer = Producer({"bootstrap.servers": settings.bootstrap, "acks": "all",
                         "enable.idempotence": True, "message.timeout.ms": 8000})

    def store(key, content):
        s3.put_object(Bucket=settings.bucket, Key=key, Body=content,
                      ContentType="application/json")

    def publish(key, event):
        results = []
        producer.produce(settings.topic, key=key.encode(), value=json.dumps(event).encode(),
                         on_delivery=lambda error, _: results.append(error))
        if producer.flush(9) or results != [None]:
            raise RuntimeError("Kafka did not confirm delivery")

    return Outbox(settings.outbox, store, publish)


if __name__ == "__main__":
    outbox = create_outbox()
    for event_id in outbox.pending_ids():
        try:
            outbox.deliver(event_id)
        except Exception:
            print("DELIVERY_PENDING", event_id)
            raise SystemExit(1) from None
        print("DELIVERY_CONFIRMED", event_id)
