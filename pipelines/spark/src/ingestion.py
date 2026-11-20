import json
import re

from .validation import MAX_BYTES, article_rows


def bronze_reader(s3, bucket):
    def read(key):
        if not isinstance(key, str) or not re.fullmatch(r"bronze_raw/[a-f0-9]{32}\.json", key):
            raise ValueError("Invalid Bronze object key")
        body = s3.get_object(Bucket=bucket, Key=key)["Body"]
        try:
            return article_rows(body.read(MAX_BYTES + 1))
        finally:
            body.close()

    return read


def decode_event(raw, read):
    event = json.loads(raw)
    if not isinstance(event, dict):
        raise ValueError("Invalid event")
    op = event.get("op")
    if op == "INGEST_FILE":
        yield from ((False, row) for row in read(event["s3_key"]))
    elif op == "UPSERT":
        yield False, event["article"]
    elif op == "DELETE":
        yield True, {"article_id": event["article_id"]}
    else:
        raise ValueError("Unsupported event")
