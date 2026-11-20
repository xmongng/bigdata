from .ingestion import decode_event
from .validation import article_id

FIELDS = ("title", "source", "category", "clean_content", "published_at")
MAX_ROWS = 10_000
MAX_TEXT_BYTES = 20 * 1024 * 1024


def prepare_batch(events, read):
    """Events arrive in deterministic partition/offset order."""
    result = {}
    rows_seen = 0
    bytes_seen = 0
    for event in events:
        for deleted, row in decode_event(event, read):
            if not isinstance(row, dict):
                raise ValueError("Invalid article")
            key = article_id(row.get("article_id"))
            values = tuple("" if row.get(field) is None else str(row[field]).strip() for field in FIELDS)
            rows_seen += 1
            bytes_seen += sum(len(value.encode("utf-8")) for value in values)
            if rows_seen > MAX_ROWS or bytes_seen > MAX_TEXT_BYTES:
                raise ValueError("Micro-batch exceeds the V2 safety bound")
            result[key] = (key, *values, deleted)
    return list(result.values())
