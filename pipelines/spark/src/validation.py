import json

MAX_BYTES = 10 * 1024 * 1024


def article_id(value):
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > 256:
        raise ValueError("Invalid article_id")
    return value.strip()


def article_rows(raw):
    if len(raw) > MAX_BYTES:
        raise ValueError("Bronze object too large")
    payload = json.loads(raw.decode("utf-8-sig"))
    rows = payload.get("articles", [payload]) if isinstance(payload, dict) else payload
    if not isinstance(rows, list) or not rows or any(not isinstance(row, dict) for row in rows):
        raise ValueError("Invalid article collection")
    return rows
