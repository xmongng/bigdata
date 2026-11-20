from datetime import datetime, timezone
from functools import lru_cache
import json
from pathlib import PurePosixPath
from uuid import uuid4

from fastapi import Depends, FastAPI, File, HTTPException, UploadFile
from pydantic import BaseModel

from .adapters import create_outbox

app = FastAPI(title="Vietnamese News Lakehouse Ingestion V2")
get_outbox = lru_cache(maxsize=1)(create_outbox)
MAX_FILE_BYTES = 10 * 1024 * 1024
MAX_REQUEST_BYTES = 50 * 1024 * 1024


class Article(BaseModel):
    article_id: str
    title: str
    source: str = ""
    category: str = ""
    clean_content: str = ""
    published_at: str = ""


def article_key(value):
    value = value.strip()
    if not value or len(value) > 256:
        raise HTTPException(422, "Invalid article_id")
    return value


def deliver(outbox, items):
    ids = [event["event_id"] for _, event, _ in items]
    try:
        outbox.add(items)
    except Exception:
        raise HTTPException(503, "Could not persist events") from None
    for event_id in ids:
        try:
            outbox.deliver(event_id)
        except Exception:
            raise HTTPException(503, {"message": "Delivery pending", "event_ids": ids}) from None


@app.get("/health")
def health():
    return {"status": "up", "service": "ingestion-api"}


@app.post("/upload-files", status_code=202)
def upload(files: list[UploadFile] = File(...), outbox=Depends(get_outbox)):
    if len(files) > 10:
        raise HTTPException(413, "Maximum 10 files per request")
    results, items = [], []
    total = 0
    for file in files:
        filename = PurePosixPath((file.filename or "input.json").replace(chr(92), "/")).name
        if not filename.lower().endswith(".json"):
            raise HTTPException(400, "Only JSON files are accepted")
        content = file.file.read(MAX_FILE_BYTES + 1)
        total += len(content)
        if len(content) > MAX_FILE_BYTES or total > MAX_REQUEST_BYTES:
            raise HTTPException(413, "Upload byte limit exceeded")
        try:
            payload = json.loads(content.decode("utf-8-sig"))
            rows = payload.get("articles", [payload]) if isinstance(payload, dict) else payload
            if not isinstance(rows, list) or not rows:
                raise ValueError
            for row in rows:
                if not isinstance(row, dict) or not isinstance(row.get("article_id"), str):
                    raise ValueError
                article_key(row["article_id"])
        except (ValueError, UnicodeError):
            raise HTTPException(422, "Invalid article JSON") from None
        file_id = uuid4().hex
        key = f"bronze_raw/{file_id}.json"
        event = {"event_id": uuid4().hex, "op": "INGEST_FILE", "file_id": file_id,
                 "s3_key": key, "filename": filename,
                 "received_at": datetime.now(timezone.utc).isoformat()}
        items.append((file_id, event, content))
        results.append({"file": filename, "file_id": file_id, "s3_key": key, "status": "queued"})
    deliver(outbox, items)
    return {"status": "accepted", "files": results}


def mutate(outbox, op, article_id, article=None):
    event = {"event_id": uuid4().hex, "op": op}
    if article is None:
        event["article_id"] = article_id
    else:
        event["article"] = {**article.model_dump(), "article_id": article_id}
    deliver(outbox, [(article_id, event, None)])
    return {"status": "accepted", "op": op, "article_id": article_id}


@app.post("/articles", status_code=202)
def create(article: Article, outbox=Depends(get_outbox)):
    return mutate(outbox, "UPSERT", article_key(article.article_id), article)


@app.put("/articles/{article_id}", status_code=202)
def update(article_id: str, article: Article, outbox=Depends(get_outbox)):
    return mutate(outbox, "UPSERT", article_key(article_id), article)


@app.delete("/articles/{article_id}", status_code=202)
def delete(article_id: str, outbox=Depends(get_outbox)):
    return mutate(outbox, "DELETE", article_key(article_id))
