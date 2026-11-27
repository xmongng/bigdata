import json
import os
import uuid
from time import perf_counter
from pathlib import Path
from datetime import datetime, timezone

import boto3
import trino
from dashboard import router as dashboard_router
from confluent_kafka import Producer
from dotenv import load_dotenv
from fastapi import FastAPI, File, HTTPException, UploadFile, Query, Response
from pydantic import BaseModel

load_dotenv()
app = FastAPI(title='Vietnamese News Lakehouse API')
app.include_router(dashboard_router)
BUCKET = os.environ['MINIO_BUCKET']
TOPIC = 'vn-news-events'

s3 = boto3.client(
    's3',
    endpoint_url=os.environ['MINIO_ENDPOINT'],
    aws_access_key_id=os.environ['MINIO_ACCESS_KEY'],
    aws_secret_access_key=os.environ['MINIO_SECRET_KEY'],
    region_name='us-east-1',
)
producer = Producer({
    'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP'],
    'acks': 'all',
    'enable.idempotence': True,
})


class Article(BaseModel):
    article_id: str
    title: str
    source: str = ''
    category: str = ''
    clean_content: str = ''
    published_at: str = ''


def send_event(key, event):
    failed = []

    def callback(error, message):
        if error is not None:
            failed.append(str(error))

    try:
        producer.produce(
            TOPIC,
            key=key.encode('utf-8'),
            value=json.dumps(event, ensure_ascii=False).encode('utf-8'),
            on_delivery=callback,
        )
        remaining = producer.flush(15)
        if remaining != 0 or failed:
            raise RuntimeError('Kafka delivery failed: ' + str(failed))
    except Exception as exc:
        raise HTTPException(503, detail='Kafka unavailable: ' + str(exc))


@app.get('/health')
def health():
    return {'status': 'up', 'service': 'windows-fastapi'}


@app.post('/upload-files', status_code=202)
def upload_files(response: Response, files: list[UploadFile] = File(...)):
    results = []
    minio_ms = kafka_ms = 0.0
    for upload in files:
        filename = Path(upload.filename or 'input.json').name
        if not filename.lower().endswith('.json'):
            raise HTTPException(400, detail='Only JSON files are accepted')
        upload.file.seek(0, 2)
        size = upload.file.tell()
        upload.file.seek(0)
        if size > 10 * 1024 * 1024:
            raise HTTPException(413, detail='File too large for demo (max 10 MB)')
        file_id = uuid.uuid4().hex
        key = 'bronze_raw/' + file_id + '.json'
        try:
            started = perf_counter()
            s3.upload_fileobj(upload.file, BUCKET, key)
            minio_ms += (perf_counter() - started) * 1000
        except Exception as exc:
            raise HTTPException(503, detail='MinIO upload failed: ' + str(exc))
        event = {
            'event_id': uuid.uuid4().hex,
            'op': 'INGEST_FILE',
            'file_id': file_id,
            's3_key': key,
            'filename': filename,
            'received_at': datetime.now(timezone.utc).isoformat(),
        }
        started = perf_counter()
        send_event(file_id, event)
        kafka_ms += (perf_counter() - started) * 1000
        results.append({'file': filename, 'file_id': file_id, 's3_key': key, 'status': 'queued'})
    response.headers['Server-Timing'] = f'minio;dur={minio_ms:.3f}, kafka;dur={kafka_ms:.3f}'
    return {'status': 'accepted', 'files': results}


@app.post('/articles', status_code=202)
def create_article(article: Article):
    send_event(article.article_id, {'op': 'UPSERT', 'article': article.model_dump()})
    return {'status': 'accepted', 'op': 'UPSERT', 'article_id': article.article_id}


@app.put('/articles/{article_id}', status_code=202)
def update_article(article_id: str, article: Article):
    data = article.model_dump()
    data['article_id'] = article_id
    send_event(article_id, {'op': 'UPSERT', 'article': data})
    return {'status': 'accepted', 'op': 'UPSERT', 'article_id': article_id}


@app.delete('/articles/{article_id}', status_code=202)
def delete_article(article_id: str):
    send_event(article_id, {'op': 'DELETE', 'article_id': article_id})
    return {'status': 'accepted', 'op': 'DELETE', 'article_id': article_id}


@app.get('/articles')
def get_articles(response: Response, limit: int = Query(default=20, ge=1, le=100)):
    try:
        started = perf_counter()
        conn = trino.dbapi.connect(
            host=os.environ['TRINO_HOST'],
            port=int(os.getenv('TRINO_PORT', '8080')),
            user='news_api',
            catalog='lake',
            schema='news_db',
        )
        cur = conn.cursor()
        cur.execute('SELECT article_id, title, source, category FROM articles ORDER BY article_id LIMIT ' + str(limit))
        records = cur.fetchall()
        cur.close()
        conn.close()
        response.headers['Server-Timing'] = f'trino;dur={(perf_counter() - started) * 1000:.3f}'
        return {'count': len(records), 'articles': [
            dict(zip(['article_id', 'title', 'source', 'category'], r)) for r in records
        ]}
    except Exception as exc:
        raise HTTPException(503, detail='Trino query failed: ' + str(exc))
