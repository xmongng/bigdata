import json
import os
import sys
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

S3_BUCKET = os.environ['MINIO_BUCKET']
KAFKA = os.environ['KAFKA_BOOTSTRAP']

spark = (
    SparkSession.builder
    .appName('vn-news-kafka-to-iceberg')
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .config('spark.sql.catalog.lake', 'org.apache.iceberg.spark.SparkCatalog')
    .config('spark.sql.catalog.lake.type', 'rest')
    .config('spark.sql.catalog.lake.uri', os.environ['POLARIS_URI'])
    .config('spark.sql.catalog.lake.oauth2-server-uri', 'http://192.168.1.195:8181/api/catalog/v1/oauth/tokens')
    .config('spark.sql.catalog.lake.warehouse', 'news_lake')
    .config('spark.sql.catalog.lake.credential',
            os.environ['POLARIS_CLIENT_ID'] + ':' + os.environ['POLARIS_CLIENT_SECRET'])
    .config('spark.sql.catalog.lake.scope', 'PRINCIPAL_ROLE:ALL')
    .config('spark.sql.catalog.lake.header.Polaris-Realm', 'POLARIS')
    .config('spark.sql.catalog.lake.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
    .config('spark.sql.catalog.lake.s3.endpoint', os.environ['MINIO_ENDPOINT'])
    .config('spark.sql.catalog.lake.s3.path-style-access', 'true')
    .config('spark.sql.catalog.lake.s3.access-key-id', os.environ['MINIO_ACCESS_KEY'])
    .config('spark.sql.catalog.lake.s3.secret-access-key', os.environ['MINIO_SECRET_KEY'])
    .config('spark.sql.catalog.lake.client.region', 'us-east-1')
    .config('spark.redaction.regex', '(?i)secret|password|token|credential|access.key')
    .getOrCreate()
)

s3 = boto3.client(
    's3',
    endpoint_url=os.environ['MINIO_ENDPOINT'],
    aws_access_key_id=os.environ['MINIO_ACCESS_KEY'],
    aws_secret_access_key=os.environ['MINIO_SECRET_KEY'],
    region_name='us-east-1',
)

# Bộ dữ liệu demo tối thiểu: một row/một article_id; cùng khóa được MERGE, tránh duplicate.
record_schema = StructType([
    StructField('article_id', StringType(), False),
    StructField('title', StringType(), True),
    StructField('source', StringType(), True),
    StructField('category', StringType(), True),
    StructField('clean_content', StringType(), True),
    StructField('published_at', StringType(), True),
])

spark.sql('CREATE NAMESPACE IF NOT EXISTS lake.news_db')
spark.sql('''
CREATE TABLE IF NOT EXISTS lake.news_db.articles (
  article_id STRING,
  title STRING,
  source STRING,
  category STRING,
  clean_content STRING,
  published_at STRING
) USING iceberg
''')
print('ICEBERG_TABLE_READY: lake.news_db.articles', flush=True)


def normalize(doc):
    if not isinstance(doc, dict):
        return None
    article_id = str(doc.get('article_id') or '').strip()
    if not article_id:
        return None
    return (
        article_id,
        str(doc.get('title') or '').strip(),
        str(doc.get('source') or '').strip(),
        str(doc.get('category') or '').strip(),
        str(doc.get('clean_content') or '').strip(),
        str(doc.get('published_at') or '').strip(),
    )


def upsert(doc):
    row = normalize(doc)
    if row is None:
        raise ValueError('article_id missing')
    incoming = spark.createDataFrame([row], schema=record_schema)
    incoming.createOrReplaceTempView('incoming_article')
    spark.sql('''
      MERGE INTO lake.news_db.articles AS t
      USING incoming_article AS s
      ON t.article_id = s.article_id
      WHEN MATCHED THEN UPDATE SET
        title=s.title, source=s.source, category=s.category,
        clean_content=s.clean_content, published_at=s.published_at
      WHEN NOT MATCHED THEN INSERT
        (article_id, title, source, category, clean_content, published_at)
      VALUES
        (s.article_id, s.title, s.source, s.category, s.clean_content, s.published_at)
    ''')


def delete(doc):
    article_id = str(doc.get('article_id') or '').strip()
    if not article_id:
        raise ValueError('DELETE event missing article_id')
    spark.createDataFrame([(article_id,)], ['article_id']).createOrReplaceTempView('deleting_article')
    spark.sql('''
      MERGE INTO lake.news_db.articles AS t
      USING deleting_article AS d ON t.article_id = d.article_id
      WHEN MATCHED THEN DELETE
    ''')


def handle_event(event):
    op = event.get('op')
    if op == 'INGEST_FILE':
        obj = s3.get_object(Bucket=S3_BUCKET, Key=event['s3_key'])
        payload = json.loads(obj['Body'].read().decode('utf-8-sig'))
        if isinstance(payload, dict):
            payload = payload.get('articles', [payload])
        if not isinstance(payload, list):
            raise ValueError('JSON file must contain an object or a list of objects')
        for item in payload:
            upsert(item)
        print('FILE_OK', event['s3_key'], 'ROWS', len(payload), flush=True)
    elif op == 'UPSERT':
        upsert(event['article'])
        print('UPSERT_OK', event['article'].get('article_id'), flush=True)
    elif op == 'DELETE':
        delete(event)
        print('DELETE_OK', event['article_id'], flush=True)
    else:
        raise ValueError('Unsupported operation: ' + str(op))


def process_batch(batch, batch_id):
    # Chỉ phù hợp demo nhỏ. Không collect dữ liệu lớn trên driver.
    rows = (batch.selectExpr('CAST(value AS STRING) AS raw', 'partition', 'offset')
            .orderBy('partition', 'offset').collect())
    for row in rows:
        handle_event(json.loads(row['raw']))
    if rows:
        print('BATCH_OK', batch_id, 'EVENTS', len(rows), flush=True)


if '--init-only' in sys.argv:
    spark.stop()
    sys.exit(0)

query = (
    spark.readStream.format('kafka')
    .option('kafka.bootstrap.servers', KAFKA)
    .option('subscribe', 'vn-news-events')
    .option('startingOffsets', 'earliest')
    .option('maxOffsetsPerTrigger', 20)
    .load()
    .writeStream.foreachBatch(process_batch)
    .option('checkpointLocation', 'file://' + os.path.expanduser('~/news-spark/checkpoints/main'))
    .trigger(processingTime='5 seconds')
    .start()
)
print('STREAM_RUNNING: waiting for Kafka events', flush=True)
query.awaitTermination()
