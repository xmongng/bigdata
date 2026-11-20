import os

from .config import Settings
from .ingestion import bronze_reader
from .iceberg_writer import merge
from .transformations import prepare_batch


def main():
    import boto3
    from botocore.config import Config
    from pyspark.sql import SparkSession

    settings = Settings.load()
    uri = os.environ["POLARIS_URI"].rstrip("/")
    prefix = "spark.sql.catalog.lake_v2."
    builder = SparkSession.builder.appName("vn-news-stream-v2").config(
        "spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    ).config("spark.redaction.regex", "(?i)secret|password|token|credential|access[.-]key")
    options = {
        "type": "rest", "uri": uri, "oauth2-server-uri": uri + "/v1/oauth/tokens",
        "warehouse": settings.warehouse, "scope": "PRINCIPAL_ROLE:ALL",
        "header.Polaris-Realm": "POLARIS", "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "credential": os.environ["POLARIS_CLIENT_ID"] + ":" + os.environ["POLARIS_CLIENT_SECRET"],
        "s3.endpoint": os.environ["MINIO_ENDPOINT"], "s3.path-style-access": "true",
        "s3.access-key-id": os.environ["MINIO_ACCESS_KEY"],
        "s3.secret-access-key": os.environ["MINIO_SECRET_KEY"], "client.region": "us-east-1",
    }
    builder.config(prefix.rstrip("."), "org.apache.iceberg.spark.SparkCatalog")
    for name, value in options.items():
        builder.config(prefix + name, value)
    spark = builder.getOrCreate()
    s3 = boto3.client("s3", endpoint_url=os.environ["MINIO_ENDPOINT"],
                      aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
                      aws_secret_access_key=os.environ["MINIO_SECRET_KEY"], region_name="us-east-1",
                      config=Config(connect_timeout=3, read_timeout=10, retries={"total_max_attempts": 2}))
    read = bronze_reader(s3, settings.bucket)

    def process(batch, batch_id):
        ordered = batch.selectExpr("CAST(value AS STRING) AS raw", "partition", "offset").orderBy("partition", "offset")
        try:
            rows = prepare_batch((row.raw for row in ordered.toLocalIterator()), read)
            merge(spark, settings.table, rows, batch_id)
        except Exception:
            print(f"BATCH_FAILED batch={batch_id}; checkpoint retained", flush=True)
            raise
        print(f"BATCH_OK batch={batch_id} unique_ids={len(rows)}", flush=True)

    stream = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP"])
              .option("subscribe", settings.topic).option("startingOffsets", "earliest")
              .option("maxOffsetsPerTrigger", 20).load())
    query = (stream.writeStream.foreachBatch(process).option("checkpointLocation", settings.checkpoint)
             .trigger(processingTime="5 seconds").start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
