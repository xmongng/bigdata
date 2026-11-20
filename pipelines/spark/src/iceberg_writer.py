SCHEMA = "article_id STRING, title STRING, source STRING, category STRING, clean_content STRING, published_at STRING, _deleted BOOLEAN, _batch_id LONG"


def merge(spark, table, rows, batch_id):
    if not rows:
        return
    incoming = spark.createDataFrame([(*row, batch_id) for row in rows], SCHEMA)
    incoming.createOrReplaceTempView("lakehouse_incoming_v2")
    try:
        spark.sql(f"""MERGE INTO {table} AS target
            USING lakehouse_incoming_v2 AS changes
            ON target.article_id = changes.article_id
            WHEN MATCHED AND changes._batch_id > target._batch_id THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """).collect()
    finally:
        spark.catalog.dropTempView("lakehouse_incoming_v2")
