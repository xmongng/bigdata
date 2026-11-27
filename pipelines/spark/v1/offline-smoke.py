import sys
import time

from pyspark.sql import SparkSession

started = time.monotonic()
spark = SparkSession.builder.appName("vn-news-offline-smoke").getOrCreate()
try:
    assert spark.version == "3.5.7", spark.version
    loader = spark._jvm.Thread.currentThread().getContextClassLoader()
    for name in (
        "org.apache.spark.sql.kafka010.KafkaSourceProvider",
        "org.apache.kafka.clients.consumer.KafkaConsumer",
        "org.apache.iceberg.spark.SparkCatalog",
        "org.apache.iceberg.aws.s3.S3FileIO",
    ):
        loader.loadClass(name)
    executor_python = spark.sparkContext.parallelize([0, 1], 2).map(
        lambda _: tuple(sys.version_info[:2])
    ).collect()
    assert executor_python == [(3, 10), (3, 10)], executor_python
    print(f"OFFLINE_SPARK_OK Spark {spark.version} executor Python {executor_python} elapsed_s={time.monotonic()-started:.3f}", flush=True)
finally:
    spark.stop()
