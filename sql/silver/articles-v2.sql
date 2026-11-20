-- Apply only to the isolated Polaris catalog. The SQL below is Spark SQL.
CREATE NAMESPACE IF NOT EXISTS lake_v2.news_db_v2;
CREATE TABLE IF NOT EXISTS lake_v2.news_db_v2.articles (
    article_id STRING NOT NULL,
    title STRING,
    source STRING,
    category STRING,
    clean_content STRING,
    published_at STRING,
    _deleted BOOLEAN NOT NULL,
    _batch_id BIGINT NOT NULL
) USING iceberg;
