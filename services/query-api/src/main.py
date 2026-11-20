from contextlib import closing
import os

from fastapi import Depends, FastAPI, HTTPException, Query

app = FastAPI(title="Vietnamese News Lakehouse Query V2")


def connection_factory():
    import trino

    catalog = os.environ["TRINO_CATALOG"]
    schema = os.environ["TRINO_SCHEMA"]
    if not catalog.endswith("_v2") or not schema.endswith("_v2"):
        raise ValueError("Use an isolated V2 Trino catalog and schema")

    return lambda: trino.dbapi.connect(
        host=os.environ["TRINO_HOST"], port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.environ["TRINO_USER"], catalog=catalog, schema=schema,
        request_timeout=5, max_attempts=2,
    )


@app.get("/health")
def health():
    return {"status": "up", "service": "query-api"}


@app.get("/articles")
def articles(limit: int = Query(default=20, ge=1, le=100), connect=Depends(connection_factory)):
    fields = ("article_id", "title", "source", "category")
    try:
        with closing(connect()) as connection, closing(connection.cursor()) as cursor:
            cursor.execute("SELECT article_id, title, source, category FROM articles WHERE NOT _deleted ORDER BY article_id LIMIT " + str(limit))
            rows = [dict(zip(fields, row)) for row in cursor.fetchall()]
    except Exception:
        raise HTTPException(503, "Trino query unavailable") from None
    return {"count": len(rows), "articles": rows}
