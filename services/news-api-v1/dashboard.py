import json
import os
from contextlib import closing
from pathlib import Path

import trino
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse

WEB = Path(__file__).resolve().parent / 'web'
DATASET = WEB.parent.parent / 'data' / 'vn_news_30_bai_that_2026-11-19' / 'articles'
RESULTS = WEB.parent / 'benchmark' / 'results'
router = APIRouter()


@router.get('/', include_in_schema=False)
def dashboard():
    return FileResponse(WEB / 'index.html')


@router.get('/dashboard/{asset}', include_in_schema=False)
def dashboard_asset(asset: str):
    if asset not in {'dashboard.css', 'dashboard.js', 'metrics.js'}:
        raise HTTPException(404)
    return FileResponse(WEB / asset)


@router.get('/api/dashboard/preset')
def preset():
    return [{'name': path.name, 'content': path.read_text(encoding='utf-8-sig')}
            for path in sorted(DATASET.glob('*.json'))]


@router.get('/api/dashboard/benchmark')
def benchmark():
    for path in sorted(RESULTS.glob('*/results.json'), reverse=True):
        try:
            result = json.loads(path.read_text(encoding='utf-8'))
            if result.get('scenarios'):
                return {'run': result['run'], 'scenarios': [
                    {**{key: row[key] for key in ('name', 'requests', 'files', 'expected', 'seen', 'last', 'complete')},
                     'ack': max((item['elapsed'] for item in row['uploads'] if item['status'] == 202), default=None)}
                    for row in result['scenarios']
                ], 'reads': result['reads']}
        except (OSError, ValueError, KeyError):
            continue
    return None


def query(sql, parameters=None):
    try:
        with closing(trino.dbapi.connect(
            host=os.environ['TRINO_HOST'],
            port=int(os.getenv('TRINO_PORT', '8080')),
            user='news_api',
            catalog='lake',
            schema='news_db',
            request_timeout=8,
            max_attempts=1,
        )) as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(sql, parameters)
                return cursor.fetchall()
            finally:
                cursor.close()
    except (trino.exceptions.Error, trino.exceptions.HttpError) as exc:
        raise HTTPException(503, 'Không đọc được bảng Iceberg qua Trino.') from exc


@router.get('/api/dashboard/analysis')
def analysis(category: str = Query(default='', max_length=100)):
    groups = query("""
        SELECT coalesce(nullif(category, ''), 'Chưa phân loại'), count(*)
        FROM articles GROUP BY 1 ORDER BY 2 DESC, 1
    """)
    rows = query("""
        SELECT article_id, title, source, category, published_at, clean_content
        FROM articles
        WHERE (? = '' OR coalesce(nullif(category, ''), 'Chưa phân loại') = ?)
        ORDER BY published_at DESC NULLS LAST, article_id
        LIMIT 100
    """, [category, category])
    fields = ('article_id', 'title', 'source', 'category', 'published_at', 'clean_content')
    sources = query("""
        SELECT coalesce(nullif(source, ''), 'Chưa rõ nguồn'), count(*)
        FROM articles GROUP BY 1 ORDER BY 2 DESC, 1
    """)
    return {
        'total': sum(count for _, count in groups),
        'categories': [{'name': name, 'count': count} for name, count in groups],
        'sources': [{'name': name, 'count': count} for name, count in sources],
        'articles': [dict(zip(fields, row)) for row in rows],
    }


@router.get('/api/dashboard/visible')
def visible(article_id: list[str] = Query(min_length=1, max_length=100)):
    if any(not value or len(value) > 256 for value in article_id):
        raise HTTPException(422, 'article_id phải dài từ 1 đến 256 ký tự.')
    placeholders = ', '.join('?' for _ in article_id)
    rows = query(f'SELECT article_id, title, source, category FROM articles WHERE article_id IN ({placeholders})', article_id)
    fields = ('article_id', 'title', 'source', 'category')
    return {'article_ids': [row[0] for row in rows], 'articles': [dict(zip(fields, row)) for row in rows]}
