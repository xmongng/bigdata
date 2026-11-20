-- Illustrative read-only query, not a deployed Gold table.
SELECT category, COUNT(*) AS article_count
FROM lake.news_db.articles
GROUP BY category
ORDER BY article_count DESC;
