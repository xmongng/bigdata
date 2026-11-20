import json
import unittest
from unittest.mock import Mock

from src.iceberg_writer import merge
from src.transformations import prepare_batch
from src.validation import article_rows


def event(operation, **fields):
    return json.dumps({"op": operation, **fields})


class BatchTests(unittest.TestCase):
    def test_bronze_shapes_and_bom(self):
        row = {"article_id": " Tiếng Việt "}
        for payload in (row, [row], {"articles": [row]}):
            raw = json.dumps(payload, ensure_ascii=False).encode("utf-8-sig")
            self.assertEqual(article_rows(raw), [row])

    def test_latest_input_per_id_and_tombstone(self):
        events = [event("INGEST_FILE", s3_key="a"), event("UPSERT", article={"article_id": "a", "title": "B"}),
                  event("DELETE", article_id="a")]
        read = lambda _: [{"article_id": " a ", "title": " A "}, {"article_id": "b"}]
        first = prepare_batch(events, read)
        self.assertEqual(first, prepare_batch(events, read))
        self.assertEqual(len(first), 2)
        self.assertEqual(first[0], ("a", "", "", "", "", "", True))

    def test_bad_input_fails_batch(self):
        for value in (None, " ", 123):
            with self.assertRaises(ValueError):
                prepare_batch([event("DELETE", article_id=value)], Mock())

    def test_single_merge_and_batch_revision_guard(self):
        spark = Mock()
        merge(spark, "lake_v2.news_db_v2.articles", [("x", "", "", "", "", "", True)], 32)
        self.assertEqual(spark.sql.call_count, 1)
        self.assertIn("changes._batch_id > target._batch_id", spark.sql.call_args.args[0])
        spark.catalog.dropTempView.assert_called_once()
