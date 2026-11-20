import importlib.util
from pathlib import Path
import sqlite3
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]


def load(name, service):
    src = ROOT / "services" / service / "src"
    package = types.ModuleType(name)
    package.__path__ = [str(src)]
    sys.modules[name] = package
    spec = importlib.util.spec_from_file_location(name + ".main", src / "main.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ingest = load("v2_ingestion", "ingestion-api")
query = load("v2_query", "query-api")
from v2_ingestion.outbox import Outbox


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store, self.publish = Mock(), Mock()
        self.outbox = Outbox(Path(self.temp.name) / "outbox.sqlite", self.store, self.publish)
        ingest.app.dependency_overrides[ingest.get_outbox] = lambda: self.outbox
        self.addCleanup(ingest.app.dependency_overrides.clear)
        self.client = TestClient(ingest.app)

    def test_multi_upload_and_validation(self):
        files = [("files", ("a.json", b'{"article_id":"a"}')),
                 ("files", ("b.json", b'{"articles":[{"article_id":"b"}]}'))]
        response = self.client.post("/upload-files", files=files)
        self.assertEqual(response.status_code, 202)
        self.assertEqual(len(response.json()["files"]), 2)
        self.assertEqual(self.store.call_count, 2)
        self.assertEqual(self.publish.call_count, 2)
        self.assertEqual(self.client.post("/upload-files", files=files + [
            ("files", ("bad.json", b"{}"))]).status_code, 422)
        self.assertEqual(self.store.call_count, 2)

    def test_outbox_retries_using_same_bronze_key(self):
        self.publish.side_effect = RuntimeError("unavailable")
        response = self.client.post("/upload-files", files={"files": ("a.json", b'{"article_id":"a"}')})
        self.assertEqual(response.status_code, 503)
        event_id = response.json()["detail"]["event_ids"][0]
        self.assertEqual(self.outbox.pending_ids(), [event_id])
        self.publish.side_effect = None
        self.outbox.deliver(event_id)
        self.outbox.deliver(event_id)
        self.assertEqual(self.store.call_count, 1)
        self.assertEqual(self.publish.call_count, 2)
        self.assertEqual(self.outbox.pending_ids(), [])

    def test_crud_and_query_contract(self):
        data = {"article_id": "source", "title": "A"}
        self.assertEqual(self.client.post("/articles", json=data).status_code, 202)
        self.assertEqual(self.client.put("/articles/target", json=data).json()["article_id"], "target")
        self.assertEqual(self.client.delete("/articles/target").json()["op"], "DELETE")
        self.assertEqual(self.publish.call_args.args[0], "target")
        conn = Mock()
        conn.cursor.return_value.fetchall.return_value = [("target", "A", "N", "tin")]
        query.app.dependency_overrides[query.connection_factory] = lambda: lambda: conn
        self.addCleanup(query.app.dependency_overrides.clear)
        reader = TestClient(query.app)
        self.assertEqual(reader.get("/articles").json()["count"], 1)
        self.assertEqual(reader.get("/articles?limit=101").status_code, 422)
        conn.close.assert_called_once()
        conn.cursor.return_value.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
