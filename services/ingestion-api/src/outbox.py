import json
import sqlite3
from contextlib import closing


class Outbox:
    def __init__(self, path, storage, publisher):
        self.path = path
        self.storage = storage
        self.publisher = publisher
        with closing(self.connect()) as connection, connection:
            connection.execute("""CREATE TABLE IF NOT EXISTS pending (
                id TEXT PRIMARY KEY, kafka_key TEXT NOT NULL, event TEXT NOT NULL,
                payload BLOB, stored INTEGER NOT NULL DEFAULT 0, sent INTEGER NOT NULL DEFAULT 0
            )""")

    def connect(self):
        return sqlite3.connect(self.path, timeout=20, isolation_level="IMMEDIATE")

    def add(self, items):
        with closing(self.connect()) as connection, connection:
            connection.executemany("INSERT INTO pending (id, kafka_key, event, payload) VALUES (?, ?, ?, ?)", [
                (event["event_id"], key, json.dumps(event, ensure_ascii=False), payload)
                for key, event, payload in items
            ])

    def pending_ids(self):
        with closing(self.connect()) as connection:
            return [row[0] for row in connection.execute(
                "SELECT id FROM pending WHERE sent=0 ORDER BY rowid"
            )]

    def deliver(self, event_id):
        # Persist the Bronze step separately so Kafka retry uses the original object.
        with closing(self.connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT event,payload,stored,sent FROM pending WHERE id=?", (event_id,)).fetchone()
            if row is None:
                raise KeyError(event_id)
            raw, payload, stored, sent = row
            if not sent and not stored and payload is not None:
                self.storage(json.loads(raw)["s3_key"], payload)
                connection.execute("UPDATE pending SET stored=1,payload=NULL WHERE id=?", (event_id,))
        with closing(self.connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            key, raw, sent, sequence = connection.execute(
                "SELECT kafka_key,event,sent,rowid FROM pending WHERE id=?", (event_id,)
            ).fetchone()
            if not sent:
                earlier = connection.execute(
                    "SELECT 1 FROM pending WHERE kafka_key=? AND sent=0 AND rowid<? LIMIT 1", (key, sequence)
                ).fetchone()
                if earlier:
                    raise RuntimeError("Earlier event for this key is still pending")
                self.publisher(key, json.loads(raw))
                connection.execute("UPDATE pending SET sent=1 WHERE id=?", (event_id,))
