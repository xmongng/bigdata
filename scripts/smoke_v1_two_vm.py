import json
import time
from uuid import uuid4

import requests


def main():
    base = "http://127.0.0.1:8000"
    article = {
        "article_id": "smoke-two-vm-" + uuid4().hex,
        "title": "Two-VM stream check",
        "source": "smoke-test",
        "category": "test",
    }
    article_id = article["article_id"]

    with requests.Session() as session:
        for path in ("/health", "/articles?limit=1"):
            session.get(base + path, timeout=15).raise_for_status()

        print(f"article_id={article_id}", flush=True)
        started = time.monotonic()
        response = session.post(
            base + "/upload-files",
            files={"files": (article_id + ".json", json.dumps(article), "application/json")},
            timeout=30,
        )
        if response.status_code != 202:
            raise SystemExit(f"Upload returned HTTP {response.status_code}")
        accepted = time.monotonic()
        key = response.json()["files"][0]["s3_key"]
        print(f"HTTP 202 after {accepted - started:.2f}s; s3_key={key}", flush=True)

        deadline = accepted + 120
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            response = session.get(
                base + "/api/dashboard/visible",
                params={"article_id": article_id},
                timeout=min(15, remaining),
            )
            response.raise_for_status()
            if article in response.json()["articles"]:
                print(f"Trino returned the expected article {time.monotonic() - accepted:.2f}s after HTTP 202")
                return
            time.sleep(min(1, max(0, deadline - time.monotonic())))

    raise SystemExit(f"Article not visible after 120s; check Spark logs for {key}")


if __name__ == "__main__":
    main()
