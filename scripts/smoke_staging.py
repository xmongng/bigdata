"""Read-only by default. --write-staging adds one uniquely named synthetic article."""
import argparse
import json
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4

INGESTION = "http://127.0.0.1:18000"
QUERY = "http://127.0.0.1:18001"


def request(url, data=None, headers=None):
    with urlopen(Request(url, data=data, headers=headers or {}), timeout=10) as response:
        return response.status, json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write-staging", action="store_true")
    args = parser.parse_args()
    for base in (INGESTION, QUERY):
        status, _ = request(base + "/health")
        if status != 200:
            raise SystemExit("Staging health check failed")
    _, initial = request(QUERY + "/articles?limit=100")
    print("STAGING_READ_OK", initial["count"])
    if not args.write_staging:
        return
    article_id = "000-smoke-" + uuid4().hex
    lookup = QUERY + "/articles?" + urlencode({"article_id": article_id, "limit": 1})
    boundary = uuid4().hex
    payload = json.dumps({"article_id": article_id, "title": "Staging smoke test", "source": "synthetic"})
    body = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"smoke.json\"\r\n"
            f"Content-Type: application/json\r\n\r\n{payload}\r\n--{boundary}--\r\n").encode()
    started = time.monotonic()
    status, accepted = request(INGESTION + "/upload-files", body, {
        "Content-Type": "multipart/form-data; boundary=" + boundary
    })
    if status != 202 or accepted.get("status") != "accepted":
        raise SystemExit("POST did not confirm delivery")
    print("STAGING_POST_202", article_id, round(time.monotonic() - started, 3))
    while time.monotonic() - started < 90:
        _, observed = request(lookup)
        if any(row["article_id"] == article_id and row["title"] == "Staging smoke test" for row in observed["articles"]):
            print("STAGING_E2E_OK", article_id, round(time.monotonic() - started, 3))
            return
        time.sleep(2)
    raise SystemExit(f"Timed out waiting for {article_id}")


if __name__ == "__main__":
    main()
