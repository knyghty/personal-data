"""Keep a local copy of the bucket's raw/ under RAW_ROOT, gunzipped, for build.py and read_json.

The collector names each new file in its ingest call; `reconcile` lists the bucket for anything a call missed.
"""

import gzip
import os
from concurrent.futures import ThreadPoolExecutor

import duckdb

BUCKET = os.environ.get("BUCKET_NAME", "personal-data-raw")
# The directory that holds raw/, so raw/<source>/... resolves from it as it did in the repo.
ROOT = os.environ.get("RAW_ROOT", "/data")
BATCH = 100


def secret() -> str:
    """CREATE SECRET for the bucket, from the variables `fly storage create` set."""

    def quoted(name: str, default: str = "") -> str:
        return "'" + os.environ.get(name, default).replace("'", "''") + "'"

    endpoint = os.environ.get("AWS_ENDPOINT_URL_S3", "").removeprefix("https://").removeprefix("http://")
    return (
        f"CREATE OR REPLACE SECRET bucket (TYPE s3, KEY_ID {quoted('AWS_ACCESS_KEY_ID')}, "
        f"SECRET {quoted('AWS_SECRET_ACCESS_KEY')}, ENDPOINT '{endpoint}', REGION {quoted('AWS_REGION', 'auto')})"
    )


def connect() -> duckdb.DuckDBPyConnection:
    """A connection that reads the bucket."""
    con = duckdb.connect()
    con.execute(secret())
    return con


def local(key: str) -> str:
    """The local path of a bucket key: raw/a/b.json.gz becomes <ROOT>/raw/a/b.json."""
    return os.path.join(ROOT, key.removesuffix(".gz"))


def download(con: duckdb.DuckDBPyConnection, keys: list[str]) -> int:
    """Fetch `keys`, eight batches at a time, and write them gunzipped, each atomically. Returns how many were written."""

    def batch(urls: list[str]) -> int:
        cur = con.cursor()  # a cursor per thread: one connection runs one query at a time
        rows = cur.execute("SELECT filename, content FROM read_blob(?)", [urls]).fetchall()
        cur.close()
        for url, content in rows:
            path = local(url.removeprefix(f"s3://{BUCKET}/"))
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path + ".tmp", "wb") as f:
                f.write(gzip.decompress(content) if url.endswith(".gz") else content)
            os.replace(path + ".tmp", path)
        return len(rows)

    urls = [f"s3://{BUCKET}/{k}" for k in keys]
    with ThreadPoolExecutor(8) as pool:
        return sum(pool.map(batch, [urls[i : i + BATCH] for i in range(0, len(urls), BATCH)]))


def keys(paths: list[str]) -> list[str]:
    """The bucket keys of the repo-relative paths in an ingest call, raw/ only."""
    return [p + ".gz" for p in paths if p.startswith("raw/") and ".." not in p.split("/")]


def reconcile(con: duckdb.DuckDBPyConnection) -> int:
    """Download every file in the bucket that the local copy lacks. Returns how many were written."""
    listed = [r[0] for r in con.execute(f"SELECT file FROM glob('s3://{BUCKET}/raw/**')").fetchall()]
    missing = [k for k in (u.removeprefix(f"s3://{BUCKET}/") for u in listed) if not os.path.exists(local(k))]
    return download(con, missing)
