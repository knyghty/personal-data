#!/bin/sh
# Download the bucket's new raw files into raw/ and build the tables into a local DuckLake (see build.py).
# Needs the bucket's AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_ENDPOINT_URL_S3 in the environment.
set -eu
cd "$(dirname "$0")/.."
RAW_ROOT=. uv run -q --with duckdb==1.5.6 python -c 'import sys; sys.path.insert(0, "duckdb"); import sync; print(sync.reconcile(sync.connect()), "files downloaded")'
LAKE_CATALOG=lake.sqlite LAKE_DATA=lake/ uv run -q duckdb/build.py || echo "build.sh: some tables failed, see above" >&2
echo "To query, run duckdb and then: ATTACH 'ducklake:sqlite:lake.sqlite' AS personal (READ_ONLY); USE personal;"
