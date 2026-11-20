# Plan: raw data out of git, tables in DuckLake

Agreed on 2026-11-19. After the last step ships, remove this file.

## Goal

Keep the repository for code only. Store raw pushes in a Tigris bucket and the tables in DuckLake on the same bucket. Run machines only while they ingest a push or answer a query. Keep every raw sample for good: 20 years at 50 times today's rate is about 5 TB of JSON, or about 500 GB of Parquet.

## Shipped on 2026-11-19

- The collector stores each file gzipped at `raw/<path>.gz` in the bucket `personal-data-raw`, then calls the query server's `/ingest` with the paths. It no longer commits to git. Files added by hand go through `PUT /admin/raw/<path>`.
- The existing 29,125 files from git are in the bucket, 494 MB gzipped. The bucket matched the checkout file for file.
- The query server keeps a gunzipped copy of `raw/` on its volume, for `build.py` and the MCP `read_json` fallback. It downloads the files each ingest call names, and lists the bucket once a day for files whose call was lost.
- `build.py` builds into DuckLake: a SQLite catalog and Parquet files on the volume. Parquet in the bucket made queries up to 25 times slower, so it moved to the volume on 2026-11-20. At about 500 GB of tables, move older partitions back to the bucket. It skips a table whose statement, files and input tables did not change. On a build with no new data, it rebuilds only `build_info`.
- `ha_history` and `ha_statistics` are incremental and merge later files by key. `ha_history`, `apple_workout_route` and `knmi_10min` are partitioned by month.
- A weekly compaction merges small files and deletes the files of snapshots older than a week.
- The server exits after 5 minutes idle, so the machine stops. Requests and ingest calls start it.
- Calendars store only new, changed and removed events after a full first file. Every table's row count matched the old `personal.duckdb` or grew by the new data.

## Changed from the first design

- Gzip instead of zstd: DuckDB and every S3 client read it, and the collector already had it.
- No catalog copy in the bucket: Fly snapshots the volume daily, and a full build from `raw/` recreates the catalog.
- The build reads a local copy of `raw/`, not the bucket, because reading thousands of small files over HTTP on every build is slow. At about 1 TB of raw JSON, keep only recent files locally and read older ones from the bucket.
- `apple_sample` and `apple_heart_rate` stay unpartitioned: writing month partitions ran out of the build's 2 GB.
- The duplicate-rate alert is not built. The measured repeat rate is under 3% outside calendars, and the tables drop repeats by key. Snapshot sources such as `bunq_account` need a file per day, also on days with no change. A general "skip repeated pushes" rule breaks them.

## Left to do

1. Move the other large derived tables to incremental merges where a full rebuild grows slow: `apple_sample`, `location`, `dns_query`, the screen time tables.
2. Rollup tables (daily, hourly, monthly) for the queries that read years of raw samples.
3. Shrink the query server to a cheaper machine once a few days of build peaks are known.

## Open questions

- Decide whether to move raw files older than a year to Tigris Infrequent Access ($0.01 per GB, plus $0.01 per GB read back).
- If storage ever costs more than a few dollars a month, compare Hetzner Object Storage, which costs about €5 per TB.
