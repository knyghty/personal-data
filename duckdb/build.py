# /// script
# requires-python = ">=3.12"
# dependencies = ["duckdb==1.5.6"]
# ///
"""Build the tables in DuckLake from raw/ with the statements in tables.sql.

Run from the directory that holds raw/. LAKE_CATALOG is the SQLite catalog file and LAKE_DATA the directory or
s3:// prefix for the Parquet files; readers see each table change as one snapshot, never a half-built table.

A statement marked `-- incremental: <glob> ...` reads `$files`, batches of files it has not read before, and appends
to its table; it starts over when the query changes, while a changed glob list only reads the new files. With
`-- merge: <column>, ...` as well, a row whose key is already in the table replaces the old row instead. A table
marked `-- partition: <expr>, ...` is created empty, partitioned by those expressions, and then filled.
Every other table is rebuilt only when its statement changed, the files its globs match changed, or a table it
reads was rebuilt in this run. A statement that reads no files and no tables runs every time.

A failed statement is printed and skipped; the exit status is 1 if any failed for a reason other than a source
without files or a table missing because of one.
"""

import glob
import hashlib
import os
import re
import resource
import sys
import time

import duckdb

from sync import secret

SQL =os.environ.get("TABLES_SQL", os.path.join(os.path.dirname(os.path.abspath(__file__)), "tables.sql"))
CATALOG = os.environ.get("LAKE_CATALOG", "lake.sqlite")
DATA = os.environ.get("LAKE_DATA", "lake/")
BATCH_BYTES = 64 << 20
MARKER = re.compile(r"^-- incremental: (.+)$", re.M)
PARTITION = re.compile(r"^-- partition: (.+)$", re.M)
MERGE = re.compile(r"^-- merge: (.+)$", re.M)
CREATE = re.compile(r"CREATE OR REPLACE TABLE (\S+) AS\s+(.*)", re.S)
COMMENT = re.compile(r"^COMMENT ON TABLE (\S+) IS", re.M)
GLOB = re.compile(r"'((?:raw|duckdb)/[^']+)'")
VARIABLE = re.compile(r"^SET VARIABLE (\w+)", re.M)
MACRO = re.compile(r"^CREATE OR REPLACE MACRO (\w+)", re.M)


def statements(sql: str) -> list[str]:
    return [s.strip() for s in re.split(r";\s*$", sql, flags=re.M) if s.strip()]


def digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def batches(files: list[str]):
    batch, size = [], 0
    for f in files:
        if batch and size + os.path.getsize(f) > BATCH_BYTES:
            yield batch
            batch, size = [], 0
        batch.append(f)
        size += os.path.getsize(f)
    if batch:
        yield batch


def sql_list(files: list[str]) -> str:
    return "[" + ", ".join("'" + f.replace("'", "''") + "'" for f in files) + "]"


def uses(stmt: str, name: str) -> bool:
    """Whether `stmt` reads the variable (`getvariable('<name>')`) or calls the macro (`<name>(`)."""
    return f"getvariable('{name}')" in stmt or bool(re.search(rf"(?<![\w.]){name}\(", stmt))


def globs(stmt: str, names: dict[str, list[str]]) -> list[str]:
    """The globs `stmt` names, directly or through a variable or macro it uses."""
    found = GLOB.findall(stmt)
    for name, patterns in names.items():
        if uses(stmt, name):
            found += patterns
    return sorted(set(found))


def inputs(patterns: list[str]) -> str:
    """Count, total size and newest change of the files each glob matches."""
    parts = []
    for pattern in patterns:
        stats = [os.stat(f) for f in glob.glob(pattern, recursive=True)]
        newest = max((s.st_mtime_ns for s in stats), default=0)
        parts.append(f"{pattern}:{len(stats)}:{sum(s.st_size for s in stats)}:{newest}")
    return digest("\n".join(parts))


def reads(stmt: str, tables: list[str]) -> list[str]:
    """The tables built earlier in this file that `stmt` names outside strings and comments."""
    # One pass, so an apostrophe in a comment does not open a string and a -- in a string does not open a comment.
    code = re.sub(r"--[^\n]*|'(?:[^']|'')*'", lambda m: "''" if m.group(0).startswith("'") else "", stmt)
    return [t for t in tables if re.search(rf"(?<![\w.]){re.escape(t)}(?![\w(])", code)]


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    # read_json can type a timestamp with an offset as one without; cast to TIMESTAMPTZ, that is then read as UTC.
    con.execute("SET TimeZone = 'UTC'")
    # The build reads each table's Parquet files from the bucket once; caching them only takes the memory it needs.
    con.execute("SET enable_external_file_cache = false")
    # Home, for km_from_home in tables.sql, from the environment so it stays out of the code.
    # Without them, distances from home are NULL.
    for name in ("HOME_LAT", "HOME_LON"):
        if value := os.environ.get(name):
            con.execute(f"SET VARIABLE {name.lower()} = {float(value)}")
    # The build may use more than the queries the server answers meanwhile: DUCKDB_BUILD_* wins over DUCKDB_*.
    for key in ("memory_limit", "threads"):
        if value := os.environ.get("DUCKDB_BUILD_" + key.upper()) or os.environ.get("DUCKDB_" + key.upper()):
            con.execute(f"SET {key} = '{value}'")
    if DATA.startswith("s3://"):
        con.execute(secret())
    con.execute(f"ATTACH 'ducklake:sqlite:{CATALOG}' AS lake (DATA_PATH '{DATA}'); USE lake")
    con.execute("CREATE SCHEMA IF NOT EXISTS staging")
    con.execute("CREATE TABLE IF NOT EXISTS staging._ingested (tbl VARCHAR, digest VARCHAR, filename VARCHAR)")
    con.execute("CREATE TABLE IF NOT EXISTS staging._built (tbl VARCHAR, digest VARCHAR, inputs VARCHAR)")
    # A partitioned table whose fill failed leaves its __new work table behind.
    for schema, table in con.execute(
        "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'lake' AND table_name LIKE '%\\_\\_new' ESCAPE '\\'"
    ).fetchall():
        con.execute(f"DROP TABLE {schema}.{table}")
    return con


def exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    found = con.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND "
        "(table_name = ? OR schema_name || '.' || table_name = ?)",
        [name, name],
    ).fetchall()
    return found[0][0] > 0


def create(con: duckdb.DuckDBPyConnection, stmt: str, name: str, body: str) -> None:
    """Replace the table; with a partition marker, create it empty and partitioned first, then fill it."""
    if not (m := PARTITION.search(stmt)):
        con.execute(stmt)
        return
    work = name + "__new"
    con.execute(f"DROP TABLE IF EXISTS {work}")
    con.execute(f"CREATE TABLE {work} AS SELECT * FROM ({body}) LIMIT 0")
    con.execute(f"ALTER TABLE {work} SET PARTITIONED BY ({m.group(1)})")
    con.execute(f"INSERT INTO {work} {body}")
    con.begin()
    con.execute(f"DROP TABLE IF EXISTS {name}")
    con.execute(f"ALTER TABLE {work} RENAME TO {name.split('.')[-1]}")
    con.commit()


def incremental(con: duckdb.DuckDBPyConnection, stmt: str, patterns: list[str]) -> bool:
    """Append the files not read before. Returns whether the table changed."""
    if not (m := CREATE.search(stmt)):
        raise ValueError(f"incremental statement is not CREATE OR REPLACE TABLE ... AS: {stmt[:60]}")
    name, body = m.groups()
    key = digest(m.group(0))
    stale = con.execute(
        "SELECT count(*) FROM staging._ingested WHERE tbl = ? AND digest <> ?", [name, key]
    ).fetchall()[0][0]
    if stale:
        con.execute(f"DROP TABLE IF EXISTS {name}")
        con.execute("DELETE FROM staging._ingested WHERE tbl = ?", [name])
    seen = {r[0] for r in con.execute("SELECT filename FROM staging._ingested WHERE tbl = ?", [name]).fetchall()}
    if not seen:
        con.execute(f"DROP TABLE IF EXISTS {name}")  # built in full before, or never
    files = sorted({f for p in patterns for f in glob.glob(p, recursive=True)} - seen)
    keys = [k.strip() for k in merge.group(1).split(",")] if (merge := MERGE.search(stmt)) else []
    for batch in batches(files):
        query = body.replace("$files", sql_list(batch))
        if not exists(con, name):
            partitioned = PARTITION.search(stmt)
            con.execute(f"CREATE TABLE {name} AS SELECT * FROM ({query}) LIMIT 0")
            if partitioned:
                con.execute(f"ALTER TABLE {name} SET PARTITIONED BY ({partitioned.group(1)})")
        con.begin()
        if keys:
            # Later files win: a row whose key is already in the table replaces it.
            on = " AND ".join(f"t.{k} IS NOT DISTINCT FROM s.{k}" for k in keys)
            con.execute(
                f"MERGE INTO {name} AS t USING ({query}) AS s ON {on} "
                "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
            )
        else:
            con.execute(f"INSERT INTO {name} BY NAME {query}")
        con.executemany("INSERT INTO staging._ingested VALUES (?, ?, ?)", [[name, key, f] for f in batch])
        con.commit()
    print(f"{name}: {len(files)} new files", file=sys.stderr)
    return bool(files or stale)


def main() -> int:
    con = connect()
    built = {r[0]: (r[1], r[2]) for r in con.execute("SELECT tbl, digest, inputs FROM staging._built").fetchall()}
    tables: list[str] = []
    names: dict[str, list[str]] = {}  # variables and macros, with the globs they read
    definitions: dict[str, str] = {}  # their statements, with those of the variables and macros they use
    rebuilt: set[str] = set()
    failed = False
    with open(SQL) as f:
        sql = f.read()
    for stmt in statements(sql):
        started = time.monotonic()
        create_m, comment_m = CREATE.search(stmt), COMMENT.search(stmt)
        name = create_m.group(1) if create_m else comment_m.group(1) if comment_m else None
        try:
            # Macros live in this session only, so they never add a snapshot to the catalog.
            text = stmt.replace("CREATE OR REPLACE MACRO", "CREATE OR REPLACE TEMP MACRO")
            if (m := MARKER.search(stmt)) and name:
                if incremental(con, text, m.group(1).split()):
                    rebuilt.add(name)
            elif create_m and name:
                patterns = globs(stmt, names)
                # A changed macro or variable changes the key of every table that uses it.
                used = "".join(text for n, text in definitions.items() if uses(stmt, n))
                key, seen = digest(stmt + used), inputs(patterns)
                deps = reads(stmt, tables)
                static = not patterns and not deps
                # A table built by two statements, the second reading the first, keeps a record per statement.
                record = f"{name}#{tables.count(name)}"
                if static or built.get(record) != (key, seen) or rebuilt & set(deps) or not exists(con, name):
                    create(con, text, name, create_m.group(2))
                    con.execute("DELETE FROM staging._built WHERE tbl = ?", [record])
                    con.execute("INSERT INTO staging._built VALUES (?, ?, ?)", [record, key, seen])
                    rebuilt.add(name)
            elif comment_m and name:
                key = digest(stmt)
                if name in rebuilt or built.get("comment:" + name, (None,))[0] != key:
                    con.execute(text)
                    con.execute("DELETE FROM staging._built WHERE tbl = ?", ["comment:" + name])
                    con.execute("INSERT INTO staging._built VALUES (?, ?, '')", ["comment:" + name, key])
            else:
                if v := VARIABLE.search(stmt) or MACRO.search(stmt):
                    names[v.group(1)] = globs(stmt, names)
                    definitions[v.group(1)] = stmt + "".join(
                        text for n, text in definitions.items() if uses(stmt, n)
                    )
                con.execute(text)
        except duckdb.Error as e:
            try:
                con.rollback()
            except duckdb.Error:
                pass  # no transaction was open
            # A source without files yet fails its table, and then the tables and comments built on it.
            if "No files found" not in str(e) and "does not exist" not in str(e):
                failed = True
            print(f"{name or stmt[:60]}: {str(e).splitlines()[0]}", file=sys.stderr)
        if create_m and name:
            tables.append(name)
        if (took := time.monotonic() - started) > 5:
            peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # KB on Linux, bytes on macOS
            print(f"  {took:.0f}s, peak {peak >> 10} MB: {stmt.splitlines()[-1][:80]}", file=sys.stderr, flush=True)
    print(f"rebuilt {len(rebuilt)} of {len(tables)} tables: {', '.join(sorted(rebuilt))}", file=sys.stderr)
    compact(con)
    con.close()
    return 1 if failed else 0


def compact(con: duckdb.DuckDBPyConnection) -> None:
    """Once a week, merge small files and delete the files of snapshots older than a week.

    Every rebuilt table and every incremental batch leaves files behind, so without this the bucket only grows.
    """
    last = con.execute("SELECT max(inputs) FROM staging._built WHERE tbl = 'compacted'").fetchall()[0][0]
    if last and time.time() - float(last) < 7 * 86400:
        return
    started = time.monotonic()
    con.execute("CALL ducklake_merge_adjacent_files('lake')")
    con.execute("CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL 7 DAY)")
    con.execute("CALL ducklake_cleanup_old_files('lake', older_than => now() - INTERVAL 7 DAY)")
    con.execute("DELETE FROM staging._built WHERE tbl = 'compacted'")
    con.execute("INSERT INTO staging._built VALUES ('compacted', '', ?)", [str(time.time())])
    print(f"compacted in {time.monotonic() - started:.0f}s", file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
