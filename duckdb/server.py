# /// script
# requires-python = ">=3.12"
# dependencies = ["duckdb==1.5.6", "httpx==0.28.1", "mcp==2.3.0", "pytz==2026.5", "uvicorn==0.37.0", "webauthn==3.0.1"]
# ///
"""Serve the DuckLake tables as a remote MCP server and update them as raw files arrive.

Every route sits under /<MCP_TOKEN>/, so the URL is the credential:
- /<token>/mcp: MCP over stateless streamable HTTP, tools `tables` and `query`.
- POST /<token>/ingest: the collector names the files it stored in the bucket. Download them into the local
  copy of raw/ and run build.py in the background; answer 202 at once.
- POST /<token>/rebuild: list the bucket for missing files, then run build.py, and answer when it is done.
- POST /sql: one read-only query (the body) as JSON rows, with `Authorization: Bearer <MCP_TOKEN>`; Grafana's
  data source.
- /map and /grafana: a map and Grafana behind a passkey login; see web.py.

The process exits after IDLE_SECONDS without requests or builds, which stops the machine; the next request
starts it again.
"""

import asyncio
import os
import subprocess
import sys
import threading
import time
from contextlib import asynccontextmanager

import duckdb
import httpx
import uvicorn
from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.routing import Mount, Route

import sync
import web

TOKEN = os.environ["MCP_TOKEN"]
CATALOG = os.environ.get("LAKE_CATALOG", "/data/lake.sqlite")
LAKE_DATA = os.environ.get("LAKE_DATA", f"s3://{sync.BUCKET}/lake/")
ROOT = sync.ROOT
REPO = os.environ.get("GITHUB_REPO", "knyghty/personal-data")
BRANCH = os.environ.get("GITHUB_BRANCH", "main")
IDLE_SECONDS = int(os.environ.get("IDLE_SECONDS", "300"))
RECONCILE_SECONDS = 24 * 3600
QUERY_TIMEOUT = 120
MAX_CHARS = 60_000
SQL_MAX_ROWS = 100_000

mcp = MCPServer(
    "personal-data",
    instructions=(
        "The user's personal data: Withings (body, sleep, heart, activity), Apple Health, Home Assistant, weather "
        "and air quality, bunq banking, Albert Heijn receipts, Steam and GitHub activity. Call `tables` first to see what exists, then "
        "`query` with DuckDB SQL. Timestamps are TIMESTAMPTZ in UTC; the user lives in Europe/Amsterdam, so use "
        "AT TIME ZONE 'Europe/Amsterdam' for local days and hours. Aggregate in SQL rather than fetching raw rows. "
        "The raw JSON the tables come from can be read with read_json('raw/<source>/<type>/**/*.json') when a "
        "table lacks a field."
    ),
)


def connect() -> duckdb.DuckDBPyConnection:
    """A database of its own for one query, with the file attached read-only.

    Opening the file itself would share one database per file within the process: settings locked by one query
    then fail the next, and the shared database keeps serving the file a rebuild has replaced. An in-memory
    database per query avoids both; attaching reads only the file's catalogue.
    """
    # Relative paths such as raw/... are checked against allowed_directories from the working directory.
    if os.getcwd() != ROOT and os.path.isdir(ROOT):
        os.chdir(ROOT)
    # ponytail: each query may use the full memory limit, so many heavy queries at once could exhaust the machine;
    # share a limit between them if that happens.
    con = duckdb.connect(
        ":memory:",
        config={
            "memory_limit": os.environ.get("DUCKDB_MEMORY_LIMIT", "1GB"),
            "threads": os.environ.get("DUCKDB_THREADS", "2"),
            "file_search_path": ROOT,
        },
    )
    if LAKE_DATA.startswith("s3://"):
        con.execute(sync.secret())
    con.execute(f"ATTACH 'ducklake:sqlite:{CATALOG}' AS personal (READ_ONLY); USE personal")
    # Files outside raw/ and the lake stay unreadable, and queries cannot change the settings back.
    con.execute(f"SET allowed_directories = ['{ROOT}/raw/', '{LAKE_DATA}', '{CATALOG}']")
    con.execute("SET TimeZone = 'UTC'; SET enable_external_access = false; SET lock_configuration = true")
    return con


@mcp.tool()
def tables() -> str:
    """List every table with its description and columns, and when the data was last built."""
    with connect() as con:
        built = con.execute("SELECT max(built_at) FROM build_info").fetchall()[0][0]
        rows = con.execute(
            """
            SELECT t.table_name, coalesce(t.comment, ''), t.estimated_size,
                   string_agg(c.column_name || ' ' || lower(c.data_type), ', ' ORDER BY c.column_index)
            FROM duckdb_tables() t JOIN duckdb_columns() c USING (database_name, schema_name, table_name)
            WHERE t.database_name = 'personal' AND t.schema_name = 'main'
            GROUP BY ALL ORDER BY 1
            """
        ).fetchall()
    lines = [f"Built at {built} UTC.", ""]
    for name, comment, size, columns in rows:
        lines.append(f"{name} (~{size} rows){': ' + comment if comment else ''}\n  {columns}")
    return "\n".join(lines)


@mcp.tool()
def query(sql: str, max_rows: int = 200) -> str:
    """Run one read-only DuckDB SQL query and return the result as tab-separated text.

    Returns at most `max_rows` rows (and about 60,000 characters); say so and aggregate instead of
    raising it when you need many rows. Queries are cancelled after two minutes.
    """
    with connect() as con:
        timer = threading.Timer(QUERY_TIMEOUT, con.interrupt)
        timer.start()
        try:
            cur = con.execute(sql)
            if cur.description is None:
                return "OK"
            header = "\t".join(d[0] for d in cur.description)
            rows = cur.fetchmany(max_rows + 1)
        except duckdb.Error as e:
            return f"Error: {e}"
        finally:
            timer.cancel()
    out = [header] + ["\t".join("" if v is None else str(v) for v in row) for row in rows[:max_rows]]
    text = "\n".join(out)
    if len(text) > MAX_CHARS:
        text = text[:MAX_CHARS] + "\n… output cut at 60,000 characters"
    elif len(rows) > max_rows:
        text += f"\n… more than {max_rows} rows; only the first {max_rows} shown"
    return text


build_lock = threading.Lock()
pending: set[str] = set()
pending_lock = threading.Lock()
work = threading.Event()
last_request = time.monotonic()


def fetch_sql() -> None:
    """Fetch tables.sql and the files it reads into ROOT/duckdb from the branch, so a push changes the tables
    without a deploy. A failed fetch keeps the last copy, or the one bundled in the image."""
    here = os.path.dirname(os.path.abspath(__file__))
    os.makedirs(os.path.join(ROOT, "duckdb"), exist_ok=True)
    for name in ("tables.sql", "watchlist.json"):
        path = os.path.join(ROOT, "duckdb", name)
        try:
            r = httpx.get(
                f"https://api.github.com/repos/{REPO}/contents/duckdb/{name}?ref={BRANCH}",
                headers={"Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}", "Accept": "application/vnd.github.raw"},
                timeout=30,
            )
            r.raise_for_status()
            with open(path + ".tmp", "w") as f:
                f.write(r.text)
            os.replace(path + ".tmp", path)
        except (httpx.HTTPError, KeyError, OSError) as e:
            print(f"{name}: keeping the last copy: {e}", file=sys.stderr, flush=True)
            if not os.path.exists(path):
                with open(os.path.join(here, name)) as src, open(path, "w") as dst:
                    dst.write(src.read())


def build_sync(paths: list[str], reconcile: bool = False) -> tuple[int, str]:
    """Download `paths`, list the bucket when asked or once a day, then run build.py from ROOT."""
    with build_lock:
        con = sync.connect()
        got = sync.download(con, sync.keys(paths))
        marker = os.path.join(ROOT, ".reconciled")
        due = not os.path.exists(marker) or time.time() - os.path.getmtime(marker) > RECONCILE_SECONDS
        if reconcile or due:
            got += sync.reconcile(con)
            with open(marker, "w"):
                pass
        con.close()
        print(f"sync: {got} files downloaded", file=sys.stderr, flush=True)
        # The build's output goes straight to the machine's log, so a build that is killed still shows how far it got.
        fetch_sql()
        env = {
            **os.environ,
            "TABLES_SQL": os.path.join(ROOT, "duckdb", "tables.sql"),
            "LAKE_CATALOG": CATALOG,
            "LAKE_DATA": LAKE_DATA,
        }
        build = os.path.join(os.path.dirname(os.path.abspath(__file__)), "build.py")
        r = subprocess.run([sys.executable, build], cwd=ROOT, env=env)
    return r.returncode, "build failed, see the log\n" if r.returncode else "ok\n"


def worker() -> None:
    """Build whenever ingest calls queued files; calls that arrive during a build share the next one.

    The first build after a start lists the bucket, for files whose ingest call came while the server was down.
    """
    first = True
    while True:
        work.wait()
        work.clear()
        with pending_lock:
            paths = sorted(pending)
            pending.clear()
        try:
            build_sync(paths, reconcile=first)
            first = False
        except Exception as e:  # noqa: BLE001 - keep the worker alive; the next ingest retries
            print(f"ingest: {e}", file=sys.stderr, flush=True)


def busy() -> bool:
    return build_lock.locked() or work.is_set() or bool(pending)


def sql_rows(sql: str) -> list[dict]:
    with connect() as con:
        timer = threading.Timer(QUERY_TIMEOUT, con.interrupt)
        timer.start()
        try:
            cur = con.execute(sql)
            if cur.description is None:
                return []
            cols = [d[0] for d in cur.description]
            rows = cur.fetchmany(SQL_MAX_ROWS)
        finally:
            timer.cancel()
    return [{c: web.plain(v) for c, v in zip(cols, row)} for row in rows]


async def sql(request: Request) -> Response:
    """One read-only query from the body (plain SQL) as JSON rows, for Grafana. Bearer MCP_TOKEN."""
    if request.headers.get("authorization") != f"Bearer {TOKEN}":
        return PlainTextResponse("unauthorized\n", status_code=401)
    text = (await request.body()).decode()
    try:
        return JSONResponse(await asyncio.to_thread(sql_rows, text))
    except duckdb.Error as e:
        return JSONResponse({"error": str(e)}, status_code=400)


async def rebuild(request: Request) -> PlainTextResponse:
    code, log = await asyncio.to_thread(build_sync, [], True)
    return PlainTextResponse(log, status_code=500 if code else 200)


async def ingest(request: Request) -> Response:
    """Queue the files the collector stored, as `{"message": ..., "paths": [...]}`, and build in the background."""
    body = await request.json()
    paths = [p for p in body.get("paths", []) if isinstance(p, str)]
    with pending_lock:
        pending.update(paths)
    work.set()
    print(f"ingest: {len(paths)} files: {body.get('message', '')}", file=sys.stderr, flush=True)
    return PlainTextResponse("queued\n", status_code=202)


class Activity:
    """Remember when the last request came in, for the idle exit."""

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        global last_request
        last_request = time.monotonic()
        await self.app(scope, receive, send)
        last_request = time.monotonic()


server: uvicorn.Server | None = None


def watchdog() -> None:
    """Exit once the server has been idle for IDLE_SECONDS, so the machine stops; requests start it again."""
    while True:
        time.sleep(15)
        if server and not busy() and time.monotonic() - last_request > IDLE_SECONDS:
            print("idle, exiting", file=sys.stderr, flush=True)
            server.should_exit = True
            return


@asynccontextmanager
async def lifespan(app: Starlette):
    threading.Thread(target=worker, daemon=True).start()
    threading.Thread(target=watchdog, daemon=True).start()
    # Catch up on files that arrived while the machine was stopped or restarting.
    work.set()
    async with mcp.session_manager.run():
        yield


mcp_app = mcp.streamable_http_app(
    stateless_http=True,
    json_response=True,
    # The secret path is the credential; the Host check is for servers bound to localhost.
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)
app = Starlette(
    routes=[
        Route("/health", lambda request: PlainTextResponse("ok")),
        Route(f"/{TOKEN}/rebuild", rebuild, methods=["POST"]),
        Route(f"/{TOKEN}/ingest", ingest, methods=["POST"]),
        Route("/sql", sql, methods=["POST"]),
        *web.auth_routes(TOKEN),
        *web.map_routes(connect),
        Mount(f"/{TOKEN}", app=mcp_app),  # serves /<token>/mcp
    ],
    lifespan=lifespan,
)

if __name__ == "__main__":
    config = uvicorn.Config(
        Activity(app), host="0.0.0.0", port=int(os.environ.get("PORT", "8080")), log_level="warning"
    )
    server = uvicorn.Server(config)
    server.run()
