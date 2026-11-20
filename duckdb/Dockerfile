FROM ghcr.io/astral-sh/uv:0.9.15-python3.12-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY server.py build.py sync.py web.py tables.sql watchlist.json home.html map.html login.html passkey.html ./
# The dependencies are pinned in server.py's script header.
RUN uv export --script server.py --no-hashes --no-header > requirements.txt && uv pip install --system -r requirements.txt
# Install the extensions at build time, so a cold start downloads nothing.
RUN python -c "import duckdb; [duckdb.execute(f'INSTALL {e}') for e in ('ducklake', 'sqlite', 'httpfs', 'excel')]"
CMD ["python", "server.py"]
