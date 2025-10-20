# IDX Watchlist

A minimal TradingView-style web application for Indonesian equities (IDX) built with FastAPI, HTMX, and Tailwind CSS. It fetches hourly Yahoo Finance prices, computes daily technical indicators (MA20/MA50, RSI14, recent 30-day highs), and surfaces a sortable/filterable dashboard optimized for professional monitoring.

## Features

- ✅ Hourly ingestion of Yahoo Finance daily and 60-minute candles (Asia/Jakarta schedule).
- ✅ SQLite storage by default with CSV fallback for lightweight deployments.
- ✅ Daily indicators: MA20/MA50, RSI14 (Wilder), rolling 30-day highs, and composite trade signal.
- ✅ FastAPI REST API with health, tickers, summary, and per-symbol history endpoints.
- ✅ Responsive Tailwind table enhanced with HTMX for searching, sorting, and pagination (50 rows/page).
- ✅ APScheduler integration (optional) for autonomous hourly refreshes plus CLI utilities for seeding, one-off fetches, and historical backfills.
- ✅ Dockerized runtime and traditional Python entrypoints.

## Project layout

```
app/
  api/                # FastAPI router definitions
  core/               # Settings, logging, scheduler
  data/               # Repository layer (SQLite/CSV) and schema
  services/           # Yahoo fetcher, indicator math, aggregation orchestration
  web/                # Jinja templates, Tailwind styles, HTMX-enabled UI
config/tickers.json   # Default IDX ticker universe
manage.py             # Click-based CLI (seed/fetch/backfill)
main.py               # FastAPI application factory
requirements.txt      # Python dependencies
```

## Prerequisites

- Python 3.11+
- `pip` (or [`uv`](https://github.com/astral-sh/uv))
- SQLite (bundled with Python) if using the default backend
- Optional: Docker and Docker Compose

## Environment configuration

Copy the example environment file and adjust as needed:

```bash
cp .env.example .env
```

| Variable | Description | Default |
| -------- | ----------- | ------- |
| `TZ` | Local timezone for scheduling and display | `Asia/Jakarta` |
| `STORAGE` | `sqlite` or `csv` backend | `sqlite` |
| `DB_PATH` | SQLite file location | `data/idx_quotes.db` |
| `CSV_DIR` | Directory for CSV mode | `data` |
| `TICKERS_PATH` | JSON array of ticker symbols | `config/tickers.json` |
| `RSI_MIN` | RSI threshold for the composite signal | `55` |
| `HIGH_LOOKBACK` | Days considered for 30-day high | `30` |
| `HIGH_WITHIN_DAYS` | Window to consider “recent” highs | `5` |
| `FETCH_INTERVAL_MIN` | Informational value for scheduling cadence | `60` |
| `ENABLE_SCHEDULER` | Enable APScheduler on app startup (`true`/`false`) | `false` |

### Managing tickers

Update `config/tickers.json` with the Yahoo Finance tickers you want to track (use the `.JK` suffix, e.g. `"BBCA.JK"`). Set `TICKERS_PATH` in the environment if you maintain an alternate list.

## Local development (non-Docker)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # adjust values if desired
```

Initialize storage, backfill historical data, and run the server:

```bash
python manage.py seed
python manage.py backfill --days 120
python manage.py fetch --once  # optional hourly snapshot
uvicorn main:app --host 0.0.0.0 --port 8000
```

Visit [http://localhost:8000](http://localhost:8000) for the HTMX-powered table. The `/api/*` endpoints are documented via the FastAPI schema at `/docs`.

## CLI utilities

- `python manage.py seed` – prepare the database/CSV files and validate ticker configuration.
- `python manage.py fetch --once` – pull the latest daily + 60m candles and recompute indicators (use `--no-intraday` to skip hourly candles).
- `python manage.py backfill --days 120` – historical refresh of the last _N_ trading days.

All commands honour the environment variables described above.

## Scheduler

Set `ENABLE_SCHEDULER=true` to start an APScheduler job at application boot. The job runs hourly at minute `:05` Asia/Jakarta and executes the same pipeline as `fetch --once`.

For environments where APScheduler is not desirable, configure a system cron job instead:

```
# /etc/cron.d/idx-watchlist
TZ=Asia/Jakarta
5 * * * * www-data cd /srv/idx-watchlist && /srv/idx-watchlist/.venv/bin/python manage.py fetch --once >> /var/log/idx-watchlist.log 2>&1
```

## Docker & Compose

Build and run with Docker:

```bash
docker build -t idx-watchlist .
docker run --rm -p 8000:8000 -e ENABLE_SCHEDULER=true -v $(pwd)/data:/app/data -v $(pwd)/config:/app/config idx-watchlist
```

Or use Docker Compose (bind-mounting data and config for persistence):

```bash
docker compose up --build
```

## Troubleshooting & Yahoo Finance notes

- Yahoo Finance rate-limits aggressively. The fetcher staggers requests with brief sleeps and retries with exponential backoff, but very large ticker universes may still hit throttling.
- Intraday (`60m`) candles can lag by several minutes. The pipeline drops any candle whose timestamp is in the future to avoid partial data.
- If RSI values appear stuck at `0`, ensure you have at least 14 historical data points per symbol (run a longer backfill).
- CSV mode is great for serverless targets or read-only hosts. Set `STORAGE=csv` and ensure the path pointed to by `CSV_DIR` is writable.

## Deployment tips

- Enable the scheduler (`ENABLE_SCHEDULER=true`) in containerized environments where a long-running worker is acceptable.
- For systemd deployments, create a unit file running `uvicorn` and pair it with the cron entry above for data refresh.
- Monitor logs via stdout (structured JSON) or pipe to your aggregation platform of choice.

## License

MIT
