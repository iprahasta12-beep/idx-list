# IDX Automation

IDX automation project that tracks selected Indonesia Stock Exchange (IDX) tickers, stores hourly price updates in CSV files, and produces daily technical indicator summaries. The workflow is orchestrated via GitHub Actions so data refreshes automatically every hour.

## Repository layout

```
idx-automation/
├── data/
│   ├── tickers.csv            # List of IDX tickers to monitor
│   ├── quotes.csv             # Historical hourly OHLCV data
│   ├── indicators.csv         # Daily technical indicators
│   └── snapshots/             # Time-stamped CSV snapshots of each run
├── automation/
│   ├── fetch_quotes.py         # Hourly data collection from Yahoo Finance
│   ├── calculate_indicators.py # Indicator calculations
│   └── utils.py                # Shared helpers (configuration, logging, etc.)
├── config/
│   └── settings.json          # Runtime configuration (intervals, timezone, retention)
├── logs/
│   └── automation.log         # Combined log output from automation scripts
├── requirements.txt            # Python dependencies
└── .github/workflows/
    └── update.yml             # GitHub Actions workflow that runs every hour
```

## Getting started locally

1. **Install dependencies**
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Set tickers**
   Update `data/tickers.csv` with the IDX tickers you want to track (one per line under the `TICKER` header).

3. **Run the automation scripts**
   ```bash
   python -m automation.fetch_quotes
   python -m automation.calculate_indicators
   ```
   Each script creates the required folders automatically, updates the master CSV files under `data/`, saves hourly/daily snapshots in `data/snapshots/`, and appends log output to `logs/automation.log`.

## GitHub Actions workflow

The workflow defined in `.github/workflows/update.yml` runs every hour using cron (`0 * * * *`). It:

1. Installs Python dependencies.
2. Executes the fetch and indicator scripts.
3. Commits newly generated CSV data and snapshots back to the repository.

You can also trigger the workflow manually through the **Run workflow** button in GitHub's Actions tab.

## Data outputs

- `data/quotes.csv`: consolidated hourly OHLCV records for each ticker.
- `data/indicators.csv`: daily snapshot of MA20, MA50, RSI14, MAX30, and other conditions.
- `data/snapshots/`: timestamped CSV exports produced on each run for auditing.

All timestamps follow the `YYYYMMDD_HHMM` format (Jakarta timezone by default) and no databases are used—only CSV storage.
