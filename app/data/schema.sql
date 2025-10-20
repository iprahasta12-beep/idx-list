CREATE TABLE IF NOT EXISTS prices (
    symbol TEXT NOT NULL,
    ts_utc INTEGER NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    PRIMARY KEY(symbol, ts_utc)
);

CREATE INDEX IF NOT EXISTS idx_prices_symbol_ts ON prices(symbol, ts_utc DESC);

CREATE TABLE IF NOT EXISTS indicators (
    symbol TEXT NOT NULL,
    ts_utc INTEGER NOT NULL,
    ma20 REAL,
    ma50 REAL,
    rsi14 REAL,
    is_30d_high INTEGER,
    signal INTEGER,
    updated_at_utc INTEGER,
    PRIMARY KEY(symbol, ts_utc)
);

CREATE INDEX IF NOT EXISTS idx_indicators_symbol_ts ON indicators(symbol, ts_utc DESC);
