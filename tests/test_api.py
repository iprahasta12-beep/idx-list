import json
import sys
import importlib
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app.core.config import get_settings as load_settings
from app.data.models import IndicatorRow, PriceRow


@pytest.fixture()
def temp_env(tmp_path, monkeypatch):
    tickers_path = tmp_path / "tickers.json"
    tickers_path.write_text(json.dumps(["TEST.JK"]))
    monkeypatch.setenv("STORAGE", "sqlite")
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("TICKERS_PATH", str(tickers_path))
    monkeypatch.setenv("ENABLE_SCHEDULER", "false")
    load_settings.cache_clear()
    if "main" in sys.modules:
        importlib.reload(sys.modules["main"])
    else:
        importlib.import_module("main")
    module = sys.modules["main"]
    return module


def test_summary_endpoint(temp_env):
    module = temp_env
    repo = module.repository
    now = int(datetime.now(timezone.utc).timestamp())
    price_rows = [
        PriceRow(symbol="TEST.JK", ts_utc=now - 3600, open=10.0, high=11.0, low=9.5, close=10.5, volume=1000),
        PriceRow(symbol="TEST.JK", ts_utc=now, open=10.5, high=11.5, low=10.0, close=11.0, volume=1200),
    ]
    indicator_rows = [
        IndicatorRow(
            symbol="TEST.JK",
            ts_utc=now,
            ma20=10.7,
            ma50=10.2,
            rsi14=57.0,
            is_30d_high=1,
            signal=1,
            updated_at_utc=now,
        )
    ]
    repo.upsert_prices(price_rows)
    repo.upsert_indicators(indicator_rows)
    client = TestClient(module.app)
    response = client.get("/api/summary")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    row = payload[0]
    assert row["symbol"] == "TEST.JK"
    assert pytest.approx(row["last_close"], rel=1e-5) == 11.0
    assert row["signal"] is True
