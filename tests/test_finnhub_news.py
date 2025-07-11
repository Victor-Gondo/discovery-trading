# tests/test_finnhub_news.py

import os
import sys

# ensure `src/` is on the import path
sys.path.append("src")

import pandas as pd
import pytest

# import our module
import etl.finnhub_news as fn


class DummyClient:
    def __init__(self, api_key):
        # verify our test fixture is setting this
        assert api_key == "TESTKEY"

    def company_news(self, symbol, _from, to):
        # return exactly one fake item
        return [{
            "id": 123,
            "category": "company",
            "datetime": int(pd.Timestamp("2025-01-01T00:00:00Z").timestamp()),
            "headline": f"News for {symbol}",
            "related": symbol,
            "source": "test",
            "url": "http://example.com",
        }]


@pytest.fixture(autouse=True)
def env_and_client(monkeypatch):
    # set a dummy API key
    monkeypatch.setenv("FINNHUB_KEY", "TESTKEY")
    # patch finnhub.Client → our DummyClient
    monkeypatch.setattr(fn.finnhub, "Client", DummyClient)


def test_fetch_news_dataframe():
    df = fn.fetch_news("AAPL", "2025-01-01", "2025-01-02", pause=0)
    assert isinstance(df, pd.DataFrame)
    # ensure our dummy record shows up
    assert df.shape[0] == 1
    assert df.loc[0, "symbol"] == "AAPL"
    assert "headline" in df.columns


def test_main_writes_parquet(tmp_path, monkeypatch):
    # run in a clean temp dir
    monkeypatch.chdir(tmp_path)

    # stub out fetch_news to avoid real API calls
    sample = pd.DataFrame([{"id": 1, "symbol": "MSFT"}])
    monkeypatch.setattr(
        fn, "fetch_news",
        lambda symbol, from_date, to_date, pause: sample
    )

    # execute ETL
    fn.main(["MSFT"], days=1)

    out_file = tmp_path / "data" / "news.parquet"
    assert out_file.exists()

    result = pd.read_parquet(str(out_file))
    # should exactly match our sample
    pd.testing.assert_frame_equal(result, sample)


if __name__ == "__main__":
    pytest.main([__file__])
