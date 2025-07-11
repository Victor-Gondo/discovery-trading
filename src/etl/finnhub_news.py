# src/etl/finnhub_news.py

import os
import time
import argparse
from datetime import datetime, timedelta

import finnhub
import pandas as pd
from dotenv import load_dotenv
load_dotenv()    # this will read .env from your cwd and set os.environ


def fetch_news(
    symbol: str,
    from_date: str,
    to_date: str,
    pause: float = 1.0
) -> pd.DataFrame:
    """
    Fetch company news for `symbol` between ISO dates, respecting rate limits.

    Args:
        symbol: e.g. "AAPL"
        from_date: "YYYY-MM-DD"
        to_date:   "YYYY-MM-DD"
        pause: seconds to sleep after each API call (<=60/min & <=30/sec)

    Returns:
        DataFrame with all returned fields + a 'symbol' column.
    """
    api_key = os.getenv("FINNHUB_KEY")
    if not api_key:
        raise RuntimeError("Please set FINNHUB_KEY in your environment")

    client = finnhub.Client(api_key=api_key)
    data = client.company_news(symbol, _from=from_date, to=to_date)
    # throttle: 1s between calls → <=60/min and <=30/sec
    time.sleep(pause)

    df = pd.DataFrame(data)
    df["symbol"] = symbol
    return df


def main(tickers: list[str], days: int = 365):
    """
    ETL entrypoint: fetch news for each ticker over the past `days` days,
    concatenate, and write to data/news.parquet.
    """
    end = datetime.utcnow()
    start = end - timedelta(days=days)

    dfs = []
    for sym in tickers:
        df = fetch_news(
            sym,
            from_date=start.date().isoformat(),
            to_date=end.date().isoformat(),
            pause=1.0,
        )
        dfs.append(df)

    if not dfs:
        print("No tickers specified; nothing to do.")
        return

    all_news = pd.concat(dfs, ignore_index=True)
    out_dir = os.path.join("data")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "news.parquet")
    all_news.to_parquet(out_path, index=False)

    print(f"Fetched {len(all_news)} news items → {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch company news via Finnhub."
    )
    parser.add_argument(
        "--tickers", "-t",
        nargs="+",
        required=True,
        help="Ticker symbols, e.g. AAPL MSFT"
    )
    parser.add_argument(
        "--days", "-d",
        type=int,
        default=365,
        help="History length in days"
    )
    args = parser.parse_args()
    main(args.tickers, args.days)
