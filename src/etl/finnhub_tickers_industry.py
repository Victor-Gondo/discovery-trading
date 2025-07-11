```python
# scripts/finnhub_tickers_industry.py

import os
import time
import json
import pandas as pd
from finnhub import Client
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

API_KEY = os.getenv("FINNHUB_KEY")
if not API_KEY:
    raise RuntimeError("Please set FINNHUB_KEY in your environment")

# Initialize Finnhub client
client = Client(api_key=API_KEY)

# Minimum market capitalization threshold (in USD million)
MIN_MARKET_CAP = 2_000_000  # 2 million


def fetch_all_symbols(exchange: str = "US") -> pd.DataFrame:
    """
    Get list of all symbols on a given exchange.
    """
    print(f"[SYMBOLS] Fetching all symbols for exchange '{exchange}'...")
    symbols = client.stock_symbols(exchange)
    df = pd.DataFrame(symbols)
    print(f"[SYMBOLS] Retrieved {len(df)} symbols.")
    return df


def fetch_industries_and_filter(
    df_symbols: pd.DataFrame,
    pause: float = 1.0,
    save_every: int = 500
) -> pd.DataFrame:
    """
    For each symbol, call company_profile2 to get industry and market cap.
    Only keep those with market cap >= MIN_MARKET_CAP.
    Saves partial results every `save_every` records to guard against crashes.
    """
    records = []
    total = len(df_symbols)
    print(f"[INDUSTRY] Fetching industries for {total} symbols...")
    for idx, row in df_symbols.iterrows():
        sym = row["symbol"]
        try:
            prof = client.company_profile2(symbol=sym)
            market_cap = prof.get("marketCapitalization") or 0
            industry = prof.get("finnhubIndustry")
            print(f"[INDUSTRY] {idx+1}/{total}  {sym} → cap={market_cap}, industry={industry}")
            # Filter by market cap threshold
            if market_cap >= MIN_MARKET_CAP:
                records.append({
                    "symbol":             sym,
                    "industry":           industry,
                    "marketCapitalization": market_cap,
                    "name":               prof.get("name"),
                    "exchange":           prof.get("exchange"),
                    "weburl":             prof.get("weburl"),
                })
        except Exception as e:
            print(f"[ERROR] {sym}: {e}")
        # Save partial every N symbols
        if (idx + 1) % save_every == 0:
            partial_df = pd.DataFrame(records)
            csv_path = f"data/symbols_industry_partial_{idx+1}.csv"
            json_path = f"data/symbols_industry_partial_{idx+1}.json"
            print(f"[SAVE] Writing partial results to {csv_path} and {json_path}")
            partial_df.to_csv(csv_path, index=False)
            with open(json_path, "w") as jf:
                json.dump(partial_df.to_dict(orient="records"), jf, indent=2)
        time.sleep(pause)
    print(f"[INDUSTRY] Completed fetching and filtering industries.")
    return pd.DataFrame(records)


def main():
    out_dir = "data"
    os.makedirs(out_dir, exist_ok=True)

    # 1. Fetch all symbols and save raw
    df_syms = fetch_all_symbols("US")
    syms_csv = os.path.join(out_dir, "all_us_symbols.csv")
    syms_json = os.path.join(out_dir, "all_us_symbols.json")
    print(f"[SAVE] Writing symbols to {syms_csv} and {syms_json}")
    df_syms.to_csv(syms_csv, index=False)
    with open(syms_json, "w") as f:
        json.dump(df_syms.to_dict(orient="records"), f, indent=2)

    # 2. Fetch industries, filter by market cap, with logging and partial saves
    try:
        df_ind = fetch_industries_and_filter(df_syms)
    except Exception as e:
        print(f"[FATAL] Error during industry fetch: {e}")
        # Save what we have so far
        fallback_df = pd.DataFrame(locals().get('records', []))
        fallback_csv = os.path.join(out_dir, "symbols_industry_fallback.csv")
        print(f"[SAVE] Writing fallback results to {fallback_csv}")
        fallback_df.to_csv(fallback_csv, index=False)
        raise

    # 3. Save full industry data
    industries_csv = os.path.join(out_dir, "symbols_industry.csv")
    industries_json = os.path.join(out_dir, "symbols_industry.json")
    print(f"[SAVE] Writing industries to {industries_csv} and {industries_json}")
    df_ind.to_csv(industries_csv, index=False)
    with open(industries_json, "w") as f:
        json.dump(df_ind.to_dict(orient="records"), f, indent=2)

    # 4. Filter for healthcare-related industries from the filtered set
    keywords = ["health", "pharma", "biotech", "medical"]
    mask = df_ind["industry"].str.lower().str.contains("|".join(keywords), na=False)
    df_health = df_ind[mask]
    health_csv = os.path.join(out_dir, "healthcare_symbols.csv")
    print(f"[SAVE] Writing {len(df_health)} healthcare symbols to {health_csv}")
    df_health.to_csv(health_csv, index=False)

    print("[DONE] All tasks completed successfully.")


if __name__ == "__main__":
    main()
