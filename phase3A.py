"""
Phase 3A: Generate Dates Research Data Files
============================================
Reads Phase 2B t-test CSVs, enriches them with asset metadata and day-over-day
volume % changes, filters to statistically significant dates, and exports two
summary files to 'Dates Research/' for use in Perplexity news research.

Output files:
  - Dates Research/significant_events.csv   (one row per asset-date event)
  - Dates Research/dates_breadth_summary.csv (one row per date, breadth stats)
"""

import pandas as pd
import numpy as np
from pathlib import Path

# ── Constants ────────────────────────────────────────────────────────────────

T_THRESHOLD = 4.303  # 95% confidence interval, df=2 (same as Phase 2B)

TTEST_DIR = Path("output/Phase 2B/Daily Volume Analysis")
CHOSEN_DIR = Path("chosen")
OUTPUT_DIR = Path("output/Phase 3A")

# Full display names for each asset
FULL_NAMES = {
    "BTC":         "Bitcoin",
    "ETH":         "Ethereum",
    "SOL":         "Solana",
    "LINK":        "Chainlink",
    "ADA":         "Cardano",
    "NVDA":        "Nvidia",
    "TSLA":        "Tesla",
    "AAPL":        "Apple",
    "MSFT":        "Microsoft",
    "GOOGL":       "Alphabet (Google)",
    "META":        "Meta Platforms",
    "AMZN":        "Amazon",
    "COIN":        "Coinbase",
    "Gold":        "Gold",
    "Silver":      "Silver",
    "Oil":         "Crude Oil",
    "Natural Gas": "Natural Gas",
}

# chosen/ CSV filename for each asset class
CHOSEN_FILES = {
    "Crypto Coin":           "Crypto Coin.csv",
    "Traditional Equity":    "Traditional Equity.csv",
    "Traditional Commodity": "Traditional Commodity.csv",
}


# ── Step 1: Build asset metadata lookup ─────────────────────────────────────

def build_asset_meta() -> dict:
    """
    Returns a dict: asset_name -> {"yf_ticker": str, "asset_class": str}
    Built from the chosen/ CSVs, deduplicated by asset name.
    """
    meta = {}
    for asset_class, filename in CHOSEN_FILES.items():
        df = pd.read_csv(CHOSEN_DIR / filename)
        # Each asset appears once per DEX — keep first occurrence for the ticker
        for asset, group in df.groupby("asset"):
            meta[asset] = {
                "yf_ticker":   group["yf_ticker"].iloc[0],
                "asset_class": asset_class,
            }
    return meta


# ── Step 2: Process each t-test CSV ─────────────────────────────────────────

def compute_tradfi_pct_change(series: pd.Series) -> pd.Series:
    """
    Computes % change from the previous valid (non-NaN) TradFi trading day.
    Returns a Series aligned to the original index; NaN where tradfi_volume is NaN.
    """
    non_nan_idx = series.dropna().index
    pct = series.dropna().pct_change() * 100
    return pct.reindex(series.index)  # NaN on weekends/holidays


def process_asset(csv_path: Path, asset_meta: dict) -> pd.DataFrame:
    """
    Loads one t-test CSV, enriches it with metadata and % changes,
    filters to significant rows, and returns the result.
    """
    asset = csv_path.stem.replace("_daily_volume_ttest", "")

    df = pd.read_csv(csv_path, parse_dates=["date"])

    # Remove warm-up rows (insufficient rolling window data)
    df = df[df["defi_window_indices"] != "insufficient_data"].copy()

    # Day-over-day % change in DeFi volume (24/7, all rows have data)
    df["defi_volume_pct_change"] = df["defi_volume"].pct_change() * 100

    # Day-over-day % change in TradFi volume (skips weekends/holidays)
    df["tradfi_volume_pct_change"] = compute_tradfi_pct_change(df["tradfi_volume"])

    # Significance flags
    df["defi_significant"]  = df["defi_t_score"].abs()  >= T_THRESHOLD
    df["tradfi_significant"] = df["tradfi_t_score"].abs() >= T_THRESHOLD
    df["both_significant"]   = df["defi_significant"] & df["tradfi_significant"]

    # Filter to rows where at least one of defi or tradfi is significant
    df = df[df["defi_significant"] | df["tradfi_significant"]].copy()

    if df.empty:
        return df

    # Add metadata
    meta = asset_meta.get(asset, {"yf_ticker": "N/A", "asset_class": "Unknown"})
    df.insert(1, "asset",       asset)
    df.insert(2, "full_name",   FULL_NAMES.get(asset, asset))
    df.insert(3, "yf_ticker",   meta["yf_ticker"])
    df.insert(4, "asset_class", meta["asset_class"])

    # Keep only the columns we need
    keep = [
        "date", "asset", "full_name", "yf_ticker", "asset_class",
        "defi_volume", "defi_volume_pct_change", "defi_t_score",
        "tradfi_volume", "tradfi_volume_pct_change", "tradfi_t_score",
        "defi_significant", "tradfi_significant", "both_significant",
    ]
    return df[keep]


# ── Step 3: Build breadth summary ────────────────────────────────────────────

def build_breadth_summary(events: pd.DataFrame) -> pd.DataFrame:
    """
    Groups significant events by date and computes breadth statistics.
    """
    def summarize(group):
        assets_list = ", ".join(sorted(group["asset"].unique()))
        classes = ", ".join(sorted(group["asset_class"].unique()))
        return pd.Series({
            "num_assets_significant": len(group),
            "assets_list":            assets_list,
            "num_defi_only":          int((group["defi_significant"] & ~group["tradfi_significant"]).sum()),
            "num_tradfi_only":        int((~group["defi_significant"] & group["tradfi_significant"]).sum()),
            "num_both":               int(group["both_significant"].sum()),
            "asset_classes_affected": classes,
        })

    summary = (
        events.groupby("date")
        .apply(summarize, include_groups=False)
        .reset_index()
        .sort_values("num_assets_significant", ascending=False)
    )
    return summary


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    print("Building asset metadata lookup...")
    asset_meta = build_asset_meta()
    print(f"  Found {len(asset_meta)} assets: {sorted(asset_meta.keys())}\n")

    csv_files = sorted(TTEST_DIR.glob("*_daily_volume_ttest.csv"))
    print(f"Processing {len(csv_files)} t-test CSVs...")

    all_events = []
    for csv_path in csv_files:
        asset = csv_path.stem.replace("_daily_volume_ttest", "")
        df = process_asset(csv_path, asset_meta)
        if not df.empty:
            all_events.append(df)
            print(f"  {asset:15s} → {len(df):4d} significant rows")
        else:
            print(f"  {asset:15s} → no significant rows")

    print()

    # ── significant_events.csv ───────────────────────────────────────────────
    events = pd.concat(all_events, ignore_index=True)
    events = events.sort_values(["date", "asset"]).reset_index(drop=True)

    out_events = OUTPUT_DIR / "significant_events.csv"
    events.to_csv(out_events, index=False)
    print(f"Saved: {out_events}")
    print(f"  {len(events):,} rows | {events['asset'].nunique()} assets | "
          f"{events['date'].nunique()} unique dates")

    # ── dates_breadth_summary.csv ────────────────────────────────────────────
    summary = build_breadth_summary(events)

    out_summary = OUTPUT_DIR / "dates_breadth_summary.csv"
    summary.to_csv(out_summary, index=False)
    print(f"\nSaved: {out_summary}")
    print(f"  {len(summary):,} unique dates")
    print(f"\nTop 10 dates by breadth:")
    print(summary.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
