import numpy as np
import pandas as pd
from pathlib import Path

from config import (
    MESSY_DIR, CLEAN_DIR, RESULTS_DIR,
    NUMERIC_COLS, TIME_COLS,
)

MESSY_CSV = MESSY_DIR / "messy_market_data.csv"
CLEANED_CSV = CLEAN_DIR / "cleaned_market_data.csv"
SAMPLE_CSV = RESULTS_DIR / "pandas_sample_results.csv"


def main():
    print("Team 2: Data Quality")
    print("=" * 50)

    # ── Task 1: Load and inspect ──
    print(f"\nLoaded {MESSY_CSV}")
    df = pd.read_csv(MESSY_CSV)
    rows, cols = df.shape
    print(f"Rows: {rows}")
    print(f"Columns: {cols}")

    report = {
        "rows_before": len(df),
        "missing_before": int(df.isna().sum().sum()),
    }
    print("\nFirst 10 rows:")
    print(df.head(10).to_string(index=False))
    print("\nData types:")
    print(df.dtypes.to_string())

    # ── Task 2: Missing values ──
    missing = df.isna().sum()
    missing_filtered = missing[missing > 0]
    print(f"\nMissing values:")
    if len(missing_filtered) > 0:
        print(missing_filtered.to_string())
        worst = missing_filtered.idxmax()
        print(f"Most affected column: {worst}")
    else:
        print("No missing values found")

    # ── Task 3: Convert numeric columns ──
    print(f"\nConverted numeric columns:")
    invalid_count = 0
    for col in NUMERIC_COLS:
        before = df[col].isna().sum()
        df[col] = pd.to_numeric(df[col], errors="coerce")
        after = df[col].isna().sum()
        invalid_count += after - before
    print(", ".join(NUMERIC_COLS))
    print(f"Invalid numeric rows after conversion: {invalid_count}")

    # ── Task 4a: Convert timestamp columns ──
    print(f"\nConverted timestamp columns:")
    invalid_times = {}
    for col in TIME_COLS:
        before = df[col].isna().sum()
        df[col] = pd.to_datetime(df[col], errors="coerce")
        after = df[col].isna().sum()
        invalid_times[col] = after - before
    print(", ".join(TIME_COLS))
    for col, count in invalid_times.items():
        print(f"Invalid {col} values: {count}")

    # ── Task 4b: Clean symbol names ──
    print(f"\nSymbols before cleaning:")
    print(", ".join(df["symbol"].unique()))
    df["symbol"] = (
        df["symbol"]
        .str.strip()
        .str.upper()
        .str.replace("/", "", regex=False)
    )
    print(f"Symbols after cleaning:")
    print(", ".join(df["symbol"].unique()))
    print(f"Unique cleaned symbols: {df['symbol'].nunique()}")

    # ── Task 5: Remove duplicates ──
    before_dedup = len(df)
    dup_count = df.duplicated().sum()
    df = df.drop_duplicates()
    after_dedup = len(df)
    print(f"\nDuplicate rows found: {dup_count}")
    print(f"Rows before removing duplicates: {before_dedup}")
    print(f"Rows after removing duplicates: {after_dedup}")

    # ── Task 6: Detect impossible values ──
    neg_vol = (df["volume"] < 0).sum()
    neg_trade = (df["trade_count"] < 0).sum()
    high_low = (df["high"] < df["low"]).sum()
    print(f"\nNegative volume rows: {neg_vol}")
    print(f"Negative trade_count rows: {neg_trade}")
    print(f"Rows where high < low: {high_low}")

    # ── Task 7: Create new columns ──
    choices = ["up", "down"]
    conditions = [
        df["close"] > df["open"],
        df["close"] < df["open"],
    ]
    
    df["price_range"] = df["high"] - df["low"]
    df["price_change"] = df["close"] - df["open"]
    df["percent_change"] = (df["price_change"] / df["open"]) * 100
    df["candle_direction"] = np.select(conditions, choices, default="flat")
    
    print(f"\nCreated columns:")
    print("price_range, price_change, percent_change, candle_direction")
    print(f"\nExample row:")
    example = df.dropna().iloc[5]
    print(f"open={example['open']:.2f} close={example['close']:.2f} "
          f"high={example['high']:.2f} low={example['low']:.2f}")
    print(f"price_range={example['price_range']:.2f} "
          f"price_change={example['price_change']:.2f} "
          f"percent_change={example['percent_change']:.2f} "
          f"candle_direction={example['candle_direction']}")

    # ── Task 8: Data-quality report ──
    report["rows_after"] = len(df)
    report["missing_after"] = int(df.isna().sum().sum())
    report["duplicates_found"] = dup_count
    report["invalid_numeric"] = invalid_count
    report["invalid_timestamps"] = sum(invalid_times.values())
    report["negative_volume"] = neg_vol

    print(f"\nData-quality report")
    print(f"Rows before cleaning: {report['rows_before']}")
    print(f"Rows after cleaning: {report['rows_after']}")
    print(f"Missing values before: {report['missing_before']}")
    print(f"Missing values after: {report['missing_after']}")
    print(f"Duplicate rows found: {report['duplicates_found']}")
    print(f"Cleaning decision: "
          f"invalid numeric values ({report['invalid_numeric']} rows) "
          f"and invalid timestamps ({report['invalid_timestamps']} rows) "
          f"were coerced to NaN. "
          f"Negative volumes ({report['negative_volume']} rows) "
          f"were not removed but flagged for review.")

    # ── Save cleaned CSV ──
    CLEANED_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(CLEANED_CSV, index=False)
    print(f"\nSaved cleaned dataset: {CLEANED_CSV}")
    print(f"Cleaned rows: {len(df)}")

    # ── Sample check: 50 records (5 per symbol) ──
    sample = (
        df.dropna()
        .groupby("symbol", group_keys=False)
        .sample(n=5, random_state=42)
    )
    sample = sample.reset_index(drop=True)

    avg_close = sample.groupby("symbol")["close"].mean()
    highest_vol_symbol = sample.groupby("symbol")["volume"].mean().idxmax()
    direction_counts = sample["candle_direction"].value_counts()
    max_range_row = sample.loc[sample["price_range"].idxmax()]

    print(f"\nSample check: 50 records")
    print(f"Average close price by symbol:")
    for symbol, price in avg_close.items():
        print(f"  {symbol}: {price:.2f}")
    print(f"Highest average volume: {highest_vol_symbol}")
    print(f"Candle direction counts:")
    for direction, count in direction_counts.items():
        print(f"  {direction}: {count}")
    print(f"Largest price range row: {max_range_row['symbol']} "
          f"(range={max_range_row['price_range']:.2f})")

    SAMPLE_CSV.parent.mkdir(parents=True, exist_ok=True)
    sample.to_csv(SAMPLE_CSV, index=False)
    print(f"\nSaved sample results: {SAMPLE_CSV}")
    print(f"Sample rows used: {len(sample)}")
    print(f"Symbols included: {sample['symbol'].nunique()}")
    print(f"Records per symbol: 5")
    print(f"Questions answered: 4")


if __name__ == "__main__":
    main()