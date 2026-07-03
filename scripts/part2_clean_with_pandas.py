import pandas as pd
from pathlib import Path

from config import SYMBOLS


MESSY_CSV = Path("data/messy/messy_market_data.csv")
CLEANED_CSV = Path("data/clean/cleaned_market_data.csv")
SAMPLE_CSV = Path("results/pandas_sample_results.csv")

NUMERIC_COLS = [
    "open", "high", "low", "close",
    "volume", "quote_volume", "trade_count",
    "taker_buy_base_volume", "taker_buy_quote_volume",
]

TIME_COLS = ["open_time", "close_time"]


def main():
    print("Team 2: Data Quality")
    print("=" * 50)

    # ── Task 1: Load and inspect ──
    print(f"\nLoaded {MESSY_CSV}")
    df = pd.read_csv(MESSY_CSV)
    rows, cols = df.shape
    print(f"Rows: {rows}")
    print(f"Columns: {cols}")
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

if __name__ == "__main__":
    main()
