import pandas as pd
from pathlib import Path


MESSY_CSV = Path("data/messy/messy_market_data.csv")
CLEANED_CSV = Path("data/clean/cleaned_market_data.csv")
SAMPLE_CSV = Path("results/pandas_sample_results.csv")

NUMERIC_COLS = [
    "open", "high", "low", "close",
    "volume", "quote_volume", "trade_count",
    "taker_buy_base_volume", "taker_buy_quote_volume",
]

TIME_COLS = ["open_time", "close_time"]

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
]


def main():
    print("Team 2: Data Quality")
    print("=" * 50)

    print(f"\nLoaded {MESSY_CSV}")
    df = pd.read_csv(MESSY_CSV)
    rows, cols = df.shape
    print(f"Rows: {rows}")
    print(f"Columns: {cols}")

    print("\nFirst 10 rows:")
    print(df.head(10).to_string(index=False))

    print("\nData types:")
    print(df.dtypes.to_string())


if __name__ == "__main__":
    main()
