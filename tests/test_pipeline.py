import csv
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from config import SYMBOLS, FIELDNAMES, NUMERIC_COLS, TIME_COLS
from mess_my_data import mess_data


EXPECTED_SAMPLE_ROWS = len(SYMBOLS) * 5
EXPECTED_NEW_COLUMNS = ["price_range", "price_change", "percent_change", "candle_direction"]


def generate_clean_csv_rows(symbols, rows_per_symbol):
    rows = []
    for symbol in symbols:
        for i in range(rows_per_symbol):
            open_price = 100.0 + i
            close_price = open_price + (1.0 if i % 2 == 0 else -1.0)
            high_price = max(open_price, close_price) + 0.5
            low_price = min(open_price, close_price) - 0.5
            rows.append(
                {
                    "symbol": symbol,
                    "interval": "1h",
                    "open_time": f"2026-06-{1 + i:02d}T00:00:00+00:00",
                    "open": str(open_price),
                    "high": str(high_price),
                    "low": str(low_price),
                    "close": str(close_price),
                    "volume": "5000.0",
                    "close_time": f"2026-06-{1 + i:02d}T00:59:59.999000+00:00",
                    "quote_volume": str(5000.0 * open_price),
                    "trade_count": "100",
                    "taker_buy_base_volume": "2500.0",
                    "taker_buy_quote_volume": str(2500.0 * open_price),
                }
            )
    return rows


def apply_cleaning_pipeline(df):
    df = df.copy()

    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in TIME_COLS:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    df["symbol"] = df["symbol"].str.strip().str.upper().str.replace("/", "", regex=False)

    before_dedup = len(df)
    df = df.drop_duplicates()

    df["price_range"] = df["high"] - df["low"]
    df["price_change"] = df["close"] - df["open"]
    df["percent_change"] = (df["price_change"] / df["open"]) * 100
    df["candle_direction"] = np.select(
        [df["close"] > df["open"], df["close"] < df["open"]],
        ["up", "down"],
        default="flat",
    )

    return df


class TestPipelineEndToEnd:

    @pytest.fixture
    def project_dir(self, tmp_path):
        return tmp_path

    @pytest.fixture
    def clean_csv_path(self, project_dir):
        path = project_dir / "data" / "clean" / "clean_market_data.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = generate_clean_csv_rows(SYMBOLS, rows_per_symbol=10)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)
        return path

    @pytest.fixture
    def messy_csv_path(self, clean_csv_path, project_dir):
        path = project_dir / "data" / "messy" / "messy_market_data.csv"
        mess_data(clean_csv_path, path, seed=42)
        return path

    def test_mess_data_produces_expected_summary_keys(self, clean_csv_path, project_dir):
        messy_path = project_dir / "data" / "messy" / "messy_market_data.csv"
        summary = mess_data(clean_csv_path, messy_path, seed=42)
        assert "original_rows" in summary
        assert "dropped_rows" in summary
        assert "duplicated_rows" in summary
        assert "final_rows" in summary

    def test_mess_data_changed_row_count(self, clean_csv_path, project_dir):
        messy_path = project_dir / "data" / "messy" / "messy_market_data.csv"
        summary = mess_data(clean_csv_path, messy_path, seed=42)
        assert summary["original_rows"] == len(SYMBOLS) * 10
        assert summary["dropped_rows"] > 0
        assert summary["duplicated_rows"] > 0

    def test_mess_data_output_file_exists(self, messy_csv_path):
        assert messy_csv_path.exists()

    def test_mess_data_applied_all_corruption_types(self, clean_csv_path, project_dir):
        messy_path = project_dir / "data" / "messy" / "messy_market_data.csv"
        summary = mess_data(clean_csv_path, messy_path, seed=42)
        assert summary["dropped_rows"] > 0
        assert summary["duplicated_rows"] > 0
        assert summary["missing_values"] > 0
        assert summary["text_in_numeric_columns"] > 0
        assert summary["invalid_times"] > 0
        assert summary["inconsistent_symbols"] > 0
        assert summary["negative_volumes"] > 0

    def test_cleaning_produces_all_expected_columns(self, messy_csv_path):
        df = pd.read_csv(messy_csv_path)
        df = apply_cleaning_pipeline(df)
        expected_columns = set(FIELDNAMES + EXPECTED_NEW_COLUMNS)
        actual_columns = set(df.columns)
        assert expected_columns.issubset(actual_columns)

    def test_cleaning_removes_all_duplicates(self, messy_csv_path):
        df = pd.read_csv(messy_csv_path)
        before = len(df)
        df = apply_cleaning_pipeline(df)
        assert df.duplicated().sum() == 0
        assert len(df) < before

    def test_cleaning_outputs_only_known_symbols(self, messy_csv_path):
        df = pd.read_csv(messy_csv_path)
        df = apply_cleaning_pipeline(df)
        actual_symbols = set(df["symbol"].unique())
        assert actual_symbols.issubset(set(SYMBOLS))

    def test_cleaning_makes_all_numeric_columns_numeric(self, messy_csv_path):
        df = pd.read_csv(messy_csv_path)
        df = apply_cleaning_pipeline(df)
        for col in NUMERIC_COLS:
            assert np.issubdtype(df[col].dtype, np.number), f"{col} is not numeric"

    def test_cleaning_makes_all_time_columns_datetime(self, messy_csv_path):
        df = pd.read_csv(messy_csv_path)
        df = apply_cleaning_pipeline(df)
        for col in TIME_COLS:
            assert pd.api.types.is_datetime64_any_dtype(df[col]), f"{col} is not datetime"

    def test_cleaning_price_range_calculation(self, messy_csv_path):
        df = pd.read_csv(messy_csv_path)
        df = apply_cleaning_pipeline(df)
        valid = df.dropna(subset=["high", "low"])
        for _, row in valid.iterrows():
            expected = row["high"] - row["low"]
            assert math.isclose(row["price_range"], expected)

    def test_cleaning_percent_change_calculation(self, messy_csv_path):
        df = pd.read_csv(messy_csv_path)
        df = apply_cleaning_pipeline(df)
        valid = df.dropna(subset=["open", "close"])
        for _, row in valid.head(20).iterrows():
            expected = ((row["close"] - row["open"]) / row["open"]) * 100
            assert math.isclose(row["percent_change"], expected)

    def test_cleaning_no_remaining_text_in_numeric_columns(self, messy_csv_path):
        df = pd.read_csv(messy_csv_path)
        df = apply_cleaning_pipeline(df)
        for col in NUMERIC_COLS:
            non_nan = df[col].dropna()
            assert non_nan.apply(lambda x: isinstance(x, (int, float, np.number))).all()

    def test_cleaning_output_has_no_empty_strings(self, messy_csv_path):
        df = pd.read_csv(messy_csv_path)
        df = apply_cleaning_pipeline(df)
        for col in df.columns:
            if df[col].dtype == object:
                assert not (df[col].astype(str).str.strip().eq("").any())

    def test_sample_check_outputs_fifty_rows(self, messy_csv_path):
        df = pd.read_csv(messy_csv_path)
        df = apply_cleaning_pipeline(df)
        sample = (
            df.dropna()
            .groupby("symbol", group_keys=False)
            .sample(n=5, random_state=42)
            .reset_index(drop=True)
        )
        assert len(sample) == 50

    def test_sample_check_includes_all_ten_symbols(self, messy_csv_path):
        df = pd.read_csv(messy_csv_path)
        df = apply_cleaning_pipeline(df)
        sample = (
            df.dropna()
            .groupby("symbol", group_keys=False)
            .sample(n=5, random_state=42)
            .reset_index(drop=True)
        )
        assert sample["symbol"].nunique() == 10

    def test_sample_check_has_exactly_five_rows_per_symbol(self, messy_csv_path):
        df = pd.read_csv(messy_csv_path)
        df = apply_cleaning_pipeline(df)
        sample = (
            df.dropna()
            .groupby("symbol", group_keys=False)
            .sample(n=5, random_state=42)
            .reset_index(drop=True)
        )
        counts = sample["symbol"].value_counts()
        assert (counts == 5).all()

    def test_sample_check_has_all_new_columns(self, messy_csv_path):
        df = pd.read_csv(messy_csv_path)
        df = apply_cleaning_pipeline(df)
        sample = (
            df.dropna()
            .groupby("symbol", group_keys=False)
            .sample(n=5, random_state=42)
            .reset_index(drop=True)
        )
        for col in EXPECTED_NEW_COLUMNS:
            assert col in sample.columns
