import math

import numpy as np
import pandas as pd
import pytest

from part1_build_dataset import convert_timestamp


SYMBOL_CASES = (
    ("  BTCUSDT", "BTCUSDT"),
    ("BTCUSDT ", "BTCUSDT"),
    (" btcusdt ", "BTCUSDT"),
    ("BTC/USDT", "BTCUSDT"),
    (" btc/usdt ", "BTCUSDT"),
    ("  linkusdt  ", "LINKUSDT"),
    ("LINKUSDT", "LINKUSDT"),
)


class TestConvertTimestamp:

    def test_epoch_zero_returns_expected_iso_string(self):
        result = convert_timestamp(0)
        assert result == "1970-01-01T00:00:00+00:00"

    def test_binance_example_timestamp(self):
        result = convert_timestamp(1717200000000)
        assert "2024" in result
        assert result.endswith("+00:00")

    def test_negative_milliseconds_returns_date_before_epoch(self):
        result = convert_timestamp(-3600000)
        assert result.startswith("1969")

    def test_milliseconds_are_converted_to_seconds_correctly(self):
        result_one = convert_timestamp(1000)
        result_two = convert_timestamp(2000)
        assert result_one == "1970-01-01T00:00:01+00:00"
        assert result_two == "1970-01-01T00:00:02+00:00"


class TestSymbolCleaning:

    def test_strips_leading_whitespace_from_symbol(self):
        df = pd.DataFrame({"symbol": ["  BTCUSDT"]})
        df["symbol"] = df["symbol"].str.strip().str.upper().str.replace("/", "", regex=False)
        assert df.iloc[0]["symbol"] == "BTCUSDT"

    def test_strips_trailing_whitespace_from_symbol(self):
        df = pd.DataFrame({"symbol": ["BTCUSDT "]})
        df["symbol"] = df["symbol"].str.strip().str.upper().str.replace("/", "", regex=False)
        assert df.iloc[0]["symbol"] == "BTCUSDT"

    def test_uppercases_lowercase_symbol(self):
        df = pd.DataFrame({"symbol": ["btcusdt"]})
        df["symbol"] = df["symbol"].str.strip().str.upper().str.replace("/", "", regex=False)
        assert df.iloc[0]["symbol"] == "BTCUSDT"

    def test_replaces_forward_slash_with_empty_string(self):
        df = pd.DataFrame({"symbol": ["BTC/USDT"]})
        df["symbol"] = df["symbol"].str.strip().str.upper().str.replace("/", "", regex=False)
        assert df.iloc[0]["symbol"] == "BTCUSDT"

    def test_cleans_all_variations_to_clean_symbol(self):
        for raw, expected in SYMBOL_CASES:
            df = pd.DataFrame({"symbol": [raw]})
            df["symbol"] = df["symbol"].str.strip().str.upper().str.replace("/", "", regex=False)
            assert df.iloc[0]["symbol"] == expected, f"failed for {raw!r} -> {expected!r}"

    def test_multiple_symbols_are_all_cleaned_correctly(self):
        raws = ["  BTCUSDT", "eth/usdt ", " linkusdt  ", "SOLUSDT"]
        expected = ["BTCUSDT", "ETHUSDT", "LINKUSDT", "SOLUSDT"]
        df = pd.DataFrame({"symbol": raws})
        df["symbol"] = df["symbol"].str.strip().str.upper().str.replace("/", "", regex=False)
        assert list(df["symbol"]) == expected


class TestCandleDirection:

    @pytest.fixture
    def df(self):
        return pd.DataFrame(
            {
                "open": [100.0, 100.0, 100.0],
                "close": [110.0, 90.0, 100.0],
            }
        )

    def test_up_when_close_greater_than_open(self, df):
        df["candle_direction"] = np.select(
            [df["close"] > df["open"], df["close"] < df["open"]],
            ["up", "down"],
            default="flat",
        )
        assert df.iloc[0]["candle_direction"] == "up"

    def test_down_when_close_less_than_open(self, df):
        df["candle_direction"] = np.select(
            [df["close"] > df["open"], df["close"] < df["open"]],
            ["up", "down"],
            default="flat",
        )
        assert df.iloc[1]["candle_direction"] == "down"

    def test_flat_when_close_equals_open(self, df):
        df["candle_direction"] = np.select(
            [df["close"] > df["open"], df["close"] < df["open"]],
            ["up", "down"],
            default="flat",
        )
        assert df.iloc[2]["candle_direction"] == "flat"

    def test_all_three_directions_assigned_correctly(self):
        df = pd.DataFrame(
            {"open": [50.0, 50.0, 50.0], "close": [60.0, 40.0, 50.0]}
        )
        df["candle_direction"] = np.select(
            [df["close"] > df["open"], df["close"] < df["open"]],
            ["up", "down"],
            default="flat",
        )
        assert list(df["candle_direction"]) == ["up", "down", "flat"]


class TestPriceDerivedColumns:

    @pytest.fixture
    def df(self):
        return pd.DataFrame(
            {
                "open": [100.0],
                "high": [110.0],
                "low": [90.0],
                "close": [105.0],
            }
        )

    def test_price_range_is_high_minus_low(self, df):
        df["price_range"] = df["high"] - df["low"]
        assert df.iloc[0]["price_range"] == 20.0

    def test_price_change_is_close_minus_open(self, df):
        df["price_change"] = df["close"] - df["open"]
        assert df.iloc[0]["price_change"] == 5.0

    def test_percent_change_is_price_change_divided_by_open_times_one_hundred(self, df):
        df["price_change"] = df["close"] - df["open"]
        df["percent_change"] = (df["price_change"] / df["open"]) * 100
        assert df.iloc[0]["percent_change"] == 5.0

    def test_price_range_with_decimals(self):
        df = pd.DataFrame({"high": [1.2345], "low": [1.0000]})
        df["price_range"] = df["high"] - df["low"]
        assert math.isclose(df.iloc[0]["price_range"], 0.2345)

    def test_price_change_negative_when_close_below_open(self):
        df = pd.DataFrame({"open": [100.0], "close": [95.0]})
        df["price_change"] = df["close"] - df["open"]
        assert df.iloc[0]["price_change"] == -5.0

    def test_percent_change_negative_when_close_below_open(self):
        df = pd.DataFrame({"open": [100.0], "close": [95.0]})
        df["price_change"] = df["close"] - df["open"]
        df["percent_change"] = (df["price_change"] / df["open"]) * 100
        assert df.iloc[0]["percent_change"] == -5.0

    def test_percent_change_zero_when_close_equals_open(self):
        df = pd.DataFrame({"open": [100.0], "close": [100.0]})
        df["price_change"] = df["close"] - df["open"]
        df["percent_change"] = (df["price_change"] / df["open"]) * 100
        assert df.iloc[0]["percent_change"] == 0.0
