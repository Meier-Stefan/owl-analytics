from pathlib import Path

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
]

INTERVAL = "1h"
LIMIT = 1000
RATE_LIMIT = 100
RATE_WINDOW = 60
MAX_WORKERS = 5
BASE_URL = "https://data-api.binance.vision/api/v3/klines"

DATA_DIR = Path("data")
CLEAN_DIR = DATA_DIR / "clean"
MESSY_DIR = DATA_DIR / "messy"
RESULTS_DIR = Path("results")
REPORTS_DIR = Path("reports")

FIELDNAMES = [
    "symbol", "interval", "open_time", "open", "high", "low", "close",
    "volume", "close_time", "quote_volume", "trade_count",
    "taker_buy_base_volume", "taker_buy_quote_volume",
]

NUMERIC_COLS = [
    "open", "high", "low", "close",
    "volume", "quote_volume", "trade_count",
    "taker_buy_base_volume", "taker_buy_quote_volume",
]

TIME_COLS = ["open_time", "close_time"]
