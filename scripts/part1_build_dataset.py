import csv
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
]
INTERVAL = "1h"
LIMIT = 1000
BASE_URL = "https://data-api.binance.vision/api/v3/klines"
REQUESTS_PER_MINUTE = 100

CLEAN_DIR = Path("data/clean")
RESULTS_DIR = Path("results")
OUTPUT_CSV = CLEAN_DIR / "clean_market_data.csv"
LOG_FILE = RESULTS_DIR / "api_download.log"
BENCHMARK_CSV = RESULTS_DIR / "runtime_comparison.csv"

FIELDNAMES = [
    "symbol", "interval", "open_time", "open", "high", "low", "close",
    "volume", "close_time", "quote_volume", "trade_count",
    "taker_buy_base_volume", "taker_buy_quote_volume",
]


def convert_timestamp(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def log_message(log_lock, message):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with log_lock:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"{timestamp} | {message}\n")


def print_and_log(log_lock, message):
    print(message)
    log_message(log_lock, message)


def write_csv(filepath, fieldnames, rows):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    log_lock = Lock()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write("")

    print_and_log(log_lock, "Script started")


if __name__ == "__main__":
    main()
