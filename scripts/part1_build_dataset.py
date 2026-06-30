import csv
import time
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock, Semaphore
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


class RateLimiter:
    def __init__(self, max_per_minute):
        self.max_per_minute = max_per_minute
        self._lock = Lock()
        self._timestamps = []
        self._wait_count = 0

    def acquire(self):
        with self._lock:
            now = time.monotonic()
            cutoff = now - 60.0
            self._timestamps = [t for t in self._timestamps if t > cutoff]

            if len(self._timestamps) >= self.max_per_minute:
                sleep_time = self._timestamps[0] - cutoff
                self._wait_count += 1
            else:
                self._timestamps.append(now)
                return 0.0

        time.sleep(sleep_time)

        with self._lock:
            self._timestamps.append(time.monotonic())
        return sleep_time

    @property
    def wait_count(self):
        with self._lock:
            return self._wait_count


def write_csv(filepath, fieldnames, rows):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def fetch_one_symbol(symbol, rate_limiter, log_lock, semaphore=None):
    context = semaphore or nullcontext()
    with context:
        sleep_time = rate_limiter.acquire()
        if sleep_time > 0:
            log_message(log_lock, f"RATE_LIMIT wait {sleep_time:.2f}s for symbol={symbol}")
            time.sleep(sleep_time)

        log_message(log_lock, f"START request symbol={symbol} interval={INTERVAL} limit={LIMIT}")

        try:
            response = requests.get(
                BASE_URL,
                params={"symbol": symbol, "interval": INTERVAL, "limit": LIMIT},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            log_message(log_lock, f"ERROR request symbol={symbol}: {e}")
            return symbol, []

        rows = [
            {
                "symbol": symbol,
                "interval": INTERVAL,
                "open_time": convert_timestamp(r[0]),
                "open": r[1],
                "high": r[2],
                "low": r[3],
                "close": r[4],
                "volume": r[5],
                "close_time": convert_timestamp(r[6]),
                "quote_volume": r[7],
                "trade_count": r[8],
                "taker_buy_base_volume": r[9],
                "taker_buy_quote_volume": r[10],
            }
            for r in data
        ]

        log_message(log_lock, f"END request symbol={symbol} records={len(rows)}")
        print(f"Downloaded {symbol}: {len(rows)} records")
        return symbol, rows


def download_serial(symbols, semaphore, rate_limiter, log_lock):
    all_rows = []
    for symbol in symbols:
        _, rows = fetch_one_symbol(symbol, semaphore, rate_limiter, log_lock)
        all_rows.extend(rows)
    return all_rows


def main():
    log_lock = Lock()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write("")

    print_and_log(log_lock, "Script started")


if __name__ == "__main__":
    main()
