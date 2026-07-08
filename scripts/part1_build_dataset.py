import csv
import sys
import time
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock, Semaphore
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from config import (
    SYMBOLS, INTERVAL, LIMIT, BASE_URL, RATE_LIMIT, RATE_WINDOW,
    MAX_WORKERS, CLEAN_DIR, RESULTS_DIR, REPORTS_DIR, FIELDNAMES,
)
from io_utils import _Tee

OUTPUT_CSV = CLEAN_DIR / "clean_market_data.csv"
LOG_FILE = RESULTS_DIR / "api_download.log"
BENCHMARK_CSV = RESULTS_DIR / "runtime_comparison.csv"

PART1_LOGS = REPORTS_DIR / "part1_logs.txt"


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
    def __init__(self, max_per_minute, log_lock=None, window=60.0):
        self.max_per_minute = max_per_minute
        self._window = window
        self._log_lock = log_lock
        self._lock = Lock()
        self._timestamps = []
        self._wait_count = 0

    def acquire(self):
        with self._lock:
            now = time.monotonic()
            cutoff = now - self._window
            self._timestamps = [t for t in self._timestamps if t > cutoff]

            if len(self._timestamps) >= self.max_per_minute:
                sleep_time = self._timestamps[0] - cutoff
                self._wait_count += 1
            else:
                self._timestamps.append(now)
                return 0.0

        if self._log_lock is not None:
            log_message(self._log_lock, f"RATE_LIMIT wait {sleep_time:.2f}s")
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
        rate_limiter.acquire()

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
            return False, []

        rows = [
            {
                "symbol": symbol,
                "interval": INTERVAL,
                "open_time": convert_timestamp(r[0]),
                "open": float(r[1]),
                "high": float(r[2]),
                "low": float(r[3]),
                "close": float(r[4]),
                "volume": float(r[5]),
                "close_time": convert_timestamp(r[6]),
                "quote_volume": float(r[7]),
                "trade_count": int(r[8]),
                "taker_buy_base_volume": float(r[9]),
                "taker_buy_quote_volume": float(r[10]),
            }
            for r in data
        ]

        log_message(log_lock, f"END request symbol={symbol} records={len(rows)}")
        print_and_log(log_lock, f"Downloaded {symbol}: {len(rows)} records")
        return True, rows


def download_serial(symbols, rate_limiter, log_lock, semaphore=None):
    all_rows = []
    failures = 0
    for symbol in symbols:
        ok, rows = fetch_one_symbol(symbol, rate_limiter, log_lock, semaphore)
        if not ok:
            failures += 1
        all_rows.extend(rows)
    return all_rows, failures


def download_multithreaded(symbols, rate_limiter, log_lock, semaphore=None):
    all_rows = []
    failures = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_one_symbol, symbol, rate_limiter, log_lock, semaphore): symbol
            for symbol in symbols
        }
        for future in as_completed(futures):
            ok, rows = future.result()
            if not ok:
                failures += 1
            all_rows.extend(rows)
    return all_rows, failures


def main():
    log_lock = Lock()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write("")

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    _report_file = open(PART1_LOGS, "w", encoding="utf-8")
    _old_stdout = sys.stdout
    sys.stdout = _Tee(_report_file)

    print()
    print_and_log(log_lock, "\n===Task 1: API Configuration ===\n" )
    print_and_log(log_lock, f"Symbols configured: {len(SYMBOLS)}")
    print_and_log(log_lock, f"Interval: {INTERVAL}")
    print_and_log(log_lock, f"Limit per symbol: {LIMIT}")
    print_and_log(log_lock, f"Expected records: {len(SYMBOLS) * LIMIT}")

    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    print_and_log(log_lock, f"Created folders: {CLEAN_DIR}, {RESULTS_DIR}")

    print()
    print_and_log(log_lock, "\n===Task 6: Download Benchmark ===\n" )
    print_and_log(log_lock, "Starting serial download for 10 symbols")
    serial_start = time.perf_counter()
    serial_rows, serial_failures = download_serial(SYMBOLS, RateLimiter(RATE_LIMIT, log_lock, RATE_WINDOW), log_lock, Semaphore(5))
    serial_time = time.perf_counter() - serial_start
    print_and_log(log_lock, f"Serial download complete: {len(serial_rows)} records, {serial_failures} failures in {serial_time:.4f}s")

    print()
    print_and_log(log_lock, "\n===Task 3: Multithreaded Download ===\n" )
    print_and_log(log_lock, "Starting multithreaded download for 10 symbols")
    mt_rate_limiter = RateLimiter(RATE_LIMIT, log_lock, RATE_WINDOW)
    mt_start = time.perf_counter()
    mt_rows, mt_failures = download_multithreaded(SYMBOLS, mt_rate_limiter, log_lock, Semaphore(5))
    mt_time = time.perf_counter() - mt_start
    print_and_log(log_lock, f"Multithreaded download complete: {len(mt_rows)} records, {mt_failures} failures")

    print()
    print_and_log(log_lock, "\n===Task 2: Combined CSV Dataset ===\n" )
    write_csv(OUTPUT_CSV, FIELDNAMES, mt_rows)
    log_message(log_lock, f"WROTE csv={OUTPUT_CSV} records={len(mt_rows)}")
    print_and_log(log_lock, f"Saved: {OUTPUT_CSV}")
    print_and_log(log_lock, f"Total records saved: {len(mt_rows)}")

    check = "passed" if len(mt_rows) == len(SYMBOLS) * LIMIT else "failed"
    print_and_log(log_lock, f"Record count check: {check}")

    print()
    print_and_log(log_lock, "\n===Task 4: Rate Limiting ===\n" )
    print_and_log(log_lock, f"Request limit: {RATE_LIMIT} requests per minute")
    print_and_log(log_lock, "Current request batch allowed")
    print_and_log(log_lock, f"Rate-limit wait events logged: {mt_rate_limiter.wait_count}")

    print()
    print_and_log(log_lock, "\n===Task 5: Log File ===\n" )
    log_message(log_lock, f"Log file: {LOG_FILE}")
    print_and_log(log_lock, f"Log file created: {LOG_FILE}")
    print_and_log(log_lock, "All requests logged with START / END / WROTE markers")

    benchmark_rows = [
        {
            "method": "serial",
            "seconds": round(serial_time, 4),
            "records": len(serial_rows),
            "note": "downloaded the ten symbols one after another",
        },
        {
            "method": "multithreading",
            "seconds": round(mt_time, 4),
            "records": len(mt_rows),
            "note": "downloaded several symbols at the same time",
        },
    ]
    write_csv(BENCHMARK_CSV, ["method", "seconds", "records", "note"], benchmark_rows)
    print()
    print_and_log(log_lock, "\n===Benchmark Results ===\n" )
    print_and_log(log_lock, f"serial_seconds: {round(serial_time, 4)}")
    print_and_log(log_lock, f"multithreading_seconds: {round(mt_time, 4)}")
    print_and_log(log_lock, f"Saved: {BENCHMARK_CSV}")

    total_failures = serial_failures + mt_failures
    if total_failures > 0:
        print_and_log(log_lock, f"Download failures detected: {total_failures}")
        print_and_log(log_lock, f"  serial failures: {serial_failures}")
        print_and_log(log_lock, f"  multithreaded failures: {mt_failures}")
        sys.exit(1)

    print()
    print_and_log(log_lock, "\n===Task 7: Script Completion ===\n" )
    print_and_log(log_lock, "Script completed successfully")
    output_files = [OUTPUT_CSV, LOG_FILE, BENCHMARK_CSV]
    existing = [p for p in output_files if p.exists()]
    print_and_log(log_lock, f"Output files found: {len(existing)}")
    print_and_log(log_lock, "No price analytics were calculated in Team 1")

    sys.stdout.flush()
    _report_file.flush()
    sys.stdout = _old_stdout
    _report_file.close()

    print(f"Report saved: {PART1_LOGS}")


if __name__ == "__main__":
    main()
