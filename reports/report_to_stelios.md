# Report

Hi Stelios,
Below is a summary of what I built during my first week at Owl Analytics, covering data collection, cleaning, and analysis of recent cryptocurrency market activity.

## API Downloader Functionality

The downloader is configured for 10 symbols (`BTCUSDT`, `ETHUSDT`, `BNBUSDT`, `SOLUSDT`, `XRPUSDT`, `ADAUSDT`, `DOGEUSDT`, `AVAXUSDT`, `LINKUSDT`, `DOTUSDT` ) at a 1-hour interval with 1,000 records per symbol, targeting exactly 10,000 records in total. The Binance Vision API was used as the data source. Each API response returns raw OHLCV kline data, which is parsed into a structured dictionary with typed fields (`open`, `high`, `low`, `close`, `volume`, `trade_count`, etc.) and ISO 8601 timestamps converted from millisecond epoch values. The output is written to `data/clean/clean_market_data.csv` using `csv.DictWriter`. The record count check confirmed: 10,000 records saved, 0 failures.

## Concurrent API Requests

The script benchmarks two download strategies. The serial download completed in 17.23 seconds; the multithreaded download with 5 workers completed the same 10000 records in 4.98 seconds, which is a 3.5 × speedup. The full results are stored in the [runtime_comparison file](results/runtime_comparison.csv) for further inspection. The multithreaded downloader uses `ThreadPoolExecutor` with `as_completed()` to process futures as they resolve. The results are accumulated per symbol before writing. All requests are logged with `START`, `END`, and `WROTE` markers.

## Log File Protection

Unlike processes, threads share their memory. This means if multiple threads write to the same file, it will result in a corrupted file. To prevent this, all writes to the log file go through `log_message()`, which acquires a `threading.Lock` before opening the file. The same lock is passed to every thread, so only one thread can write at a time. The rate limiter also uses a separate lock to protect its shared timestamp list from concurrent reads and updates. In the capstone 1 project, we worked with processes, and there was a similar risk with the log file present: Even though processes don't share memory, they can share the file system. Two a process could overwrite the results of another one if they were to write at the same time.

## Data Cleaning

The messy dataset contained 9,991 rows across 13 columns. Inspection revealed that Dara introduced the following issues: 496 missing values before cleaning; 399 invalid numeric values coerced to `NaN` during type conversion; 299 invalid timestamps (152 in `open_time`, 147 in `close_time`); 213 duplicate rows; and 65 rows with negative volume dropped as impossible values. No rows had high < low or negative trade_count. After cleaning, duplicate rows were removed and the dataset contained 9,713 rows, with residual `NaN` values (1,672) in numeric columns where coercion could not recover the original value. Four engineered columns were added: `price_range`, `price_change`, `percent_change`, and `candle_direction` (up/down/flat). The cleaned dataset was saved to [the cleaned_market_data file](data/clean/cleaned_market_data.csv).

## Pandas

Pandas was used for the data quality work because it provides interactive inspection, easy type coercion with `errors="coerce"`, vectorised column operations, and quick visibility into missing values across the full dataset. The 50-record sample in `results/pandas_sample_results.csv` immediately exposed the messy symbol formats and the negative volume rows that would have been harder to spot in a raw CSV scan. Pandas was the right tool here because the cleaning work is inherently exploratory and the dataset fits comfortably in memory.

## Spark

Spark was used for the full-dataset analytics in Part 3. After starting a local Spark session and loading the cleaned dataset, a temporary SQL view (`market_data`) was registered to allow standard SQL queries. Spark allowed running grouped aggregations, window-function-style ranking, and time-based summaries over the full dataset quickly and with a clear, readable query structure. An additional benefit is that Spark runs on top of the JVM and integrates with the Hadoop ecosystem, meaning the same pipeline code could be pointed at the Hadoop Distributed File System or cloud storage to process data that does not fit on a single machine.

## Final Analytics

The ranked market summary revealed the following:

- Most volatile symbols (by average price range): `BTCUSDT` ranked 1st with an average price range of $450.41 per hour and a standard deviation of $326.79, followed by `ETHUSDT` ($15.84 avg range). `ADAUSDT` and `DOGEUSDT` had the lowest absolute volatility.
- Most active symbols (by combined trade and quote volume score): `BTCUSDT` ranked 1st with 153 million total trades and $49.2 billion in total quote volume. `ETHUSDT` ranked 2nd. `DOTUSDT` ranked last with 1.5 million trades and $276 million quote volume.
- When trading activity is highest: The busiest hour across all symbols was 15:00 UTC, with 34.2 million trades and $7.0 billion in quote volume. The busiest single date was 5 June 2026, with 34.1 million trades and $6.2 billion in quote volume.
- Average percent change: All symbols showed a slightly negative average percent change per hour (around −0.02% to −0.05%), with `SOLUSDT` the only symbol averaging 0.0%, suggesting a relatively flat period overall. `DOTUSDT` had the most bearish average at −0.05%.  
The full ranked summary has been saved to `results/spark_market_summary.csv` for further inspection.

Thank you so much for the opportunity to explore the full lifecycle of a data pipeline experience work in the different teams. Even though both project contained data pipelines, this experience with the focus on big data processing was different from the [SpeechSense group project](thttps://github.com/SpeechSense/SpeechSense), where the delivery of a product and collaboration was in the foreground.

Best,
Stefan