# Stefan Meier — Big Data Analytics Final Project

**Repository link:** `[github.com/Meier-Stefan/owl-analytics](https://github.com/Meier-Stefan/owl-analytics)`

## Prerequisites

- **Python 3.13+** and **uv** (see [Install UV](https://docs.astral.sh/uv/#installation))
- **Java 17+** — required by PySpark for Team 3

### Installing Java 17 on macOS

```sh
brew install openjdk@17
```

After installation, link it and verify:

```sh
sudo ln -sfn $(brew --prefix)/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk
java -version
# Expected: openjdk version "17.x.x"
```

If you already have a different Java version, set `JAVA_HOME` before running Team 3:

```sh
export JAVA_HOME=$(brew --prefix)/opt/openjdk@17
```

## Install Dependencies

```sh
uv sync
```

This creates a virtual environment (`.venv/`) and installs all packages from `pyproject.toml` and `uv.lock`, including PySpark, pandas, requests, and pytest.

## Company Scenario

Owl Analytics is a small company building market-monitoring dashboards for clients. As a newly hired junior data scientist, I rotated through three teams during my first week:

- **Team 1 (Data Collection, led by Elena):** Download 10,000 historical kline records from Binance across 10 cryptocurrency symbols.
- **Team 2 (Data Quality, led by Dara):** Clean the downloaded data after Dara deliberately introduced quality issues.
- **Team 3 (Analytics, led by Zehra):** Analyse the full cleaned dataset with Spark to identify the most active and volatile symbols.

My line manager **Stelios** requested a final report summarising what was built, whether the data is reliable, and what the analytics show.

## How to Run Team 1 Code

```sh
uv run scripts/part1_build_dataset.py
```

Downloads kline data from the Binance API for 10 symbols (1h interval, 1000 records per symbol). Benchmarks serial vs. multithreaded download with a sliding-window rate limiter (100 req/min). Saves clean data to `data/clean/clean_market_data.csv`, a runtime comparison to `results/runtime_comparison.csv`, and logs to `results/api_download.log`. Full stdout is captured to `reports/part1_logs.md`.

## How to Run Team 2 Code

```sh
uv run scripts/part2_clean_with_pandas.py
```

Reads `data/messy/messy_market_data.csv`, reports Dara's intentional defects, cleans the data (missing values, numeric/time coercion, symbol normalisation, duplicate removal, impossible-values filter), adds derived columns (`price_range`, `price_change`, `percent_change`, `candle_direction`), runs a 50-record pandas quality check, and writes the cleaned dataset to `data/clean/cleaned_market_data.csv`. Full stdout is captured to `reports/part2_logs.md`.

## How to Run Team 3 Code

```sh
uv run scripts/part3_spark_analytics.py
```

Runs the full Spark analysis on the cleaned dataset (requires Java 17+ and PySpark 4.1+). Produces volatility ranking, activity ranking (using `total_trades * LOG(total_quote_volume + 1)`), time-based activity analysis, and a final ranked market summary saved to `results/spark_market_summary.csv`. Full stdout is captured to `reports/part3_logs.md`.

## Team 3 Colab Notebook

The assignment spec recommends a Google Colab notebook (`.ipynb`) for Team 3. This submission uses `scripts/part3_spark_analytics.py` (a local Python script) that mirrors the same analysis tasks and outputs.

## Links

- **Final report to Stelios:** [`reports/report_to_stelios.md`](reports/report_to_stelios.md)
- **Reflection:** [`reports/reflection.md`](reports/reflection.md)

## Submitted Files

```
scripts/
├── config.py                  Shared constants and configuration
├── get_one_record.py          Helper to fetch one API record
├── io_utils.py                _Tee stdout-capture utility
├── mess_my_data.py            Corrupts clean CSV for pipeline tests
├── part1_build_dataset.py     Team 1: data collection
├── part2_clean_with_pandas.py Team 2: data cleaning
├── part3_spark_analytics.py   Team 3: Spark analytics
└── save_dictionary_to_csv.py  Helper to write a CSV row

tests/
├── test_config.py
├── test_file_io.py
├── test_io_utils.py
├── test_pipeline.py
├── test_rate_limiter.py
├── test_spark_analytics.py
└── test_transforms.py

reports/
├── report_to_stelios.md       Final report (to be completed)
├── reflection.md              Reflection (to be completed)
├── part1_logs.md              Team 1 run output
├── part2_logs.md              Team 2 run output
└── part3_logs.md              Team 3 run output

data/clean/
├── clean_market_data.csv      10,000-row raw download
├── cleaned_market_data.csv    Cleaned output (9713 rows)
├── example_dictionary_row.csv Single-row example
└── one_record.csv             Single-record test fixture

data/messy/
└── messy_market_data.csv      Dara's intentionally corrupted version

results/
├── api_download.log           API request log
├── pandas_sample_results.csv  50-record pandas sample check
├── runtime_comparison.csv     Serial vs. multithreaded benchmark
└── spark_market_summary.csv   Final ranked market summary

Root files:
├── README.md                  This file
├── pyproject.toml             Project metadata and dependencies
├── pyrightconfig.json         Pyright venv configuration
├── uv.lock                    Locked dependency versions
└── .gitignore
```
