import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, min as spark_min, max as spark_max, stddev,
    sum as spark_sum, to_date, hour, date_format, count, when, lit, log,
)

from config import CLEAN_DIR, RESULTS_DIR, REPORTS_DIR
from io_utils import _Tee

CLEANED_CSV = CLEAN_DIR / "cleaned_market_data.csv"
SUMMARY_CSV = RESULTS_DIR / "spark_market_summary.csv"

PART3_LOGS = REPORTS_DIR / "part3_logs.txt"


def main():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    _report_file = open(PART3_LOGS, "w", encoding="utf-8")
    _old_stdout = sys.stdout
    sys.stdout = _Tee(_report_file)

    print("Team 3: Spark Analytics")
    print("=" * 50)


    print("\n=== Task 1: Start Spark and Load Data ===\n")
    spark = SparkSession.builder.appName("OwlAnalytics").getOrCreate()
    print(f"Spark session started")

    df = spark.read.csv(str(CLEANED_CSV), header=True, inferSchema=True)
    print(f"Loaded file: {CLEANED_CSV}")
    row_count = df.count()
    print(f"Row count: {row_count}")
    print(f"Columns: {', '.join(df.columns)}")
    print(f"\nSchema:")
    for field in df.schema.fields:
        print(f"  {field.name}: {field.dataType}")


    print("\n=== Task 2: Register Temp View and Test Query ===\n")
    df.createOrReplaceTempView("market_data")
    print("Temporary SQL view created: market_data")
    test_result = spark.sql("SELECT * FROM market_data LIMIT 10")
    test_row_count = test_result.count()
    print(f"Test query returned rows: {test_row_count}")
    print("Preview:")
    test_result.show(10, truncate=False)


    print("\n=== Task 3: Verify Derived Columns ===\n")

    derived = ["price_range", "price_change", "percent_change", "candle_direction"]
    derived_cols = ", ".join(derived)
    existing = [c for c in derived if c in df.columns]
    missing = [c for c in derived if c not in df.columns]

    if missing:
        print(f"Adding missing columns: {', '.join(missing)}")
        if "price_range" in missing:
            df = df.withColumn("price_range", col("high") - col("low"))
        if "price_change" in missing:
            df = df.withColumn("price_change", col("close") - col("open"))
        if "percent_change" in missing:
            df = df.withColumn("percent_change", (col("close") - col("open")) / col("open") * 100)
        if "candle_direction" in missing:
            df = df.withColumn("candle_direction",
                when(col("close") > col("open"), "up")
                .when(col("close") < col("open"), "down")
                .otherwise("flat"))
    else:
        print("All derived columns already exist")

    df.createOrReplaceTempView("market_data")
    print(f"Verified existing: {', '.join(existing)}")
    print(f"Added missing: {', '.join(missing) if missing else 'none'}")

    example = spark.sql(f"""
    SELECT symbol, {derived_cols}
    FROM market_data WHERE open IS NOT NULL LIMIT 1
    """)
    print("Example row:")
    example.show(1, truncate=False)


    print("\n=== Task 4: Time Features and Full-Dataset Queries ===\n")
    df = df.withColumn("trade_date", to_date("open_time"))
    df = df.withColumn("trade_hour", hour("open_time"))
    df = df.withColumn("day_of_week", date_format("open_time", "E"))
    df.createOrReplaceTempView("market_data")
    print("Created time features: trade_date, trade_hour, day_of_week")
    print("Example row:")
    df.select("open_time", "trade_date", "trade_hour", "day_of_week").show(1, truncate=False)

    print("Average close price by symbol:")
    avg_close = spark.sql(
        "SELECT symbol, ROUND(AVG(close), 2) AS avg_close "
        "FROM market_data "
        "WHERE close IS NOT NULL "
        "GROUP BY symbol "
        "ORDER BY symbol"
    )
    avg_close.show(10, truncate=False)

    print("Average volume by symbol:")
    avg_volume = spark.sql(
        "SELECT symbol, ROUND(AVG(volume), 2) AS avg_volume "
        "FROM market_data "
        "WHERE volume IS NOT NULL "
        "GROUP BY symbol "
        "ORDER BY symbol"
    )
    avg_volume.show(10, truncate=False)


    print("Row count by symbol:")
    symbol_counts = spark.sql(
        "SELECT symbol, COUNT(*) AS row_count "
        "FROM market_data "
        "GROUP BY symbol "
        "ORDER BY symbol"
    )
    symbol_counts.show(10, truncate=False)
    symbol_counts.createOrReplaceTempView("symbol_counts")
    print("Registered temp view: symbol_counts")


    print("Full Spark result uses all cleaned rows, not only 50 sample rows.")
    sample_dir = Path("results/pandas_sample_results.csv")
    if sample_dir.exists():
        print(
            "\nComparison with Team 2 pandas sample:\n"
            "Team 2 used a balanced 50-record pandas sample to check data quality. "
            "The Spark queries above use all cleaned rows for every symbol, so the "
            "average close, average volume, and row counts are based on the full "
            "dataset, not just a small subset. This makes the Spark results more "
            "reliable for analytics and ranking, because they capture all candles "
            "across all symbols and time periods. The same would work for larger "
            "datasets too."
            )

    from pyspark.sql.functions import row_number
    from pyspark.sql.window import Window


    print("\n=== Task 5: Volatility Ranking ===\n")
    volatility = spark.sql(
        "SELECT symbol, "
        "  ROUND(AVG(price_range), 2) AS avg_price_range, "
        "  ROUND(MIN(price_range), 2) AS min_price_range, "
        "  ROUND(MAX(price_range), 2) AS max_price_range, "
        "  ROUND(STDDEV(price_range), 2) AS stddev_price_range "
        "FROM market_data "
        "WHERE price_range IS NOT NULL "
        "GROUP BY symbol "
        "ORDER BY avg_price_range DESC"
    )

    volatility = volatility.withColumn(
        "volatility_rank",
        row_number().over(Window.orderBy(col("avg_price_range").desc()))
    )

    volatility.show(10, truncate=False)
    volatility.createOrReplaceTempView("volatility_results")
    print("Registered temp view: volatility_results")

    print("\n=== Task 6: Activity Ranking ===\n")
    activity = spark.sql(
        "SELECT symbol, "
        "  SUM(trade_count) AS total_trades, "
        "  SUM(quote_volume) AS total_quote_volume, "
        "  ROUND(AVG(volume), 2) AS avg_volume "
        "FROM market_data "
        "WHERE trade_count IS NOT NULL AND quote_volume IS NOT NULL "
        "GROUP BY symbol"
    )
    
    activity = activity.withColumn(
        "symbol_activity_score",
        col("total_trades") * log(col("total_quote_volume") + lit(1))
    )
    
    activity = activity.withColumn(
        "symbol_activity_rank",
        row_number().over(Window.orderBy(col("symbol_activity_score").desc()))
    )
    
    activity.orderBy(col("symbol_activity_score").desc()).show(10, truncate=False)
    activity.createOrReplaceTempView("activity_results")
    print("Registered temp view: activity_results")

    print("\n=== Task 7: Activity by Time ===\n")

    hour_activity = spark.sql(
        "SELECT trade_hour, "
        "  SUM(trade_count) AS total_trades, "
        "  SUM(quote_volume) AS total_quote_volume "
        "FROM market_data "
        "WHERE trade_hour IS NOT NULL "
        "GROUP BY trade_hour"
    )

    hour_activity = hour_activity.withColumn(
        "hour_activity_score",
        col("total_trades") * log(col("total_quote_volume") + lit(1))
    )

    busiest_hour = hour_activity.orderBy(col("hour_activity_score").desc()).limit(1)

    print("Activity by hour:")
    hour_activity.orderBy(col("hour_activity_score").desc()).show(24, truncate=False)

    date_activity = spark.sql(
        "SELECT trade_date, "
        "  SUM(trade_count) AS total_trades, "
        "  SUM(quote_volume) AS total_quote_volume "
        "FROM market_data "
        "WHERE trade_date IS NOT NULL "
        "GROUP BY trade_date"
    )

    date_activity = date_activity.withColumn(
        "date_activity_score",
        col("total_trades") * log(col("total_quote_volume") + lit(1))
    )

    busiest_date = date_activity.orderBy(col("date_activity_score").desc()).limit(1)

    print("Activity by date:")
    date_activity.orderBy(col("date_activity_score").desc()).show(10, truncate=False)
    print("\n\nBusiest hour by activity_score:")
    busiest_hour.show(1, truncate=False)
    print("\nBusiest date by activity_score:")
    busiest_date.show(1, truncate=False)

    print(
        "\nInterpretation:\n"
        "Activity is defined consistently with Task 6 as a combined score "
        "based on total trades and total quote volume. The log for quote volume is to "
        "prevent very large volumes from dominating the score. The tables above show "
        "how this activity_score varies by hour and by date. The busiest hour "
        "and date are the time intervals with the highest activity_score in the "
        "cleaned dataset, meaning they had both many trades and large quote "
        "volume across all symbols."
    )


    print("\n=== Task 8: Final Ranked Market Summary ===\n")
    summary = spark.sql(
        "SELECT "
        "  v.symbol, "
        "  rc.row_count, "
        "  v.avg_price_range, "
        "  v.min_price_range, "
        "  v.max_price_range, "
        "  v.stddev_price_range, "
        "  v.volatility_rank, "
        "  a.total_trades, "
        "  a.total_quote_volume, "
        "  a.avg_volume, "
        "  a.symbol_activity_score, "
        "  a.symbol_activity_rank, "
        "  ROUND(AVG(m.percent_change), 2) AS avg_percent_change, "
        "  SUM(CASE WHEN m.candle_direction = 'up' THEN 1 ELSE 0 END) AS up_count, "
        "  SUM(CASE WHEN m.candle_direction = 'down' THEN 1 ELSE 0 END) AS down_count, "
        "  SUM(CASE WHEN m.candle_direction = 'flat' THEN 1 ELSE 0 END) AS flat_count "
        "FROM market_data m "
        "JOIN volatility_results v ON m.symbol = v.symbol "
        "JOIN activity_results a ON m.symbol = a.symbol "
        "JOIN symbol_counts rc ON m.symbol = rc.symbol "
        "WHERE m.percent_change IS NOT NULL AND m.candle_direction IS NOT NULL "
        "GROUP BY v.symbol, rc.row_count, v.avg_price_range, v.min_price_range, "
        "         v.max_price_range, v.stddev_price_range, v.volatility_rank, "
        "         a.total_trades, a.total_quote_volume, a.avg_volume, "
        "         a.symbol_activity_score, a.symbol_activity_rank "
        "ORDER BY a.symbol_activity_rank"
    )
    summary.show(10, truncate=False)

    top_activity_row = summary.orderBy("symbol_activity_rank").first()
    top_volatility_row = summary.orderBy("volatility_rank").first()
    assert top_activity_row is not None and top_volatility_row is not None
    top_activity = top_activity_row["symbol"]
    top_volatility = top_volatility_row["symbol"]


    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    summary.toPandas().to_csv(SUMMARY_CSV, index=False)
    print(f"Saved: {SUMMARY_CSV}")
    print("Final ranked market summary created")
    print(f"Rows in summary: {summary.count()}")

    print(
    f"Top activity symbol: {top_activity} "
    f"(highest combined trades and quote volume)."
    )
    print(
    f"Top volatility symbol: {top_volatility} "
    f"(largest average price range and volatility rank)."
    )
    print(
    "In this summary table, activity_score is defined as "
    "total_trades * log(total_quote_volume + 1). "
    "This means symbols with many trades "
    "and high quote volume rank as more active, but taking the log of "
    "quote volume prevents a few very large volumes from dominating the "
    "score. For each symbol, the table also shows how many records were "
    "in the cleaned dataset, average volume, volatility statistics based "
    "on price_range, and how often candles closed up, down, or flat. "
    "This helps Zehra see which symbols are most active, most volatile, "
    "and most important to mention in the final report."
)




    spark.stop()
    print("Spark session stopped.")

    sys.stdout.flush()
    _report_file.flush()
    sys.stdout = _old_stdout
    _report_file.close()

    print(f"Report saved: {PART3_LOGS}")


if __name__ == "__main__":
    main()
