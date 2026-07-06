import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, min as spark_min, max as spark_max, stddev,
    sum as spark_sum, to_date, hour, date_format, count, when, lit,
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

    spark.stop()
    print("Spark session stopped.")

    sys.stdout.flush()
    _report_file.flush()
    sys.stdout = _old_stdout
    _report_file.close()

    print(f"Report saved: {PART3_LOGS}")


if __name__ == "__main__":
    main()
