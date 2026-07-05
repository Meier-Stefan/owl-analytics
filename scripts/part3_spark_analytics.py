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

PART3_LOGS = REPORTS_DIR / "part3_logs.md"


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
    test_rows = test_result.count()
    print(f"Test query returned rows: {test_rows}")
    print("Preview:")
    test_result.show(10, truncate=False)


    spark.stop()
    print("Spark session stopped.")

    sys.stdout.flush()
    _report_file.flush()
    sys.stdout = _old_stdout
    _report_file.close()

    print(f"Report saved: {PART3_LOGS}")


if __name__ == "__main__":
    main()
