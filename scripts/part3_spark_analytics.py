import sys
from pathlib import Path

from pyspark.sql import SparkSession


from config import CLEAN_DIR, RESULTS_DIR, REPORTS_DIR
from io_utils import _Tee

CLEANED_CSV = CLEAN_DIR / "cleaned_market_data.csv"

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


if __name__ == "__main__":
    main()
