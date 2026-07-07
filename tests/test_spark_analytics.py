import math

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, when, lit, log, row_number, sum as spark_sum,
    to_date, hour, date_format,
)
from pyspark.sql.window import Window


TEST_DATA = [
    ("BTCUSDT", "2026-06-01T00:00:00+00:00", 100.0, 110.0, 90.0, 105.0, 5000.0, 500000.0, 1000),
    ("BTCUSDT", "2026-06-01T01:00:00+00:00", 105.0, 115.0, 100.0, 95.0, 6000.0, 600000.0, 1200),
    ("BTCUSDT", "2026-06-02T00:00:00+00:00", 95.0, 100.0, 90.0, 95.0, 4000.0, 400000.0, 800),
    ("ETHUSDT", "2026-06-01T00:00:00+00:00", 10.0, 11.0, 9.0, 10.5, 20000.0, 200000.0, 500),
    ("ETHUSDT", "2026-06-01T01:00:00+00:00", 10.5, 11.5, 10.0, 9.5, 25000.0, 250000.0, 600),
    ("ETHUSDT", "2026-06-02T00:00:00+00:00", 9.5, 10.0, 9.0, 9.5, 15000.0, 150000.0, 400),
]

COLUMNS = [
    "symbol", "open_time", "open", "high", "low", "close",
    "volume", "quote_volume", "trade_count",
]


@pytest.fixture(scope="session")
def spark():
    import os
    import sys
    os.environ["PYSPARK_PYTHON"] = sys.executable
    session = SparkSession.builder \
        .appName("TestOwlAnalytics") \
        .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


@pytest.fixture
def df(spark):
    return spark.createDataFrame(TEST_DATA, COLUMNS)


@pytest.fixture
def df_with_derived(df):
    df = df.withColumn("price_range", col("high") - col("low"))
    df = df.withColumn("price_change", col("close") - col("open"))
    df = df.withColumn("percent_change", (col("close") - col("open")) / col("open") * 100)
    df = df.withColumn("candle_direction",
        when(col("close") > col("open"), "up")
        .when(col("close") < col("open"), "down")
        .otherwise("flat"))
    return df


class TestSparkDerivedColumns:

    def test_price_range_is_high_minus_low(self, df):
        result = df.withColumn("price_range", col("high") - col("low"))
        rows = result.collect()
        assert rows[0]["price_range"] == 20.0
        assert rows[1]["price_range"] == 15.0
        assert rows[2]["price_range"] == 10.0

    def test_price_change_is_close_minus_open(self, df):
        result = df.withColumn("price_change", col("close") - col("open"))
        rows = result.collect()
        assert rows[0]["price_change"] == 5.0
        assert rows[1]["price_change"] == -10.0
        assert rows[2]["price_change"] == 0.0

    def test_percent_change_formula(self, df):
        result = df.withColumn("percent_change", (col("close") - col("open")) / col("open") * 100)
        rows = result.collect()
        assert rows[0]["percent_change"] == pytest.approx(5.0)
        assert rows[1]["percent_change"] == pytest.approx(-9.5238, rel=1e-3)
        assert rows[2]["percent_change"] == pytest.approx(0.0)

    def test_candle_direction_up_down_flat(self, df):
        result = df.withColumn("candle_direction",
            when(col("close") > col("open"), "up")
            .when(col("close") < col("open"), "down")
            .otherwise("flat"))
        rows = result.collect()
        assert rows[0]["candle_direction"] == "up"
        assert rows[1]["candle_direction"] == "down"
        assert rows[2]["candle_direction"] == "flat"

    def test_all_four_derived_columns_present(self, df_with_derived):
        expected = {"price_range", "price_change", "percent_change", "candle_direction"}
        actual = set(df_with_derived.columns)
        assert expected.issubset(actual)


class TestSparkTempView:

    def test_create_and_query_temp_view(self, df):
        df.createOrReplaceTempView("test_view")
        result = df.sparkSession.sql("SELECT * FROM test_view")
        assert result.count() == 6
        df.sparkSession.catalog.dropTempView("test_view")

    def test_create_or_replace_refreshes_view(self, spark):
        first = spark.createDataFrame([("A", 1)], ["s", "v"])
        first.createOrReplaceTempView("test_refresh")
        second = spark.createDataFrame([("B", 2)], ["s", "v"])
        second.createOrReplaceTempView("test_refresh")
        result = spark.sql("SELECT s FROM test_refresh")
        assert result.collect()[0]["s"] == "B"
        spark.catalog.dropTempView("test_refresh")


class TestSparkTimeFeatures:

    def test_trade_date_from_open_time(self, df):
        result = df.withColumn("trade_date", to_date("open_time"))
        rows = result.collect()
        assert str(rows[0]["trade_date"]) == "2026-06-01"
        assert str(rows[2]["trade_date"]) == "2026-06-02"

    def test_trade_hour_from_open_time(self, df):
        result = df.withColumn("trade_hour", hour("open_time"))
        rows = result.collect()
        assert rows[0]["trade_hour"] == 0
        assert rows[1]["trade_hour"] == 1

    def test_day_of_week_from_open_time(self, df):
        result = df.withColumn("day_of_week", date_format("open_time", "E"))
        rows = result.collect()
        assert len(rows[0]["day_of_week"]) > 0


class TestSparkVolatilityRanking:

    def test_avg_price_range_per_symbol(self, df_with_derived):
        result = df_with_derived.groupBy("symbol").agg(avg("price_range").alias("avg_price_range"))
        rows = {r["symbol"]: round(r["avg_price_range"], 2) for r in result.collect()}
        assert rows["BTCUSDT"] == pytest.approx(15.0)
        assert rows["ETHUSDT"] == pytest.approx(1.5)

    def test_volatility_rank_higher_for_wider_range(self, df_with_derived):
        vol = df_with_derived.groupBy("symbol").agg(
            avg("price_range").alias("avg_price_range")
        )
        vol = vol.withColumn(
            "volatility_rank",
            row_number().over(Window.orderBy(col("avg_price_range").desc()))
        )
        rows = vol.orderBy("volatility_rank").collect()
        assert rows[0]["symbol"] == "BTCUSDT"
        assert rows[0]["volatility_rank"] == 1
        assert rows[1]["symbol"] == "ETHUSDT"
        assert rows[1]["volatility_rank"] == 2


class TestSparkActivityRanking:

    def test_activity_score_formula(self, df_with_derived):
        activity = df_with_derived.groupBy("symbol").agg(
            spark_sum("trade_count").alias("total_trades"),
            spark_sum("quote_volume").alias("total_quote_volume"),
        )
        activity = activity.withColumn(
            "activity_score",
            col("total_trades") * log(col("total_quote_volume") + lit(1))
        )
        rows = {r["symbol"]: r["activity_score"] for r in activity.collect()}
        btc_expected = 3000 * math.log(1500000 + 1)
        eth_expected = 1500 * math.log(600000 + 1)
        assert rows["BTCUSDT"] == pytest.approx(btc_expected, rel=1e-2)
        assert rows["ETHUSDT"] == pytest.approx(eth_expected, rel=1e-2)

    def test_activity_rank_higher_for_busier_symbol(self, df_with_derived):
        activity = df_with_derived.groupBy("symbol").agg(
            spark_sum("trade_count").alias("total_trades"),
            spark_sum("quote_volume").alias("total_quote_volume"),
        )
        activity = activity.withColumn(
            "activity_score",
            col("total_trades") * log(col("total_quote_volume") + lit(1))
        )
        activity = activity.withColumn(
            "activity_rank",
            row_number().over(Window.orderBy(col("activity_score").desc()))
        )
        rows = activity.orderBy("activity_rank").collect()
        assert rows[0]["symbol"] == "BTCUSDT"
        assert rows[0]["activity_rank"] == 1
        assert rows[1]["symbol"] == "ETHUSDT"
        assert rows[1]["activity_rank"] == 2


class TestSparkActivityByTime:

    def test_hour_activity_score(self, df_with_derived):
        df_with_derived.createOrReplaceTempView("test_time_data")
        hour_df = df_with_derived.sparkSession.sql(
            "SELECT trade_hour, SUM(trade_count) AS total_trades, "
            "SUM(quote_volume) AS total_quote_volume "
            "FROM (SELECT *, hour(open_time) AS trade_hour FROM test_time_data) "
            "WHERE trade_hour IS NOT NULL "
            "GROUP BY trade_hour"
        )
        hour_df = hour_df.withColumn(
            "activity_score",
            col("total_trades") * log(col("total_quote_volume") + lit(1))
        )
        busiest = hour_df.orderBy(col("activity_score").desc()).limit(1)
        row = busiest.collect()[0]
        assert row["trade_hour"] == 0
        assert row["activity_score"] > 0
        df_with_derived.sparkSession.catalog.dropTempView("test_time_data")

    def test_date_activity_score(self, df_with_derived):
        df_with_derived.createOrReplaceTempView("test_date_data")
        date_df = df_with_derived.sparkSession.sql(
            "SELECT trade_date, SUM(trade_count) AS total_trades, "
            "SUM(quote_volume) AS total_quote_volume "
            "FROM (SELECT *, to_date(open_time) AS trade_date FROM test_date_data) "
            "WHERE trade_date IS NOT NULL "
            "GROUP BY trade_date"
        )
        date_df = date_df.withColumn(
            "activity_score",
            col("total_trades") * log(col("total_quote_volume") + lit(1))
        )
        busiest = date_df.orderBy(col("activity_score").desc()).limit(1)
        row = busiest.collect()[0]
        assert str(row["trade_date"]) == "2026-06-01"
        df_with_derived.sparkSession.catalog.dropTempView("test_date_data")


class TestSparkFinalSummary:

    def test_final_summary_joins_three_views(self, df_with_derived):
        df_with_derived.createOrReplaceTempView("market_data")
        df_with_derived.withColumn("trade_date", to_date("open_time"))
        df_with_derived.withColumn("trade_hour", hour("open_time"))

        symbol_counts = df_with_derived.sparkSession.sql(
            "SELECT symbol, COUNT(*) AS row_count FROM market_data GROUP BY symbol"
        )
        symbol_counts.createOrReplaceTempView("symbol_counts")

        volatility = df_with_derived.sparkSession.sql(
            "SELECT symbol, ROUND(AVG(price_range), 2) AS avg_price_range "
            "FROM market_data WHERE price_range IS NOT NULL GROUP BY symbol"
        )
        volatility = volatility.withColumn(
            "volatility_rank",
            row_number().over(Window.orderBy(col("avg_price_range").desc()))
        )
        volatility.createOrReplaceTempView("volatility_results")

        activity = df_with_derived.sparkSession.sql(
            "SELECT symbol, SUM(trade_count) AS total_trades, "
            "SUM(quote_volume) AS total_quote_volume "
            "FROM market_data WHERE trade_count IS NOT NULL GROUP BY symbol"
        )
        activity = activity.withColumn(
            "activity_score",
            col("total_trades") * log(col("total_quote_volume") + lit(1))
        )
        activity = activity.withColumn(
            "activity_rank",
            row_number().over(Window.orderBy(col("activity_score").desc()))
        )
        activity.createOrReplaceTempView("activity_results")

        summary = df_with_derived.sparkSession.sql(
            "SELECT v.symbol, rc.row_count, v.avg_price_range, "
            "a.total_trades, a.total_quote_volume, "
            "a.activity_score, a.activity_rank, v.volatility_rank "
            "FROM market_data m "
            "JOIN volatility_results v ON m.symbol = v.symbol "
            "JOIN activity_results a ON m.symbol = a.symbol "
            "JOIN symbol_counts rc ON m.symbol = rc.symbol "
            "GROUP BY v.symbol, rc.row_count, v.avg_price_range, "
            "v.volatility_rank, a.total_trades, a.total_quote_volume, "
            "a.activity_score, a.activity_rank "
            "ORDER BY a.activity_rank"
        )
        rows = summary.collect()
        assert len(rows) == 2
        assert rows[0]["symbol"] == "BTCUSDT"
        assert rows[0]["row_count"] == 3
        assert rows[0]["activity_rank"] == 1

        df_with_derived.sparkSession.catalog.dropTempView("market_data")
        df_with_derived.sparkSession.catalog.dropTempView("symbol_counts")
        df_with_derived.sparkSession.catalog.dropTempView("volatility_results")
        df_with_derived.sparkSession.catalog.dropTempView("activity_results")


class TestSparkCsvSave:

    def test_save_summary_as_single_csv(self, spark, tmp_path):
        data = [("A", 1), ("B", 2)]
        df = spark.createDataFrame(data, ["symbol", "value"])
        out = tmp_path / "test.csv"
        df.toPandas().to_csv(out, index=False)
        assert out.exists()
        assert out.is_file()
        content = out.read_text()
        assert "symbol,value\nA,1\nB,2\n" == content
