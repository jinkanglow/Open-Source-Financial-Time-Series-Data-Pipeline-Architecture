"""
Spark Batch Job for Feature Calculation

Calculates SMA_20 and other features for historical data
Based on: hoangsonww/End-to-End-Data-Pipeline
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, window, first, last, max as spark_max, min as spark_min,
    sum as spark_sum, stddev, when, lag, lead, lit
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from delta import configure_spark_with_delta_pip
import os


def create_spark_session():
    """Create Spark session with Delta Lake support"""
    builder = SparkSession.builder \
        .appName("FinancialTimeseriesBatchJob") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def calculate_sma_20(spark, input_path, output_path):
    """
    Calculate 20-period Simple Moving Average
    
    This matches the TimescaleDB continuous aggregate calculation
    """
    # Read market data from Delta Lake
    df = spark.read.format("delta").load(input_path)
    
    # Calculate SMA_20 using window function
    from pyspark.sql.window import Window
    
    window_spec = Window.partitionBy("symbol") \
        .orderBy("time") \
        .rowsBetween(-19, 0)  # 20 periods including current
    
    df_with_sma = df.withColumn(
        "sma_20",
        avg("price").over(window_spec)
    )
    
    # Select only the columns we need
    result = df_with_sma.select(
        col("time"),
        col("symbol"),
        col("price"),
        col("sma_20")
    )
    
    # Write to Delta Lake
    result.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(output_path)
    
    return result


def calculate_volatility_1h(spark, input_path, output_path):
    """
    Calculate 1-hour volatility (stddev of log returns)
    
    This matches TimescaleDB volatility_1h_agg
    """
    df = spark.read.format("delta").load(input_path)
    
    # Calculate log returns
    from pyspark.sql.window import Window
    
    window_spec = Window.partitionBy("symbol").orderBy("time")
    
    df_with_returns = df.withColumn(
        "log_return",
        when(
            lag("price", 1).over(window_spec).isNotNull(),
            (col("price") / lag("price", 1).over(window_spec)).log()
        ).otherwise(lit(0.0))
    )
    
    # Calculate hourly volatility
    df_hourly = df_with_returns \
        .withColumn("hour", window(col("time"), "1 hour").start) \
        .groupBy("hour", "symbol") \
        .agg(
            stddev("log_return").alias("volatility_1h"),
            spark_sum("volume").alias("total_volume")
        )
    
    # Write to Delta Lake
    df_hourly.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(output_path)
    
    return df_hourly


def calculate_ohlc_1m(spark, input_path, output_path):
    """
    Calculate 1-minute OHLC aggregates
    
    This matches TimescaleDB ohlc_1m_agg continuous aggregate
    """
    df = spark.read.format("delta").load(input_path)
    
    # Calculate 1-minute OHLC
    df_ohlc = df \
        .withColumn("minute", window(col("time"), "1 minute").start) \
        .groupBy("minute", "symbol") \
        .agg(
            first("price", ignorenulls=True).alias("open"),
            spark_max("price").alias("high"),
            spark_min("price").alias("low"),
            last("price", ignorenulls=True).alias("close"),
            spark_sum("volume").alias("volume"),
            spark_sum(lit(1)).alias("trade_count")
        )
    
    # Write to Delta Lake
    df_ohlc.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(output_path)
    
    return df_ohlc


def calculate_vwap_5m(spark, input_path, output_path):
    """
    Calculate 5-minute Volume Weighted Average Price
    
    VWAP = sum(price * volume) / sum(volume)
    """
    df = spark.read.format("delta").load(input_path)
    
    # Calculate 5-minute VWAP
    df_vwap = df \
        .withColumn("five_min", window(col("time"), "5 minutes").start) \
        .groupBy("five_min", "symbol") \
        .agg(
            (spark_sum(col("price") * col("volume")) / spark_sum("volume")).alias("vwap_5m"),
            spark_sum("volume").alias("total_volume")
        )
    
    # Write to Delta Lake
    df_vwap.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(output_path)
    
    return df_vwap


def main():
    """Main batch job entry point"""
    spark = create_spark_session()
    
    # Configuration
    input_path = os.getenv("INPUT_PATH", "s3://feature-store/raw/market_data")
    output_base = os.getenv("OUTPUT_BASE", "s3://feature-store/processed")
    
    # Calculate all features
    print("Calculating SMA_20...")
    calculate_sma_20(
        spark,
        input_path,
        f"{output_base}/sma_20"
    )
    
    print("Calculating volatility_1h...")
    calculate_volatility_1h(
        spark,
        input_path,
        f"{output_base}/volatility_1h"
    )
    
    print("Calculating OHLC_1m...")
    calculate_ohlc_1m(
        spark,
        input_path,
        f"{output_base}/ohlc_1m"
    )
    
    print("Calculating VWAP_5m...")
    calculate_vwap_5m(
        spark,
        input_path,
        f"{output_base}/vwap_5m"
    )
    
    print("Batch job completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
