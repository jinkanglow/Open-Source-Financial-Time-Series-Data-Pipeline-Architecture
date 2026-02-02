"""
Spark Streaming Job for Real-time Feature Updates

Processes Kafka stream and updates Delta Lake
Based on: hoangsonww/End-to-End-Data-Pipeline
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, first, last, max as spark_max, min as spark_min,
    sum as spark_sum, avg, stddev, when, lag, lit, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, LongType
)
from delta import configure_spark_with_delta_pip
import os


def create_spark_session():
    """Create Spark session with Delta Lake and Kafka support"""
    builder = SparkSession.builder \
        .appName("FinancialTimeseriesStreamingJob") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.streaming.schemaInference", "true")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def create_market_data_schema():
    """Create schema for market data"""
    return StructType([
        StructField("time", LongType(), True),
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("trade_id", StringType(), True),
        StructField("side", StringType(), True),
        StructField("bid", DoubleType(), True),
        StructField("ask", DoubleType(), True),
    ])


def process_market_data_stream(spark, kafka_bootstrap_servers, topics, output_path):
    """
    Process Kafka stream and write to Delta Lake
    
    This maintains the single source of truth: Kafka → Delta Lake
    """
    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topics) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON
    schema = create_market_data_schema()
    df_parsed = df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    # Convert timestamp
    df_with_time = df_parsed.withColumn(
        "time",
        to_timestamp(col("time") / 1000)  # Convert from milliseconds
    )
    
    # Write to Delta Lake with merge
    def write_to_delta(batch_df, batch_id):
        """Write each batch to Delta Lake"""
        batch_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(output_path)
    
    query = df_with_time.writeStream \
        .foreachBatch(write_to_delta) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoint/market_data") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    return query


def calculate_streaming_ohlc(spark, input_path, output_path):
    """
    Calculate streaming OHLC aggregates
    
    Updates Delta Lake with 1-minute OHLC in real-time
    """
    df = spark.readStream.format("delta").load(input_path)
    
    # Calculate 1-minute OHLC with watermark
    df_ohlc = df \
        .withWatermark("time", "1 minute") \
        .groupBy(
            window(col("time"), "1 minute"),
            col("symbol")
        ) \
        .agg(
            first("price", ignorenulls=True).alias("open"),
            spark_max("price").alias("high"),
            spark_min("price").alias("low"),
            last("price", ignorenulls=True).alias("close"),
            spark_sum("volume").alias("volume")
        ) \
        .select(
            col("window.start").alias("minute"),
            col("symbol"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume")
        )
    
    # Write to Delta Lake
    def write_ohlc_to_delta(batch_df, batch_id):
        """Write OHLC aggregates to Delta Lake"""
        batch_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(output_path)
    
    query = df_ohlc.writeStream \
        .foreachBatch(write_ohlc_to_delta) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoint/ohlc") \
        .trigger(processingTime="1 minute") \
        .start()
    
    return query


def main():
    """Main streaming job entry point"""
    spark = create_spark_session()
    
    # Configuration
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topics = os.getenv("KAFKA_TOPICS", "market-data")
    raw_output = os.getenv("RAW_OUTPUT", "s3://feature-store/raw/market_data")
    ohlc_output = os.getenv("OHLC_OUTPUT", "s3://feature-store/processed/ohlc_1m")
    
    # Process market data stream
    print("Starting market data stream processing...")
    market_data_query = process_market_data_stream(
        spark,
        kafka_bootstrap_servers,
        kafka_topics,
        raw_output
    )
    
    # Calculate streaming OHLC
    print("Starting OHLC calculation...")
    ohlc_query = calculate_streaming_ohlc(
        spark,
        raw_output,
        ohlc_output
    )
    
    # Wait for queries
    print("Streaming jobs started. Waiting for termination...")
    market_data_query.awaitTermination()
    ohlc_query.awaitTermination()


if __name__ == "__main__":
    main()
