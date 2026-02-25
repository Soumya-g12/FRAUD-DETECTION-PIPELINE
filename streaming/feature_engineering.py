"""
PySpark job for real-time feature computation.
Runs on Databricks, outputs to feature store.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, avg, sum as spark_sum

spark = SparkSession.builder \
    .appName("FraudFeatures") \
    .getOrCreate()

def compute_aggregates(stream_df):
    """
    Windowed aggregates for transaction patterns.
    Critical for fraud signals: velocity, amount trends.
    """
    return stream_df \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            col("account_id")
        ) \
        .agg(
            count("*").alias("tx_count_5m"),
            avg("amount").alias("avg_amount_5m"),
            spark_sum("amount").alias("total_amount_5m")
        )

def compute_velocity_features(df):
    """Transaction velocity features for fraud detection."""
    return df \
        .groupBy("account_id") \
        .agg(
            count("*").alias("tx_count_24h"),
            avg("amount").alias("avg_tx_24h")
        )

# Read from Kinesis, process, write to feature store
# Actual implementation uses boto3 and Delta Lake
