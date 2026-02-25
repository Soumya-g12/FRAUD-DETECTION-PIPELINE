"""
Databricks job definitions for batch and streaming processing.
"""
from pyspark.sql import SparkSession

def run_batch_feature_engineering():
    """Daily batch job for feature computation."""
    spark = SparkSession.builder.appName("BatchFeatures").getOrCreate()
    
    # Read transaction history
    df = spark.read.format("delta").load("/mnt/transactions")
    
    # Compute features
    features = df.groupBy("account_id").agg(
        count("*").alias("total_tx_count"),
        avg("amount").alias("avg_tx_amount"),
        max("timestamp").alias("last_tx_time")
    )
    
    # Write to feature store
    features.write.format("delta").mode("overwrite").save("/mnt/features/account")
    
    spark.stop()

def optimize_tables():
    """Optimize Delta tables for query performance."""
    spark = SparkSession.builder.appName("Optimize").getOrCreate()
    
    spark.sql("OPTIMIZE delta.`/mnt/transactions` ZORDER BY (account_id)")
    spark.sql("VACUUM delta.`/mnt/transactions` RETAIN 168 HOURS")
    
    spark.stop()
