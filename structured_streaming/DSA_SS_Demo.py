# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Structured Streaming Demo
# MAGIC ## Solutions Architect Demo: Common Patterns, Issues, and Best Practices
# MAGIC 
# MAGIC This demo covers:
# MAGIC - **Common Issues**: Skew, Spill, Shuffle
# MAGIC - **Key Settings**: maxBytesPerTrigger, maxFilesPerTrigger, ProcessingTime
# MAGIC - **Output Modes**: Append, Complete, Update
# MAGIC - **Merge Operations**: Delta Lake streaming merges
# MAGIC - **Stateful vs Stateless**: Aggregations, windows, joins
# MAGIC - **Watermarking**: Handling late data
# MAGIC - **ForEachBatch**: Custom processing logic
# MAGIC - **Gotchas**: Common pitfalls and solutions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# MAGIC %pip install dbldatagen==0.4.0

# COMMAND ----------

import random
import time
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQuery
from delta import DeltaTable
import dbldatagen as dg

# Configuration
catalog_name = "dlt_demo_lr"
schema_name = "streaming_demo"
volume_name = "raw_data"

# Create catalog and schema if they don't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# Create volume for raw data
try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")
except:
    print("Volume already exists or permission issue")

volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
checkpoint_path = f"{volume_path}/checkpoints"
output_path = f"{volume_path}/output"

# Create directories
dbutils.fs.mkdirs(f"{volume_path}/source_data")
dbutils.fs.mkdirs(checkpoint_path)
dbutils.fs.mkdirs(output_path)

print(f"Volume path: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generation
# MAGIC Creating realistic taxi trip data with patterns that will demonstrate streaming concepts

# COMMAND ----------

def generate_taxi_trip_data(num_records=50000):
    """Generate realistic taxi trip data with potential skew and various patterns"""
    
    # NYC zip codes with intentional skew (some zips more common)
    nyc_zip_codes = [10001, 10002, 10003, 10004, 10005, 10006, 10007, 10008, 10009, 10010,
                     10011, 10012, 10013, 10014, 10016, 10017, 10018, 10019, 10020, 10021]
    
    # Create skewed distribution for pickup locations
    skewed_zips = [10001] * 40 + [10002] * 30 + [10003] * 20 + nyc_zip_codes[3:] * 2
    
    ds = (
        dg.DataGenerator(spark, name="taxi_trips", rows=num_records, partitions=4)
        .withIdOutput()
        .withColumn("trip_id", LongType(), minValue=1000000, maxValue=9999999, uniqueValues=num_records)
        .withColumn("taxi_id", IntegerType(), minValue=1, maxValue=1000, random=True)
        .withColumn("driver_id", IntegerType(), minValue=1000, maxValue=5000, random=True)
        .withColumn("passenger_count", IntegerType(), 
                   values=[1, 2, 3, 4, 5, 6], 
                   weights=[50, 30, 15, 3, 1, 1], random=True)
        .withColumn("pickup_zip", IntegerType(), values=skewed_zips, random=True)
        .withColumn("dropoff_zip", IntegerType(), values=nyc_zip_codes, random=True)
        .withColumn("trip_distance", FloatType(), minValue=0.1, maxValue=50.0, random=True)
        .withColumn("fare_amount", FloatType(), minValue=5.0, maxValue=500.0, random=True)
        .withColumn("tip_amount", FloatType(), minValue=0.0, maxValue=100.0, random=True)
        .withColumn("payment_type", StringType(), 
                   values=["credit_card", "cash", "mobile"], 
                   weights=[70, 25, 5], random=True)
        .withColumn("pickup_datetime", TimestampType(),
                   begin="2024-01-01 00:00:00",
                   end="2024-12-31 23:59:59",
                   interval="1 minute")
        .withColumn("vendor_id", IntegerType(), values=[1, 2, 3], random=True)
        .withColumn("rate_code", IntegerType(), values=[1, 2, 3, 4, 5], weights=[80, 10, 5, 3, 2])
    )
    
    return ds.build().drop("id")

# Generate initial batch of data
print("Generating initial taxi trip data...")
df = generate_taxi_trip_data(100000)

# Add calculated fields that will be useful for streaming demos
df_enhanced = (df
    .withColumn("pickup_hour", F.hour("pickup_datetime"))
    .withColumn("pickup_date", F.to_date("pickup_datetime"))
    .withColumn("total_amount", F.col("fare_amount") + F.col("tip_amount"))
    .withColumn("is_rush_hour", 
                F.when((F.col("pickup_hour").between(7, 9)) | 
                       (F.col("pickup_hour").between(17, 19)), True)
                .otherwise(False))
    .withColumn("event_timestamp", F.current_timestamp())
)

# Write initial data to volume
df_enhanced.write.mode("overwrite").json(f"{volume_path}/source_data/batch_001")
print(f"Initial data written to {volume_path}/source_data/batch_001")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Basic Streaming with Key Settings
# MAGIC ### maxBytesPerTrigger and maxFilesPerTrigger

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demonstration of maxBytesPerTrigger
# MAGIC This controls how much data is processed per trigger to prevent overwhelming the cluster

# Clean up any existing streams
for stream in spark.streams.active:
    stream.stop()

# Basic streaming read with maxBytesPerTrigger
def demo_max_bytes_per_trigger():
    print("=== Demo: maxBytesPerTrigger ===")
    
    # Read stream with small maxBytesPerTrigger for demonstration
    df_stream = (spark
        .readStream
        .format("json")
        .option("maxBytesPerTrigger", "1mb")  # Process only 1MB per trigger
        .option("multiLine", "true")
        .load(f"{volume_path}/source_data")
    )
    
    # Simple transformation
    df_processed = (df_stream
        .withColumn("processing_time", F.current_timestamp())
        .select("trip_id", "pickup_zip", "total_amount", "pickup_datetime", "processing_time")
    )
    
    # Write to console to observe trigger behavior
    query = (df_processed
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("numRows", 10)
        .trigger(processingTime="10 seconds")  # Trigger every 10 seconds
        .start()
    )
    
    # Let it run for a bit
    time.sleep(30)
    query.stop()
    
    return "maxBytesPerTrigger demo completed"

demo_max_bytes_per_trigger()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demonstration of maxFilesPerTrigger
# MAGIC This controls how many files are processed per trigger

def demo_max_files_per_trigger():
    print("=== Demo: maxFilesPerTrigger ===")
    
    # First, let's create multiple small files
    for i in range(10):
        small_df = generate_taxi_trip_data(1000)
        small_df.write.mode("overwrite").json(f"{volume_path}/source_data/small_batch_{i:03d}")
    
    # Read stream with maxFilesPerTrigger
    df_stream = (spark
        .readStream
        .format("json")
        .option("maxFilesPerTrigger", "2")  # Process only 2 files per trigger
        .load(f"{volume_path}/source_data/small_batch_*")
    )
    
    # Add processing metadata
    df_processed = (df_stream
        .withColumn("batch_id", F.spark_partition_id())
        .withColumn("processing_time", F.current_timestamp())
        .groupBy("batch_id")
        .agg(
            F.count("*").alias("record_count"),
            F.first("processing_time").alias("processed_at")
        )
    )
    
    query = (df_processed
        .writeStream
        .outputMode("complete")
        .format("console")
        .trigger(processingTime="15 seconds")
        .start()
    )
    
    time.sleep(45)
    query.stop()
    
    return "maxFilesPerTrigger demo completed"

demo_max_files_per_trigger()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Output Modes and Delta Lake Integration
# MAGIC ### Append vs Complete vs Update modes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Append Mode - Most Common for Streaming
# MAGIC New records are added to the result table

# Create Delta tables for different output modes
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.taxi_trips_append (
    trip_id BIGINT,
    pickup_zip INT,
    total_amount FLOAT,
    pickup_hour INT,
    pickup_date DATE,
    processing_time TIMESTAMP
) USING DELTA
LOCATION '{output_path}/taxi_trips_append'
""")

def demo_append_mode():
    print("=== Demo: Append Mode ===")
    
    df_stream = (spark
        .readStream
        .format("json")
        .option("maxBytesPerTrigger", "2mb")
        .load(f"{volume_path}/source_data")
    )
    
    df_processed = (df_stream
        .select("trip_id", "pickup_zip", "total_amount", "pickup_hour", "pickup_date")
        .withColumn("processing_time", F.current_timestamp())
    )
    
    query = (df_processed
        .writeStream
        .outputMode("append")
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/append_mode")
        .table(f"{catalog_name}.{schema_name}.taxi_trips_append")
        .trigger(processingTime="20 seconds")
        .start()
    )
    
    time.sleep(60)
    query.stop()
    
    # Check results
    result_count = spark.table(f"{catalog_name}.{schema_name}.taxi_trips_append").count()
    print(f"Records in append table: {result_count}")
    
    return "Append mode demo completed"

demo_append_mode()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Complete Mode - For Aggregations
# MAGIC Entire result table is outputted to the sink

# Create table for complete mode demo
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.hourly_trip_counts (
    pickup_hour INT,
    trip_count BIGINT,
    avg_fare DOUBLE,
    last_updated TIMESTAMP
) USING DELTA
LOCATION '{output_path}/hourly_trip_counts'
""")

def demo_complete_mode():
    print("=== Demo: Complete Mode ===")
    
    df_stream = (spark
        .readStream
        .format("json")
        .option("maxBytesPerTrigger", "2mb")
        .load(f"{volume_path}/source_data")
    )
    
    # Aggregation - requires complete mode
    df_aggregated = (df_stream
        .groupBy("pickup_hour")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare")
        )
        .withColumn("last_updated", F.current_timestamp())
    )
    
    query = (df_aggregated
        .writeStream
        .outputMode("complete")
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/complete_mode")
        .table(f"{catalog_name}.{schema_name}.hourly_trip_counts")
        .trigger(processingTime="30 seconds")
        .start()
    )
    
    time.sleep(90)
    query.stop()
    
    # Check results
    spark.table(f"{catalog_name}.{schema_name}.hourly_trip_counts").orderBy("pickup_hour").display()
    
    return "Complete mode demo completed"

demo_complete_mode()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Merge Operations with Delta Lake
# MAGIC ### Streaming Merge (Upsert) Operations

# COMMAND ----------

# Create target table for merge operations
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.taxi_driver_summary (
    driver_id INT,
    total_trips BIGINT,
    total_revenue DOUBLE,
    avg_trip_distance DOUBLE,
    last_trip_date DATE,
    last_updated TIMESTAMP
) USING DELTA
LOCATION '{output_path}/taxi_driver_summary'
""")

def demo_streaming_merge():
    print("=== Demo: Streaming Merge Operations ===")
    
    df_stream = (spark
        .readStream
        .format("json")
        .option("maxBytesPerTrigger", "1mb")
        .load(f"{volume_path}/source_data")
    )
    
    # Aggregate by driver
    df_driver_stats = (df_stream
        .groupBy("driver_id")
        .agg(
            F.count("*").alias("total_trips"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.max("pickup_date").alias("last_trip_date")
        )
        .withColumn("last_updated", F.current_timestamp())
    )
    
    def merge_driver_stats(df, epoch_id):
        """Custom merge logic using foreachBatch"""
        target_table = DeltaTable.forName(spark, f"{catalog_name}.{schema_name}.taxi_driver_summary")
        
        # Merge operation
        (target_table.alias("target")
            .merge(df.alias("source"), "target.driver_id = source.driver_id")
            .whenMatchedUpdate(set={
                "total_trips": "target.total_trips + source.total_trips",
                "total_revenue": "target.total_revenue + source.total_revenue",
                "avg_trip_distance": "(target.avg_trip_distance + source.avg_trip_distance) / 2",
                "last_trip_date": "GREATEST(target.last_trip_date, source.last_trip_date)",
                "last_updated": "source.last_updated"
            })
            .whenNotMatchedInsert(values={
                "driver_id": "source.driver_id",
                "total_trips": "source.total_trips",
                "total_revenue": "source.total_revenue",
                "avg_trip_distance": "source.avg_trip_distance",
                "last_trip_date": "source.last_trip_date",
                "last_updated": "source.last_updated"
            })
            .execute()
        )
        
        print(f"Processed batch {epoch_id}")
    
    query = (df_driver_stats
        .writeStream
        .foreachBatch(merge_driver_stats)
        .option("checkpointLocation", f"{checkpoint_path}/merge_demo")
        .trigger(processingTime="30 seconds")
        .start()
    )
    
    time.sleep(120)
    query.stop()
    
    # Check results
    print("Driver summary after merge:")
    spark.table(f"{catalog_name}.{schema_name}.taxi_driver_summary").orderBy("total_revenue", ascending=False).limit(10).display()
    
    return "Streaming merge demo completed"

demo_streaming_merge()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Stateful vs Stateless Transformations
# MAGIC ### Understanding the difference and performance implications

# COMMAND ----------

def demo_stateless_transformations():
    print("=== Demo: Stateless Transformations ===")
    print("Stateless: map, filter, select, withColumn - no state between batches")
    
    df_stream = (spark
        .readStream
        .format("json")
        .option("maxBytesPerTrigger", "1mb")
        .load(f"{volume_path}/source_data")
    )
    
    # Stateless transformations - each record processed independently
    df_stateless = (df_stream
        .filter(F.col("fare_amount") > 10)  # Filter
        .select("trip_id", "pickup_zip", "fare_amount", "tip_amount")  # Select
        .withColumn("tip_percentage", F.col("tip_amount") / F.col("fare_amount") * 100)  # WithColumn
        .withColumn("fare_category", 
                   F.when(F.col("fare_amount") < 15, "low")
                   .when(F.col("fare_amount") < 50, "medium")
                   .otherwise("high"))  # Conditional logic
    )
    
    query = (df_stateless
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("numRows", 5)
        .trigger(processingTime="20 seconds")
        .start()
    )
    
    time.sleep(60)
    query.stop()
    
    return "Stateless transformations demo completed"

def demo_stateful_transformations():
    print("\n=== Demo: Stateful Transformations ===")
    print("Stateful: aggregations, window operations, joins - maintain state between batches")
    
    df_stream = (spark
        .readStream
        .format("json")
        .option("maxBytesPerTrigger", "1mb")
        .load(f"{volume_path}/source_data")
    )
    
    # Stateful transformation - aggregation maintains state
    df_stateful = (df_stream
        .withWatermark("pickup_datetime", "1 hour")  # Handle late data
        .groupBy(
            F.window("pickup_datetime", "30 minutes", "15 minutes"),  # Sliding window
            "pickup_zip"
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
            F.sum("total_amount").alias("total_revenue")
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "pickup_zip",
            "trip_count",
            "avg_fare",
            "total_revenue"
        )
    )
    
    query = (df_stateful
        .writeStream
        .outputMode("append")  # Can use append with watermark
        .format("console")
        .option("truncate", False)
        .option("numRows", 10)
        .trigger(processingTime="30 seconds")
        .start()
    )
    
    time.sleep(90)
    query.stop()
    
    return "Stateful transformations demo completed"

# Run both demos
demo_stateless_transformations()
demo_stateful_transformations()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Watermarking for Late Data
# MAGIC ### Handling out-of-order events

# COMMAND ----------

def demo_watermarking():
    print("=== Demo: Watermarking for Late Data ===")
    
    # Create some late data by modifying timestamps
    df_late_data = generate_taxi_trip_data(5000)
    df_late_data_modified = (df_late_data
        .withColumn("pickup_datetime", 
                   F.col("pickup_datetime") - F.expr("INTERVAL 2 HOURS"))  # Make data 2 hours late
        .withColumn("event_timestamp", F.current_timestamp())
    )
    
    # Write late data to a separate location
    df_late_data_modified.write.mode("overwrite").json(f"{volume_path}/source_data/late_data")
    
    df_stream = (spark
        .readStream
        .format("json")
        .option("maxBytesPerTrigger", "1mb")
        .load(f"{volume_path}/source_data")
    )
    
    # Demo without watermarking (will accumulate state indefinitely)
    print("Without watermarking - state grows indefinitely")
    df_no_watermark = (df_stream
        .groupBy(
            F.window("pickup_datetime", "1 hour"),
            "pickup_zip"
        )
        .agg(F.count("*").alias("trip_count"))
    )
    
    query_no_watermark = (df_no_watermark
        .writeStream
        .outputMode("complete")
        .format("memory")
        .queryName("no_watermark_demo")
        .trigger(processingTime="20 seconds")
        .start()
    )
    
    time.sleep(60)
    query_no_watermark.stop()
    
    # Demo with watermarking (manages state efficiently)
    print("\nWith watermarking - state is managed efficiently")
    df_with_watermark = (df_stream
        .withWatermark("pickup_datetime", "1 hour")  # Allow 1 hour late data
        .groupBy(
            F.window("pickup_datetime", "1 hour"),
            "pickup_zip"
        )
        .agg(F.count("*").alias("trip_count"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "pickup_zip",
            "trip_count"
        )
    )
    
    query_with_watermark = (df_with_watermark
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="20 seconds")
        .start()
    )
    
    time.sleep(60)
    query_with_watermark.stop()
    
    return "Watermarking demo completed"

demo_watermarking()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: ForEachBatch - Custom Processing Logic
# MAGIC ### Advanced processing patterns

# COMMAND ----------

def demo_foreach_batch():
    print("=== Demo: ForEachBatch - Custom Processing ===")
    
    # Create tables for different outputs
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.high_value_trips (
        trip_id BIGINT,
        driver_id INT,
        total_amount FLOAT,
        pickup_datetime TIMESTAMP,
        alert_generated TIMESTAMP
    ) USING DELTA
    LOCATION '{output_path}/high_value_trips'
    """)
    
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.trip_alerts (
        alert_id STRING,
        trip_id BIGINT,
        alert_type STRING,
        alert_message STRING,
        created_at TIMESTAMP
    ) USING DELTA
    LOCATION '{output_path}/trip_alerts'
    """)
    
    df_stream = (spark
        .readStream
        .format("json")
        .option("maxBytesPerTrigger", "1mb")
        .load(f"{volume_path}/source_data")
    )
    
    def process_batch(df, epoch_id):
        """Custom processing logic for each batch"""
        print(f"Processing batch {epoch_id}")
        
        # Cache the dataframe as we'll use it multiple times
        df.cache()
        
        # 1. Filter and save high-value trips
        high_value_trips = (df
            .filter(F.col("total_amount") > 100)
            .select("trip_id", "driver_id", "total_amount", "pickup_datetime")
            .withColumn("alert_generated", F.current_timestamp())
        )
        
        if high_value_trips.count() > 0:
            (high_value_trips
                .write
                .mode("append")
                .format("delta")
                .saveAsTable(f"{catalog_name}.{schema_name}.high_value_trips")
            )
            print(f"Saved {high_value_trips.count()} high-value trips")
        
        # 2. Generate alerts for suspicious patterns
        alerts_df = df.select("trip_id", "fare_amount", "tip_amount", "trip_distance")
        
        # Detect potential anomalies
        suspicious_tips = alerts_df.filter(
            (F.col("tip_amount") / F.col("fare_amount")) > 2  # Tip > 200% of fare
        ).withColumn("alert_type", F.lit("HIGH_TIP_RATIO"))
        
        suspicious_distance = alerts_df.filter(
            (F.col("fare_amount") / F.col("trip_distance")) > 50  # Very high fare per mile
        ).withColumn("alert_type", F.lit("HIGH_FARE_PER_MILE"))
        
        all_alerts = suspicious_tips.union(suspicious_distance)
        
        if all_alerts.count() > 0:
            alerts_final = (all_alerts
                .withColumn("alert_id", F.concat(F.lit("ALERT_"), F.col("trip_id")))
                .withColumn("alert_message", 
                           F.when(F.col("alert_type") == "HIGH_TIP_RATIO", "Unusually high tip ratio detected")
                           .otherwise("High fare per mile detected"))
                .withColumn("created_at", F.current_timestamp())
                .select("alert_id", "trip_id", "alert_type", "alert_message", "created_at")
            )
            
            (alerts_final
                .write
                .mode("append")
                .format("delta")
                .saveAsTable(f"{catalog_name}.{schema_name}.trip_alerts")
            )
            print(f"Generated {alerts_final.count()} alerts")
        
        # 3. Custom logging/monitoring
        batch_stats = df.agg(
            F.count("*").alias("total_records"),
            F.avg("fare_amount").alias("avg_fare"),
            F.max("total_amount").alias("max_total"),
            F.min("pickup_datetime").alias("earliest_pickup"),
            F.max("pickup_datetime").alias("latest_pickup")
        ).collect()[0]
        
        print(f"Batch {epoch_id} stats:")
        print(f"  - Total records: {batch_stats['total_records']}")
        print(f"  - Average fare: ${batch_stats['avg_fare']:.2f}")
        print(f"  - Max total: ${batch_stats['max_total']:.2f}")
        print(f"  - Time range: {batch_stats['earliest_pickup']} to {batch_stats['latest_pickup']}")
        
        df.unpersist()
    
    query = (df_stream
        .writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", f"{checkpoint_path}/foreach_batch_demo")
        .trigger(processingTime="30 seconds")
        .start()
    )
    
    time.sleep(120)
    query.stop()
    
    # Check results
    print("\nHigh-value trips:")
    spark.table(f"{catalog_name}.{schema_name}.high_value_trips").orderBy("total_amount", ascending=False).display()
    
    print("\nGenerated alerts:")
    spark.table(f"{catalog_name}.{schema_name}.trip_alerts").display()
    
    return "ForEachBatch demo completed"

demo_foreach_batch()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Common Issues and Solutions
# MAGIC ### Skew, Spill, and Shuffle Problems

# COMMAND ----------

def demo_skew_issues():
    print("=== Demo: Data Skew Issues ===")
    
    # Create heavily skewed data
    skewed_data = generate_taxi_trip_data(50000)
    
    # Make 80% of data go to one partition (extreme skew)
    skewed_data_extreme = (skewed_data
        .withColumn("pickup_zip", F.when(F.rand() < 0.8, F.lit(10001)).otherwise(F.col("pickup_zip")))
    )
    
    skewed_data_extreme.write.mode("overwrite").json(f"{volume_path}/source_data/skewed_data")
    
    print("Demonstrating skew in streaming aggregation...")
    
    df_stream = (spark
        .readStream
        .format("json")
        .option("maxBytesPerTrigger", "2mb")
        .load(f"{volume_path}/source_data/skewed_data")
    )
    
    # This will cause skew - one partition handles most data
    df_skewed_agg = (df_stream
        .groupBy("pickup_zip")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
            F.sum("total_amount").alias("total_revenue")
        )
    )
    
    query_skewed = (df_skewed_agg
        .writeStream
        .outputMode("complete")
        .format("memory")
        .queryName("skewed_demo")
        .trigger(processingTime="20 seconds")
        .start()
    )
    
    time.sleep(60)
    query_skewed.stop()
    
    # Show the skew
    skew_results = spark.sql("SELECT * FROM skewed_demo ORDER BY trip_count DESC")
    print("Skew demonstration - notice the imbalance:")
    skew_results.display()
    
    # Solution: Add salt to reduce skew
    print("\nSolution: Using salt to reduce skew")
    
    df_stream_salted = (spark
        .readStream
        .format("json")
        .option("maxBytesPerTrigger", "2mb")
        .load(f"{volume_path}/source_data/skewed_data")
    )
    
    # Add salt to distribute load
    df_salted = (df_stream_salted
        .withColumn("salt", (F.rand() * 10).cast("int"))
        .withColumn("salted_key", F.concat(F.col("pickup_zip"), F.lit("_"), F.col("salt")))
    )
    
    # First level aggregation with salt
    df_salted_agg = (df_salted
        .groupBy("salted_key", "pickup_zip")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("fare_amount").alias("fare_sum"),
            F.sum("total_amount").alias("total_revenue")
        )
    )
    
    # Second level aggregation to combine salted results
    df_final_agg = (df_salted_agg
        .groupBy("pickup_zip")
        .agg(
            F.sum("trip_count").alias("trip_count"),
            F.avg("fare_sum").alias("avg_fare"),
            F.sum("total_revenue").alias("total_revenue")
        )
    )
    
    query_salted = (df_final_agg
        .writeStream
        .outputMode("complete")
        .format("console")
        .trigger(processingTime="30 seconds")
        .start()
    )
    
    time.sleep(90)
    query_salted.stop()
    
    return "Skew demo completed"

demo_skew_issues()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Performance Gotchas and Best Practices
# MAGIC ### Common pitfalls and how to avoid them

# COMMAND ----------

def demo_performance_gotchas():
    print("=== Demo: Performance Gotchas ===")
    
    # Gotcha 1: Too frequent triggers
    print("Gotcha 1: Too frequent triggers (< 1 second)")
    print("This can cause overhead and poor performance")
    
    df_stream = (spark
        .readStream
        .format("json")
        .option("maxBytesPerTrigger", "500kb")
        .load(f"{volume_path}/source_data")
    )
    
    # BAD: Too frequent triggers
    print("BAD: Trigger every 500ms")
    query_bad = (df_stream
        .select("trip_id", "pickup_zip", "fare_amount")
        .writeStream
        .outputMode("append")
        .format("console")
        .option("numRows", 3)
        .trigger(processingTime="500 milliseconds")  # TOO FREQUENT!
        .start()
    )
    
    time.sleep(15)
    query_bad.stop()
    
    # GOOD: Reasonable trigger interval
    print("\nGOOD: Trigger every 10 seconds")
    query_good = (df_stream
        .select("trip_id", "pickup_zip", "fare_amount")
        .writeStream
        .outputMode("append")
        .format("console")
        .option("numRows", 3)
        .trigger(processingTime="10 seconds")  # REASONABLE
        .start()
    )
    
    time.sleep(30)
    query_good.stop()
    
    # Gotcha 2: Not using maxBytesPerTrigger with auto-scaling
    print("\nGotcha 2: Processing too much data per trigger")
    print("This can overwhelm the cluster and cause memory issues")
    
    # Gotcha 3: Inefficient stateful operations
    print("\nGotcha 3: Inefficient stateful operations without watermarking")
    print("State can grow indefinitely without proper watermarking")
    
    # Gotcha 4: Incorrect output modes
    print("\nGotcha 4: Using wrong output mode")
    print("Complete mode with non-aggregated data will fail")
    
    try:
        # This will fail
        query_fail = (df_stream
            .select("trip_id", "fare_amount")  # No aggregation
            .writeStream
            .outputMode("complete")  # Complete mode requires aggregation
            .format("console")
            .start()
        )
        time.sleep(5)
        query_fail.stop()
    except Exception as e:
        print(f"Expected error: {e}")
    
    return "Performance gotchas demo completed"

def demo_best_practices():
    print("\n=== Best Practices Summary ===")
    
    best_practices = """
    1. TRIGGER INTERVALS:
       - Use processingTime >= 1 second for most cases
       - Use continuous processing only for very low latency requirements
       - Monitor trigger execution time vs interval
    
    2. BATCH SIZE CONTROL:
       - Use maxBytesPerTrigger for consistent processing loads
       - Start with 100MB-1GB per trigger, adjust based on cluster size
       - Use maxFilesPerTrigger for many small files
    
    3. WATERMARKING:
       - Always use watermarking with time-based aggregations
       - Set watermark threshold based on expected late data
       - Monitor late data metrics
    
    4. STATEFUL OPERATIONS:
       - Minimize state size with proper watermarking
       - Use appropriate partitioning for stateful operations
       - Consider state store memory requirements
    
    5. OUTPUT MODES:
       - Use Append for most streaming scenarios
       - Use Complete only for small result sets
       - Use Update for aggregations with watermarking
    
    6. DELTA LAKE INTEGRATION:
       - Use Delta Lake for all streaming outputs
       - Enable auto-compaction for streaming tables
       - Use OPTIMIZE and VACUUM regularly
    
    7. MONITORING:
       - Monitor streaming metrics (inputRowsPerSecond, processingTime)
       - Set up alerts for streaming failures
       - Use Spark UI to identify bottlenecks
    
    8. ERROR HANDLING:
       - Implement proper error handling in foreachBatch
       - Use retry logic for transient failures
       - Set up dead letter queues for bad data
    """
    
    print(best_practices)
    
    return "Best practices overview completed"

demo_performance_gotchas()
demo_best_practices()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 9: Monitoring and Debugging
# MAGIC ### Key metrics and debugging techniques

# COMMAND ----------

def demo_monitoring():
    print("=== Demo: Streaming Monitoring ===")
    
    df_stream = (spark
        .readStream
        .format("json")
        .option("maxBytesPerTrigger", "1mb")
        .load(f"{volume_path}/source_data")
    )
    
    df_processed = (df_stream
        .withWatermark("pickup_datetime", "1 hour")
        .groupBy(
            F.window("pickup_datetime", "10 minutes"),
            "pickup_zip"
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare")
        )
    )
    
    query = (df_processed
        .writeStream
        .outputMode("append")
        .format("console")
        .option("numRows", 5)
        .trigger(processingTime="15 seconds")
        .start()
    )
    
    # Monitor the query
    print("Monitoring streaming query...")
    for i in range(6):  # Monitor for 90 seconds
        time.sleep(15)
        
        # Get query progress
        progress = query.lastProgress
        if progress:
            print(f"\n--- Monitoring Iteration {i+1} ---")
            print(f"Batch ID: {progress.get('batchId', 'N/A')}")
            print(f"Input rows/sec: {progress.get('inputRowsPerSecond', 'N/A')}")
            print(f"Processing time: {progress.get('durationMs', {}).get('triggerExecution', 'N/A')} ms")
            print(f"Trigger execution: {progress.get('durationMs', {}).get('triggerExecution', 'N/A')} ms")
            print(f"State store rows: {progress.get('stateOperators', [{}])[0].get('numRowsTotal', 'N/A') if progress.get('stateOperators') else 'N/A'}")
            
            # Check for issues
            if progress.get('inputRowsPerSecond', 0) == 0:
                print("⚠️  Warning: No input data detected")
            
            processing_time = progress.get('durationMs', {}).get('triggerExecution', 0)
            if processing_time > 10000:  # 10 seconds
                print("⚠️  Warning: Processing time is high")
    
    query.stop()
    
    # Demonstrate query status checking
    print("\n=== Query Status Information ===")
    print(f"Query ID: {query.id}")
    print(f"Query Name: {query.name}")
    print(f"Is Active: {query.isActive}")
    print(f"Exception: {query.exception}")
    
    return "Monitoring demo completed"

demo_monitoring()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 10: Advanced Patterns
# MAGIC ### Stream-Stream Joins and Complex Event Processing

# COMMAND ----------

def demo_stream_stream_joins():
    print("=== Demo: Stream-Stream Joins ===")
    
    # Create two related streams
    # Stream 1: Taxi trips
    trips_stream = (spark
        .readStream
        .format("json")
        .option("maxBytesPerTrigger", "1mb")
        .load(f"{volume_path}/source_data")
        .select("trip_id", "driver_id", "pickup_datetime", "fare_amount")
        .withWatermark("pickup_datetime", "1 hour")
    )
    
    # Stream 2: Driver ratings (simulated)
    driver_ratings_data = (spark.range(1000, 5000)
        .withColumn("driver_id", F.col("id"))
        .withColumn("rating", F.rand() * 5)
        .withColumn("rating_datetime", 
                   F.current_timestamp() - F.expr("INTERVAL " + 
                   (F.rand() * 24 * 60).cast("int").cast("string") + " MINUTES"))
        .select("driver_id", "rating", "rating_datetime")
    )
    
    # Write driver ratings to a location for streaming
    driver_ratings_data.write.mode("overwrite").json(f"{volume_path}/source_data/driver_ratings")
    
    ratings_stream = (spark
        .readStream
        .format("json")
        .option("maxBytesPerTrigger", "500kb")
        .load(f"{volume_path}/source_data/driver_ratings")
        .withWatermark("rating_datetime", "1 hour")
    )
    
    # Stream-stream join
    joined_stream = (trips_stream.alias("trips")
        .join(
            ratings_stream.alias("ratings"),
            F.expr("""
                trips.driver_id = ratings.driver_id AND
                trips.pickup_datetime >= ratings.rating_datetime AND
                trips.pickup_datetime <= ratings.rating_datetime + interval 1 hour
            """),
            "inner"
        )
        .select(
            F.col("trips.trip_id"),
            F.col("trips.driver_id"),
            F.col("trips.pickup_datetime"),
            F.col("trips.fare_amount"),
            F.col("ratings.rating"),
            F.col("ratings.rating_datetime")
        )
    )
    
    query = (joined_stream
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="30 seconds")
        .start()
    )
    
    time.sleep(90)
    query.stop()
    
    return "Stream-stream joins demo completed"

demo_stream_stream_joins()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup and Summary

# COMMAND ----------

# Clean up any remaining active streams
print("Cleaning up active streams...")
for stream in spark.streams.active:
    try:
        stream.stop()
        print(f"Stopped stream: {stream.name}")
    except:
        pass

print("\n" + "="*60)
print("DATABRICKS STRUCTURED STREAMING DEMO COMPLETED")
print("="*60)

summary = """
📊 DEMO SECTIONS COVERED:

1. ✅ Basic Streaming Setup & Configuration
2. ✅ maxBytesPerTrigger & maxFilesPerTrigger
3. ✅ Output Modes (Append, Complete, Update)
4. ✅ Delta Lake Integration & Merge Operations
5. ✅ Stateful vs Stateless Transformations
6. ✅ Watermarking for Late Data
7. ✅ ForEachBatch Custom Processing
8. ✅ Common Issues (Skew, Spill, Shuffle)
9. ✅ Performance Gotchas & Best Practices
10. ✅ Monitoring & Debugging Techniques
11. ✅ Advanced Patterns (Stream-Stream Joins)

🔧 KEY TAKEAWAYS:
- Use appropriate trigger intervals (≥1 second)
- Control batch sizes with maxBytesPerTrigger
- Always use watermarking with time-based operations
- Monitor streaming metrics regularly
- Use Delta Lake for reliable streaming outputs
- Handle skew with salting techniques
- Implement proper error handling

📈 PERFORMANCE TIPS:
- Start with 100MB-1GB per trigger
- Use continuous processing sparingly
- Optimize stateful operations
- Monitor state store memory usage
- Use appropriate partitioning strategies

🚨 COMMON GOTCHAS ADDRESSED:
- Too frequent triggers causing overhead
- Missing watermarking in stateful operations
- Incorrect output mode selection
- Uncontrolled state growth
- Data skew in streaming aggregations
"""

print(summary)