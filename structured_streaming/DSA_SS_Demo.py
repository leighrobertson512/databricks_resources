# Databricks notebook source
# MAGIC %md
# MAGIC # Weather Data Structured Streaming Demo
# MAGIC ## Solutions Architect Demo: Common Patterns, Issues, and Best Practices
# MAGIC 
# MAGIC **Based on NOAA Weather Data Pipeline**
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

import random
import time
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQuery
from delta.tables import DeltaTable

# Configuration based on your existing setup
source_table = 'leigh_robertson_demo.bronze_noaa.forecasts'
silver_table = "leigh_robertson_demo.silver_noaa.forecasts_ss"
demo_checkpoint_base = "s3://one-env/leigh_robertson/streaming_metadata/demo/"

print("Weather Streaming Demo Configuration:")
print(f"Source Table: {source_table}")
print(f"Silver Table: {silver_table}")
print(f"Checkpoint Base: {demo_checkpoint_base}")

# Clean up any existing streams
for stream in spark.streams.active:
    print(f"Stopping existing stream: {stream.name}")
    stream.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generation for Continuous Streaming
# MAGIC Creating a continuous data generator to simulate real-time weather data updates

# COMMAND ----------

def generate_weather_data_batch(postal_codes, batch_size=100):
    """Generate simulated weather forecast data for continuous streaming"""
    from pyspark.sql.functions import rand, randn, when
    from datetime import datetime, timedelta
    
    # Create base data with random weather patterns
    data = []
    for i in range(batch_size):
        postal_code = random.choice(postal_codes)
        base_time = datetime.now() + timedelta(hours=random.randint(0, 168))  # Next 7 days
        
        # Simulate realistic weather patterns
        temperature = random.randint(-10, 100)
        humidity = random.randint(10, 100)
        precipitation_prob = min(100, max(0, random.randint(0, 100)))
        
        data.append({
            'post_code': str(postal_code),
            'number': i,
            'name': f'Weather Forecast {i}',
            'startTime': base_time.strftime('%Y-%m-%dT%H:%M:%S-05:00'),
            'endTime': (base_time + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S-05:00'),
            'isDaytime': base_time.hour >= 6 and base_time.hour <= 18,
            'temperature': temperature,
            'temperatureUnit': 'F',
            'temperatureTrend': None,
            'probabilityOfPrecipitation': {'value': precipitation_prob},
            'dewpoint': {'value': float(temperature - random.randint(10, 30))},
            'relativeHumidity': {'value': humidity},
            'windSpeed': f"{random.randint(5, 25)} mph",
            'windDirection': random.choice(['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']),
            'icon': 'https://api.weather.gov/icons/land/day/clear',
            'shortForecast': random.choice(['Sunny', 'Partly Cloudy', 'Cloudy', 'Rain', 'Snow']),
            'detailedForecast': f'Temperature around {temperature}°F with {precipitation_prob}% chance of precipitation.'
        })
    
    df = spark.createDataFrame(data)
    return df.withColumn('audit_update_ts', F.current_timestamp())

# Get postal codes from your existing data
postal_codes_df = spark.sql("""
    SELECT DISTINCT post_code 
    FROM leigh_robertson_demo.bronze_noaa.zip_code 
    WHERE state_abbreviation = 'NY'
    LIMIT 20
""")
postal_codes = [row.post_code for row in postal_codes_df.collect()]
print(f"Using {len(postal_codes)} postal codes for demo: {postal_codes[:5]}...")

def start_continuous_data_generation():
    """Function to continuously generate and write weather data to source table"""
    print("Starting continuous data generation...")
    
    for batch_num in range(10):  # Generate 10 batches
        print(f"Generating batch {batch_num + 1}")
        
        # Generate new weather data
        new_data = generate_weather_data_batch(postal_codes, batch_size=50)
        
        # Write to bronze table (simulating real API ingestion)
        new_data.write.mode("append").saveAsTable(source_table)
        
        print(f"Written batch {batch_num + 1} to {source_table}")
        time.sleep(5)  # Wait 5 seconds between batches
    
    return "Data generation completed"

# Generate some initial data
print("Generating initial weather data batch...")
initial_data = generate_weather_data_batch(postal_codes, batch_size=200)
initial_data.write.mode("append").saveAsTable(source_table)
print("Initial data generated successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Basic Streaming with Key Settings
# MAGIC ### maxBytesPerTrigger and Processing Time - Based on Your Weather Pipeline

# COMMAND ----------

def demo_max_bytes_per_trigger():
    """Demonstrate maxBytesPerTrigger using your weather data pipeline"""
    print("=== Demo: maxBytesPerTrigger with Weather Data ===")
    
    # Read from your bronze weather table with controlled batch size
    stream_df = (spark.readStream
        .option("readChangeFeed", "true")
        .option("maxBytesPerTrigger", "1mb")  # Small batches for demo
        .table(source_table)
    )
    
    # Simple transformation - extract key weather metrics
    processed_df = (stream_df
        .select("post_code", "temperature", "probabilityOfPrecipitation", 
                "startTime", "audit_update_ts")
        .withColumn("processing_time", F.current_timestamp())
        .withColumn("temp_category", 
                   F.when(F.col("temperature") < 32, "freezing")
                   .when(F.col("temperature") < 60, "cold")
                   .when(F.col("temperature") < 80, "mild")
                   .otherwise("hot"))
    )
    
    # Write to console to observe trigger behavior
    query = (processed_df
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("numRows", 10)
        .trigger(processingTime="15 seconds")
        .start()
    )
    
    time.sleep(45)
    query.stop()
    print("maxBytesPerTrigger demo completed")

demo_max_bytes_per_trigger()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Output Modes with Weather Data
# MAGIC ### Append vs Complete vs Update modes

# COMMAND ----------

def demo_append_mode():
    """Demonstrate Append mode - most common for streaming"""
    print("=== Demo: Append Mode with Weather Alerts ===")
    
    # Create alerts table
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS leigh_robertson_demo.silver_noaa.weather_alerts (
        alert_id STRING,
        post_code STRING,
        alert_type STRING,
        temperature INT,
        precipitation_prob INT,
        alert_message STRING,
        created_at TIMESTAMP
    ) USING DELTA
    """)
    
    stream_df = (spark.readStream
        .option("readChangeFeed", "true")
        .option("maxBytesPerTrigger", "2mb")
        .table(source_table)
    )
    
    # Generate weather alerts using append mode
    alerts_df = (stream_df
        .filter((F.col("temperature") < 20) | (F.col("temperature") > 95) |
                (F.col("probabilityOfPrecipitation.value") > 80))
        .select("post_code", "temperature", "probabilityOfPrecipitation.value", "startTime")
        .withColumn("alert_id", F.concat(F.lit("ALERT_"), F.col("post_code"), F.lit("_"), 
                                        F.unix_timestamp().cast("string")))
        .withColumn("alert_type", 
                   F.when(F.col("temperature") < 20, "EXTREME_COLD")
                   .when(F.col("temperature") > 95, "EXTREME_HEAT")
                   .otherwise("HIGH_PRECIPITATION"))
        .withColumn("precipitation_prob", F.col("probabilityOfPrecipitation.value"))
        .withColumn("alert_message", 
                   F.concat(F.lit("Weather alert for "), F.col("post_code")))
        .withColumn("created_at", F.current_timestamp())
        .drop("probabilityOfPrecipitation.value", "startTime")
    )
    
    query = (alerts_df
        .writeStream
        .outputMode("append")
        .format("delta")
        .option("checkpointLocation", f"{demo_checkpoint_base}weather_alerts")
        .table("leigh_robertson_demo.silver_noaa.weather_alerts")
        .trigger(processingTime="20 seconds")
        .start()
    )
    
    time.sleep(60)
    query.stop()
    print("Append mode demo completed")

demo_append_mode()

# COMMAND ----------

# MAGIC %md  
# MAGIC ## Section 3: Merge Operations with ForEachBatch
# MAGIC ### Your existing pattern enhanced with custom logic

# COMMAND ----------

def demo_streaming_merge_enhanced():
    """Enhanced version of your streaming merge with additional processing"""
    print("=== Demo: Enhanced Streaming Merge ===")
    
    stream_df = (spark.readStream
        .option("readChangeFeed", "true")
        .option("maxBytesPerTrigger", "5mb")
        .table(source_table)
    )
    
    def process_weather_batch(micro_batch_df, batch_id):
        """Enhanced processing function based on your original"""
        print(f"Processing batch {batch_id}")
        
        # Your original transformation logic enhanced
        from pyspark.sql.functions import regexp_extract, regexp_replace, when, expr, row_number, desc
        from pyspark.sql.window import Window
        
        micro_batch_df = micro_batch_df.withColumn("batch_id", F.lit(batch_id))
        
        transformed_df = (
            micro_batch_df
            .withColumn("timezoneOffset", regexp_extract(F.col("startTime"), r"([+-]\d{2}:\d{2})$", 1))
            .withColumn("startTime", regexp_replace(F.col("startTime"), r"[+-]\d{2}:\d{2}$", ""))
            .withColumn("endTime", regexp_replace(F.col("endTime"), r"[+-]\d{2}:\d{2}$", ""))
            .withColumn("windSpeed", regexp_extract(F.col("windSpeed"), "(\\d+)", 1).cast("int"))
            .withColumn("dewpoint", F.col("dewpoint.value"))
            .withColumn("probabilityOfPrecipitation", F.col("probabilityOfPrecipitation.value"))
            .withColumn("relativeHumidity", F.col("relativeHumidity.value"))
            .withColumn("audit_update_ts", F.current_timestamp())
        )
        
        # Deduplication with window function
        window_spec = Window.partitionBy("post_code", "startTime").orderBy(
            desc("audit_update_ts"), desc("batch_id")
        )
        
        deduped_df = (
            transformed_df
            .withColumn("row_num", row_number().over(window_spec))
            .filter(F.col("row_num") == 1)
            .drop("row_num", "batch_id")
        )
        
        # Enhanced merge with your table
        delta_table = DeltaTable.forName(spark, silver_table)
        
        delta_table.alias("target").merge(
            deduped_df.alias("source"),
            "target.post_code = source.post_code AND target.startTime = source.startTime"
        ).whenMatchedUpdateAll(
            condition="source.audit_update_ts > target.audit_update_ts"
        ).whenNotMatchedInsertAll(
        ).execute()
        
        print(f"Processed {deduped_df.count()} records in batch {batch_id}")
    
    query = (stream_df
        .writeStream
        .foreachBatch(process_weather_batch)
        .outputMode("update")
        .option("checkpointLocation", f"{demo_checkpoint_base}enhanced_merge")
        .trigger(processingTime="30 seconds")
        .start()
    )
    
    time.sleep(120)
    query.stop()
    print("Enhanced merge demo completed")

demo_streaming_merge_enhanced()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Stateful vs Stateless Operations
# MAGIC ### Weather aggregations and windowing

# COMMAND ----------

def demo_stateful_aggregations():
    """Demonstrate stateful operations with weather data"""
    print("=== Demo: Stateful Weather Aggregations ===")
    
    stream_df = (spark.readStream
        .option("readChangeFeed", "true")
        .option("maxBytesPerTrigger", "3mb")
        .table(source_table)
    )
    
    # Stateful aggregation - rolling weather averages
    weather_stats = (stream_df
        .withWatermark("startTime", "2 hours")  # Handle late data
        .groupBy(
            F.window(F.to_timestamp(F.col("startTime")), "1 hour", "30 minutes"),
            "post_code"
        )
        .agg(
            F.avg("temperature").alias("avg_temp"),
            F.max("temperature").alias("max_temp"),
            F.min("temperature").alias("min_temp"),
            F.avg("probabilityOfPrecipitation.value").alias("avg_precipitation"),
            F.count("*").alias("forecast_count")
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "post_code", "avg_temp", "max_temp", "min_temp", 
            "avg_precipitation", "forecast_count"
        )
    )
    
    query = (weather_stats
        .writeStream
        .outputMode("append")  # Can use append with watermark
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="45 seconds")
        .start()
    )
    
    time.sleep(120)
    query.stop()
    print("Stateful aggregations demo completed")

demo_stateful_aggregations()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Performance Gotchas and Best Practices
# MAGIC ### Common issues with weather streaming pipelines

# COMMAND ----------

def demo_performance_gotchas():
    """Demonstrate common performance issues and solutions"""
    print("=== Demo: Performance Gotchas ===")
    
    # Gotcha 1: Too frequent triggers
    print("Gotcha 1: Avoid triggers < 1 second")
    print("Your current setup uses 'availableNow=True' which is good for batch processing")
    print("For continuous streaming, use processingTime >= 1 second")
    
    # Gotcha 2: Data skew demonstration
    print("Gotcha 2: Data skew by postal code")
    
    # Check for skew in your data
    skew_check = spark.sql(f"""
        SELECT post_code, COUNT(*) as record_count
        FROM {source_table}
        GROUP BY post_code
        ORDER BY record_count DESC
        LIMIT 10
    """)
    
    print("Top postal codes by record count (potential skew):")
    skew_check.display()
    
    # Gotcha 3: Missing watermarking
    print("Gotcha 3: Always use watermarking for time-based operations")
    print("Your pipeline should include watermarking when doing time-window aggregations")
    
    return "Performance gotchas demo completed"

demo_performance_gotchas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Best Practices

# COMMAND ----------

print("🌦️  WEATHER DATA STREAMING DEMO COMPLETED")
print("="*60)

summary = """
📊 DEMO SECTIONS COVERED (Weather Data Pipeline):

1. ✅ maxBytesPerTrigger with Change Data Feed
2. ✅ Append Output Mode with Weather Alerts
3. ✅ Enhanced Merge Operations with ForEachBatch
4. ✅ Stateful Aggregations with Watermarking
5. ✅ Performance Gotchas and Skew Detection

🔧 KEY TAKEAWAYS FOR WEATHER PIPELINE:
- Use Change Data Feed for incremental processing
- Control batch sizes with maxBytesPerTrigger (1-10MB)
- Always use watermarking for time-based aggregations  
- Implement proper deduplication in foreachBatch
- Monitor for data skew by postal code
- Use appropriate trigger intervals (≥30 seconds for weather data)

📈 WEATHER-SPECIFIC OPTIMIZATIONS:
- Partition by post_code for better performance
- Use time-based windows for weather trends
- Implement proper timezone handling
- Handle missing/null weather values gracefully

🚨 COMMON WEATHER STREAMING GOTCHAS:
- Timezone inconsistencies in startTime/endTime
- Late-arriving weather updates
- Data quality issues with nested JSON structures
- Memory pressure from large probabilityOfPrecipitation objects
"""

print(summary)

# Clean up active streams
for stream in spark.streams.active:
    try:
        stream.stop()
        print(f"Stopped stream: {stream.name}")
    except:
        pass

print("\n🌦️  Weather Streaming Demo Setup Complete!")
print("Run the sections above to demonstrate different streaming concepts with your weather data.")

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