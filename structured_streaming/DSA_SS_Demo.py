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
source_table = 'leigh_robertson_demo.bronze_noaa.forecasts_streaming_demo'
silver_table = "leigh_robertson_demo.silver_noaa.forecasts_ss_streaming_demo_expanded"
demo_checkpoint_base = "s3://one-env/leigh_robertson/streaming_metadata/demo/"

print("Weather Streaming Demo Configuration:")
print(f"Source Table: {source_table}")
print(f"Silver Table: {silver_table}")
print(f"Checkpoint Base: {demo_checkpoint_base}")

# Clean up any existing streams
for stream in spark.streams.active:
    print(f"Stopping existing stream: {stream.name}")
    #stream.stop()+

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generation for Continuous Streaming
# MAGIC Creating a continuous data generator to simulate real-time weather data updates
# MAGIC 
# MAGIC **Note: The main data generation functions have been moved to Weather_Data_Generator.py**
# MAGIC **Run that notebook to generate continuous streaming data for this demo**

# COMMAND ----------

# Get postal codes from your existing data
postal_codes_df = spark.sql("""
    SELECT DISTINCT post_code 
    FROM leigh_robertson_demo.bronze_noaa.zip_code 
    WHERE state_abbreviation = 'NY'
    LIMIT 1000
""")
postal_codes = [row.post_code for row in postal_codes_df.collect()]
print(f"Using {len(postal_codes)} postal codes for demo: {postal_codes[:5]}...")

# Note: For continuous data generation, run the Weather_Data_Generator.py notebook
print("💡 For continuous data generation, run the Weather_Data_Generator.py notebook")
print("💡 All data generation functions are now centralized there with the working schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Basic Streaming with Key Settings
# MAGIC ### maxBytesPerTrigger and Processing Time - Based on Your Weather Pipeline

# COMMAND ----------

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
    .option("checkpointLocation", f"{demo_checkpoint_base}/weather_alerts")
    .table("leigh_robertson_demo.silver_noaa.weather_alerts")
    .trigger(processingTime="20 seconds")
    .start()
)

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
        .option("checkpointLocation", f"{demo_checkpoint_base}/weather_alerts")
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
