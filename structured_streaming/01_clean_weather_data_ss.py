# Databricks notebook source
"""
Here is what I plan to cover:
- 
"""

# COMMAND ----------

spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, lit
source_table = source_table = 'leigh_robertson_demo.bronze_noaa.forecasts_streaming_demo'


stream_df = (spark.readStream
    #.option("readChangeFeed", "true")
    .option("maxBytesPerTrigger", "100k")
    .table(source_table)
)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, lit, regexp_extract, expr, when, from_utc_timestamp, to_timestamp, regexp_replace, row_number, desc
from pyspark.sql.window import Window

def process_microbatch(micro_batch_df, batch_id):
    # Add batch_id to track processing order
    micro_batch_df = micro_batch_df.withColumn("batch_id", lit(batch_id))
    
    transformed_df = (
        micro_batch_df
        .withColumn("timezoneOffset", regexp_extract(col("startTime"), r"([+-]\d{2}:\d{2})$", 1))
        .withColumn("startTime", regexp_replace(col("startTime"), r"[+-]\d{2}:\d{2}$", ""))
        .withColumn("endTime", regexp_replace(col("endTime"), r"[+-]\d{2}:\d{2}$", ""))
        .withColumn("startTimeUTC", 
                    when(col("timezoneOffset") != "", 
                         expr("from_utc_timestamp(startTime, timezoneOffset)"))
                    .otherwise(col("startTime")))
        .withColumn("endTimeUTC", 
                    when(col("timezoneOffset") != "", 
                         expr("from_utc_timestamp(endTime, timezoneOffset)"))
                    .otherwise(col("endTime")))
        .withColumn("windSpeed", regexp_extract(col("windSpeed"), "(\\d+)", 1).cast("int"))
        .withColumn("audit_update_ts", current_timestamp())
        .withColumn("dewpoint", col("dewpoint.value"))
        .withColumn("probabilityOfPrecipitation", col("probabilityOfPrecipitation.value"))
        .withColumn("relativeHumidity", col("relativeHumidity.value"))
        .drop("batch_id")  # Remove batch_id before writing
    )

    # Append transformed data to target table
    transformed_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(target_table)


# COMMAND ----------

# Start the streaming query with microbatch processing
import time
checkpoint_location = "s3://one-env/leigh_robertson/streaming_metadata/forecasts_streaming_demo_expanded/"
target_table = "leigh_robertson_demo.silver_noaa.forecasts_streaming_demo_expanded"

query = stream_df.writeStream \
    .foreachBatch(process_microbatch) \
    .outputMode("append") \
    .queryName("forecasts_streaming_demo_expanded") \
    .trigger(processingTime="30 seconds") \
    .option("checkpointLocation", checkpoint_location) \
    .start()

# Wait for the query to terminate
#query.awaitTermination()
#.trigger("availableNow" = True) \
    
# Let the query run for 10 minutes (600 seconds)
# time.sleep(600)

# # Stop the streaming query
# query.stop()

# COMMAND ----------

# dbutils.fs.rm(checkpoint_location, recurse=True)
# spark.sql("TRUNCATE TABLE leigh_robertson_demo.silver_noaa.forecasts_streaming_demo_expanded;")


# COMMAND ----------

# %sql 
# CREATE TABLE leigh_robertson_demo.silver_noaa.forecasts_ss (
#   post_code STRING,
#   number BIGINT,
#   name STRING,
#   startTime STRING,
#   endTime STRING,
#   isDaytime BOOLEAN,
#   temperature BIGINT,
#   temperatureUnit STRING,
#   temperatureTrend STRING,
#   probabilityOfPrecipitation BIGINT,
#   dewpoint DOUBLE,
#   relativeHumidity BIGINT,
#   windSpeed INT,
#   windDirection STRING,
#   icon STRING,
#   shortForecast STRING,
#   detailedForecast STRING,
#   audit_update_ts TIMESTAMP,
#   timezoneOffset STRING,
#   startTimeUTC STRING,
#   endTimeUTC STRING)
# COMMENT 'SCD Type 1 managed silver forecasts'
# TBLPROPERTIES (
#   'quality' = 'silver')


# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE leigh_robertson_demo.bronze_noaa.forecasts_streaming_demo

# COMMAND ----------


