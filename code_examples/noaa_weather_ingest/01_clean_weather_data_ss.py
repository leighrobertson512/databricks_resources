# Databricks notebook source
spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, lit
source_table = 'leigh_robertson_demo.bronze_noaa.forecasts'


stream_df = (spark.readStream
    .option("readChangeFeed", "true")
    .option("maxBytesPerTrigger", "10g")
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
    )

    # Add window function to get latest record
    window_spec = Window.partitionBy("post_code", "startTime").orderBy(
        desc("audit_update_ts"), 
        desc("batch_id")  # Tiebreaker for same timestamp
    )
    
    deduped_df = (
        transformed_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num", "batch_id")
    )

    # Merge with target table
    delta_table = DeltaTable.forName(spark, target_table)
    
    delta_table.alias("target").merge(
        deduped_df.alias("source"),
        "target.post_code = source.post_code AND target.startTime = source.startTime"
    ).whenMatchedUpdateAll(
        condition="source.audit_update_ts > target.audit_update_ts"
    ).whenNotMatchedInsertAll(
    ).execute()


# COMMAND ----------

# Start the streaming query
checkpoint_location = "s3://one-env/leigh_robertson/streaming_metadata/silver_noaa_forecasts_ss/"
target_table = "leigh_robertson_demo.silver_noaa.forecasts_ss"
query = stream_df.writeStream \
    .foreachBatch(process_microbatch) \
    .outputMode("update") \
    .trigger(availableNow=True) \
    .option("checkpointLocation", checkpoint_location) \
    .start()

# Wait for the query to terminate
#query.awaitTermination()

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
# MAGIC SELECT *
# MAGIC FROM leigh_robertson_demo.silver_noaa.forecasts_ss

# COMMAND ----------



# COMMAND ----------


