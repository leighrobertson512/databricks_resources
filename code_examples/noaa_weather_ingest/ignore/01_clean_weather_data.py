# Databricks notebook source
df = spark.read.table('leigh_robertson_demo.bronze_noaa.forecasts').limit(1)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_extract, expr, when, regexp_replace, lit, current_timestamp

#df = spark.read.table('leigh_robertson_demo.bronze_noaa.forecasts')

def transform_noaa_bronze_df(df):
    # Extract timezone offset and convert startTime and endTime to UTC
    df = df.withColumn("timezoneOffset", regexp_extract(col("startTime"), r"([+-]\d{2}:\d{2})$", 1))
    df = df.withColumn("startTime", regexp_replace(col("startTime"), r"[+-]\d{2}:\d{2}$", ""))
    df = df.withColumn("endTime", regexp_replace(col("endTime"), r"[+-]\d{2}:\d{2}$", ""))
    df = df.withColumn("startTimeUTC", 
                       when(col("timezoneOffset") != "", 
                            expr("from_utc_timestamp(startTime, timezoneOffset)"))
                       .otherwise(col("startTime")))
    df = df.withColumn("endTimeUTC", 
                       when(col("timezoneOffset") != "", 
                            expr("from_utc_timestamp(endTime, timezoneOffset)"))
                       .otherwise(col("endTime")))
    
    # Extract integer value from windSpeed
    df = df.withColumn("windSpeed", regexp_extract(col("windSpeed"), "(\\d+)", 1).cast("int"))
    df = df.withColumn("audit_update_ts", lit(current_timestamp()))
    
    # Extract value from dewpoint, probabilityOfPrecipitation, and relativeHumidity
    df = df.withColumn("dewpoint", col("dewpoint.value")) \
           .withColumn("probabilityOfPrecipitation", col("probabilityOfPrecipitation.value")) \
           .withColumn("relativeHumidity", col("relativeHumidity.value"))
       
    
    return df

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE df_transformed

# COMMAND ----------



# COMMAND ----------

"""
Prompt used
I need help writing a function which will do the following transformations on the df object above
- create a new field which converts startTime and endTime to UTC. the offset for each timezone is contained at the end of the timestamp field. It will be after the last part of the timestamp. 
-  For the wind speed field, it will replace the existing column which has the MPH extracted after so the value is an integer and not a string
- For the dewPoint, probabilityOfPrecipitation, relativeHumidity fields, it needs to extract the value and replace the column with just the extracted number part.

