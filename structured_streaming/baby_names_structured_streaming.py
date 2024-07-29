# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

source_table = "leigh_robertson_demo.dlt_demo.baby_names_raw_autoloader"
target_table = "leigh_robertson_demo.dlt_demo.baby_names_sql_prepared_structured_streaming"  
checkpoint_location = "/Volumes/leigh_robertson_demo/dlt_demo_bronze/checkpoints/baby_names_sql_prepared/"

# COMMAND ----------


# Read the streaming data from the source table
streamingDF = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "delta") \
    .option("maxBytesPerTrigger", "250MB") \
    .table(source_table)

# Transformation logic
transformedDF = streamingDF \
    .withColumnRenamed("year", "year_of_birth") \
    .select("year_of_birth", "first_name", "count") \
    .filter(col("first_name").isNotNull()) \
    .filter(col("count") > 0) \
    .withColumn("audit_update_ts", lit(current_timestamp()))

# COMMAND ----------

# Write the stream to the target table with checkpointing
query = transformedDF.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_location) \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .toTable(target_table) \
    .awaitTermination()

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * 
# MAGIC FROM leigh_robertson_demo.dlt_demo.baby_names_sql_prepared_structured_streaming
# MAGIC ORDER BY audit_update_ts DESC;
# MAGIC

# COMMAND ----------

#dbutils.fs.rm(checkpoint_location, recurse = True)

# COMMAND ----------


