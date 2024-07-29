# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp, expr, lit
from pyspark.sql.types import IntegerType

# COMMAND ----------

source_path = "/Volumes/leigh_robertson_demo/dlt_demo_bronze/baby_names_data/"
target_table = "leigh_robertson_demo.dlt_demo.baby_names_raw_autoloader"  
checkpoint_location = "/Volumes/leigh_robertson_demo/dlt_demo_bronze/checkpoints/baby_names_data/"
schema_location = "/Volumes/leigh_robertson_demo/dlt_demo_bronze/schemas/baby_names_data/"

df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    #something I recommend having below in the event an errant file change is needed
    .option("pathGlobfilter", "*.csv") \
    .option("cloudFiles.schemaLocation", schema_location) \
    #I prefer this option or rescue new data as the assumption is that new columns should automatically added
    .option("schemaEvolutionMode", "addNewColumns") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load(source_path)

#rename columns to conform to naming conventions 
new_column_names = [c.lower().replace(" ", "_") for c in df.columns]

# Applying the new column names to the DataFrame
df = df.select([col(c).alias(new_col) for c, new_col in zip(df.columns, new_column_names)])

#df = df.withColumn(.selectExpr("*", "_metadata as source_metadata"))
df = df.withColumn("source_metadata", expr("_metadata")).selectExpr("*").withColumn("count", col("count").cast(IntegerType()))
df = df.select("year",
               "first_name",
               "county",
               "sex",
               "count",
               col("_metadata.file_name")).withColumn("audit_update_ts", lit(current_timestamp()))

# COMMAND ----------

df.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_location) \
    .option("maxFilesPerTrigger", 20000) \
    .trigger(availableNow=True) \
    .outputMode("append") \
    .option("mergeSchema", "true") \
    .table(target_table) \
    .awaitTermination()

# COMMAND ----------

df = spark.sql("SELECT * FROM leigh_robertson_demo.dlt_demo.baby_names_raw_autoloader LIMIT 1;")

# COMMAND ----------

import random

def generate_random_name():
    # List of sample names for demonstration
    names = ["Olivia", "Liam", "Emma", "Noah", "Ava", "Elijah", "Charlotte", "William", "Sophia", "James", "Alex", "Mia", "Owen", "Jim", "Raquel", "Leigh"]
    # Select a random name from the list
    name = random.choice(names)
    return name

# Assign a random name to the variable 'name'
name = generate_random_name()

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

df_modified = df.withColumn("first_name", lit(name)) \
                .withColumn("sex", lit("male")) \
                .withColumn("file_name", lit("manual_add")) \
                .withColumn("audit_update_ts", lit(current_timestamp()))

display(df_modified)

# COMMAND ----------

df_modified.write \
  .mode("append") \
  .saveAsTable(target_table)

# COMMAND ----------


