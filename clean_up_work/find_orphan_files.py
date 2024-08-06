# Databricks notebook source
"""
What this will do 
- Use describe detail to find all tables in a catalog/schema
- For that same schema, it will search in a given s3 directory and join on the ID's/path 
"""

# COMMAND ----------

location_of_s3_files = 's3://SAMPLE_FILE_PATH'
dbutils.fs.ls(location_of_s3_files)

# COMMAND ----------

# MAGIC %md 
# MAGIC Code to generate sample delta tables which will get dropped later

# COMMAND ----------

# DBTITLE 1,Create sample external table
from pyspark.sql.functions import rand, randn

# Define the schema with at least 10 columns
schema = """
    id INT, 
    col1 DOUBLE, 
    col2 DOUBLE, 
    col3 DOUBLE, 
    col4 DOUBLE, 
    col5 DOUBLE, 
    col6 DOUBLE, 
    col7 DOUBLE, 
    col8 DOUBLE, 
    col9 DOUBLE, 
    col10 DOUBLE
"""
table_name = "leigh_robertson_demo.raw.sample_table"
s3_location = location_of_s3_files
# Create a DataFrame with 10,000 rows using the defined schema
df = spark.range(0, 10000).withColumn("col1", rand()).withColumn("col2", randn())\
    .withColumn("col3", rand()).withColumn("col4", randn())\
    .withColumn("col5", rand()).withColumn("col6", randn())\
    .withColumn("col7", rand()).withColumn("col8", randn())\
    .withColumn("col9", rand()).withColumn("col10", randn())

# # Write the DataFrame to a Delta table in the specified S3 location
# (df.write
#  .format("delta")
#  .mode("overwrite")
#  .saveAsTable(table_name, path = s3_location))

# COMMAND ----------

# DBTITLE 1,Create sample external table which will later get dropped to show how the issues occur
from pyspark.sql.functions import rand, randn

# Define the schema with at least 10 columns
schema = """
    id INT, 
    col1 DOUBLE, 
    col2 DOUBLE, 
    col3 DOUBLE, 
    col4 DOUBLE, 
    col5 DOUBLE, 
    col6 DOUBLE, 
    col7 DOUBLE, 
    col8 DOUBLE, 
    col9 DOUBLE, 
    col10 DOUBLE
"""
table_name = "leigh_robertson_demo.raw.sample_table_w_issues"
s3_location = location_of_s3_files_w_issues
# Create a DataFrame with 10,000 rows using the defined schema
df = spark.range(0, 10000).withColumn("col1", rand()).withColumn("col2", randn())\
    .withColumn("col3", rand()).withColumn("col4", randn())\
    .withColumn("col5", rand()).withColumn("col6", randn())\
    .withColumn("col7", rand()).withColumn("col8", randn())\
    .withColumn("col9", rand()).withColumn("col10", randn())

# # Write the DataFrame to a Delta table in the specified S3 location
# (df.write
#  .format("delta")
#  .mode("overwrite")
#  .saveAsTable(table_name, path = s3_location))

# COMMAND ----------

# %sql 
# CREATE SCHEMA IF NOT EXISTS leigh_robertson_demo.raw;

# COMMAND ----------

# %sql 
# DROP TABLE leigh_robertson_demo.raw.sample_table_w_issues;

# COMMAND ----------

# %sql 
# OPTIMIZE leigh_robertson_demo.raw.sample_table_w_issues ZORDER BY id

# COMMAND ----------

# MAGIC %md
# MAGIC Functions for getting required information

# COMMAND ----------

from pyspark.sql.functions import lit

def describe_catalog_schema(catalog, schema):
    # Get the list of tables in the specified catalog and schema
    tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
    tables = tables_df.collect()
    
    # Initialize an empty DataFrame to hold the results
    result_df = None
    
    # Loop through each table and describe its details
    for table in tables:
        table_name = table['tableName']
        description_df = spark.sql(f"DESCRIBE DETAIL {catalog}.{schema}.{table_name}").withColumn("tableName", lit(table_name))
        
        # Append the result to the result_df DataFrame
        if result_df is None:
            result_df = description_df
        else:
            result_df = result_df.union(description_df)
    
    # Display the final DataFrame containing details of all tables
    return result_df

# COMMAND ----------

from pyspark.sql import Row

def list_files_to_df(s3_path):
    files = dbutils.fs.ls(s3_path)
    rows = [Row(name=file.name, path=file.path, size=file.size) for file in files]
    df = spark.createDataFrame(rows)
    df = df.withColumnRenamed("name", "short_table_name")
    return df

# COMMAND ----------

from pyspark.sql.functions import expr

def left_join_dfs(s3_location_df, databricks_tables_df):
    # Remove the last '/' from the 'path' column in df
    s3_location_df = s3_location_df.withColumn("path_modified", expr("substring(path, 1, length(path)-1)"))
    
    # Perform the left join using the modified 'path' column from df and 'location' column from table_df
    joined_df = s3_location_df.join(databricks_tables_df, s3_location_df.path_modified == databricks_tables_df.location, "left")
    
    # Drop the modified path column after join
    joined_df = joined_df.select("name", "location", "path_modified")
    
    return joined_df

# Assuming table_df is already defined and available
# joined_df = left_join_dfs(df, table_df)
# display(joined_df)

# COMMAND ----------

#get all tables in catalog
catalog = "leigh_robertson_demo"
schema = "raw"
s3_path_where_tables_are = location_of_s3_files
tables_df = describe_catalog_schema(catalog, schema)

# get all s3 files
df = list_files_to_df(s3_path_where_tables_are)

joined_df = left_join_dfs(df, tables_df)

# COMMAND ----------

joined_df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %environment
# MAGIC "client": "1"
# MAGIC "base_environment": ""
