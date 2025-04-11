# Databricks notebook source
"""
https://www.zippopotam.us/
"""

# COMMAND ----------

# MAGIC %run ./sql_ingestion_framework/utils_file

# COMMAND ----------

import requests
import pandas as pd
import time
def get_zip_code_data(zip_code):
    try: 
        # Construct the API request URL
        url = f'http://api.zippopotam.us/us/{zip_code}'

        # Make the GET request
        response = requests.get(url)

        # Parse the response
        if response.status_code == 200:
            data = response.json()
        else:
            print(f"Failed to retrieve {zip_code} data.")
            
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        print(f'{zip_code} is not valid')

# COMMAND ----------

import requests
import pandas as pd
import time

def get_zip_code_data(zip_code):
    try:
        # Construct the API request URL
        url = f'http://api.zippopotam.us/us/{zip_code}'

        # Make the GET request
        response = requests.get(url)

        # Check if the request was successful
        if response.status_code != 200:
            raise Exception(f"Failed to retrieve {zip_code} data.")

        # Parse the response
        data = response.json()

        # Check if the data contains places (i.e., the zip code is valid)
        if not data['places']:
            raise Exception(f"No data found for {zip_code}.")  # Raise an exception if no places are found

        # Convert data to DataFrame
        df = pd.DataFrame(data)
        return df

    except Exception as e:
        print(f"{zip_code} is not valid: {e}")
        return None  # Return None to indicate failure



# COMMAND ----------

def replace_spaces_in_column_names(df):
    for col_name in df.columns:
        new_col_name = col_name.replace(' ', '_')
        df = df.withColumnRenamed(col_name, new_col_name)
    return df

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp
def load_and_clean_zip_code_data(df):
  
  df_spark = spark.createDataFrame(df)
  df_spark = (df_spark.withColumn("latitude", col("places.latitude"))
              .withColumn("longitude", col("places.longitude"))
              .withColumn('place_name', col("places.place name"))
              .withColumn("state", col("places.state"))
              .withColumn("state_abbreviation", col("places.state abbreviation"))
              .withColumn("audit_update_ts", lit(current_timestamp()))
              .drop("places")        
              )
  df_spark = replace_spaces_in_column_names(df_spark).withColumn("post_code", col("post_code").cast("STRING")) #cast as string to ensure leading zeros are respected
  table_name = "leigh_robertson_demo.bronze_noaa.zip_code"
  match_columns, update_columns, insert_columns = generate_match_insert_columns("post_code", df_spark)
  merge_sql = dynamic_merge_sql(table_name, "updates", match_columns, update_columns, insert_columns)
  df_spark.createOrReplaceTempView("updates")
  spark.sql(merge_sql)
  zip_code = df_spark.select("post_code").first()[0]
  print(f'Loaded {zip_code}')


# COMMAND ----------

def load_zip_codes(start, end):
    for zip_code in range(start, end + 1):
        zip_code = (f'{zip_code:05}')  # Format zip code to be 5 digits
        print(f'loading zip code: {zip_code}')
        df = get_zip_code_data(zip_code)
        if df is not None:
            load_and_clean_zip_code_data(df)
            #sleep to limit API calls
        time.sleep(5)

# Define the start and end zip codes
start_zip = 82556
end_zip = 99950

# Call the function
load_zip_codes(start_zip, end_zip)


# COMMAND ----------

# MAGIC %sql 
# MAGIC -- SELECT MAX(CAST(post_code AS INT))
# MAGIC -- FROM leigh_robertson_demo.bronze_noaa.zip_code
# MAGIC OPTIMIZE leigh_robertson_demo.bronze_noaa.zip_code;
# MAGIC VACUUM leigh_robertson_demo.bronze_noaa.zip_code;

# COMMAND ----------


