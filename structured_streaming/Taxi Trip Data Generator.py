# Databricks notebook source
dbutils.widgets.text("wNumberOfFiles", "300", "Number of new files to generate")

# COMMAND ----------

# MAGIC %pip install dbldatagen==0.4.0

# COMMAND ----------

catalog_name = "dlt_demo_lr"
schema_name = "taxi"
volume_name = "taxi_raw"

sql_query = f"""
CREATE VOLUME {catalog_name}.{schema_name}.{volume_name}
"""

#spark.sql(sql_query)

volume_base_path = "/Volumes/dlt_demo_lr/taxi/taxi_raw"
dbutils.fs.mkdirs(f"{volume_base_path}/tmp/taxi_data")
dbutils.fs.mkdirs(f"{volume_base_path}/tmp/taxi_data_chkpnt")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS dlt_demo_ra;
# MAGIC CREATE SCHEMA IF NOT EXISTS dlt_demo_ra.weather;

# COMMAND ----------

weather_catalog_name = "dlt_demo_ra"
weather_schema_name = "weather"
weather_volume_name = "weather_raw"



sql_query = f"""
CREATE VOLUME {weather_catalog_name}.{weather_schema_name}.{weather_volume_name}
"""

spark.sql(sql_query)

weather_volume_base_path = f"/Volumes/{weather_catalog_name}/{weather_schema_name}/{weather_volume_name}"
dbutils.fs.mkdirs(f"{weather_volume_base_path}/tmp/weather_data")
dbutils.fs.mkdirs(f"{weather_volume_base_path}/tmp/weather_data_chkpnt")

# COMMAND ----------

nyc_zip_codes = [
10096, 10272, 10114, 10133, 10110, 10260, 10169, 10177,
10203, 10212, 10012, 10039, 10124, 10179, 10104
]

# COMMAND ----------

def generate_weather_forecast_data():
    """Generates random weather forecast data for NYC ZIPs"""
    import dbldatagen as dg
    from pyspark.sql.types import TimestampType, StringType, FloatType, IntegerType

    ds = (
        dg.DataGenerator(spark, name="nyc_weather_forecasts", rows=1000000, partitions=8)
        .withIdOutput()
        .withColumn("post_code", IntegerType(), values=nyc_zip_codes, random=True)
        .withColumn("startTime", TimestampType(), 
                   begin="2024-01-01 00:00:00", 
                   end="2025-05-31 23:59:59",
                   interval="1 hour",
                   uniqueValues=12384)  # 365 days * 24 hours
        .withColumn("temperature", FloatType(), 
                   minValue=-10.0, maxValue=100.0, 
                   random=True, precision=1)
        .withColumn("temperatureUnit", StringType(), 
                   values=['F'], random=True)
        .withColumn("probabilityOfPrecipitation", FloatType(),
                   minValue=0.0, maxValue=1.0, 
                   step=0.01, random=True)
    )
    
    return ds.build()

# Example writing logic (similar to taxi example)
import random
from pyspark.sql.functions import col, to_date
max_num_files = dbutils.widgets.get("wNumberOfFiles")
for i in range(int(max_num_files)):
  df = generate_weather_forecast_data().drop("id").withColumn("forecast_date", to_date(col("startTime")))
  file_name = f"{weather_volume_base_path}/tmp/weather_data/forecasts_{random.randint(1, 1000000)}.json"
  df.write.mode("append").json(file_name)
  print(f"Wrote weather data to: '{file_name}'")

# COMMAND ----------

def generate_taxi_trip_data():
  """Generates random taxi trip data"""
  import dbldatagen as dg
  from pyspark.sql.types import IntegerType, StringType, FloatType, DateType

  ds = (
      dg.DataGenerator(spark, name="random_taxi_trip_dataset", rows=100000, partitions=8)
      .withColumn("trip_id", IntegerType(), minValue=1000000, maxValue=2000000)
      .withColumn("taxi_number", IntegerType(), uniqueValues=10000, random=True)
      .withColumn("driver_id", IntegerType(), minValue=1000000, maxValue=2000000)
      .withColumn("passenger_count", IntegerType(), minValue=0, maxValue=4)
      .withColumn("trip_amount", FloatType(), minValue=-100.0, maxValue=1000.0, random=True)
      .withColumn("trip_distance", FloatType(), minValue=0.1, maxValue=1000.0)
      .withColumn("trip_date", DateType(), uniqueValues=300, random=True)
      .withColumn("driver_name", percentNulls=0.01, template=r'A\\w A\\w|Aw\\ A. A\\w')
      .withColumn("email", template=r'\\w.\\w@\\w.com')  
      .withColumn("pickup_zip_code", IntegerType(), values=nyc_zip_codes, random=True)
      .withColumn("tariff_type", percentNulls=0.01, values=['Regular','Morning rush', 'Afternoon rush', 'Evening rush', 'Night', 'Other'],
                random=True))

  return ds.build()

import random

max_num_files = dbutils.widgets.get("wNumberOfFiles")
for i in range(int(max_num_files)):
  df = generate_taxi_trip_data()
  file_name = f"{volume_base_path}/tmp/taxi_data/taxi_data_{random.randint(1, 1000000)}.json"
  df.write.mode("append").json(file_name)
  print(f"Wrote trip data to: '{file_name}'")

# COMMAND ----------

df = (
  spark.read.json(f"{volume_base_path}/tmp/taxi_data/taxi_data_*.json")
)
df.display()

# COMMAND ----------

weather_df = (
  spark.read.json(f"{weather_volume_base_path}/tmp/weather_data/forecasts_*.json")
)
weather_df.display()

# COMMAND ----------

# Optional - Cleanup random generated data
#dbutils.fs.rm("/Volumes/quickstarts/taxi/taxi_raw/tmp/", True)
#dbutils.fs.rm("/tmp/chp_03/taxi_data_chkpnt", recurse=True)
#spark.sql("DROP SCHEMA IF EXISTS hive_metastore.chp_03 CASCADE")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dlt_demo_lr.taxi.tariffs (
# MAGIC     tariff_ID BIGINT,
# MAGIC     tariff_type STRING,
# MAGIC     surcharge DECIMAL(10, 2)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO dlt_demo_lr.taxi.tariffs (tariff_ID, tariff_type, surcharge) VALUES
# MAGIC (1, 'Regular', 1.00),
# MAGIC (2, 'Morning rush', 1.50),
# MAGIC (3, 'Afternoon rush', 1.50),
# MAGIC (4, 'Evening rush', 2.00),
# MAGIC (5, 'Night', 2.50),
# MAGIC (6, 'Other', 1.00);

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT forecasts.post_code,
# MAGIC forecasts.startTime,
# MAGIC forecasts.endTime,
# MAGIC forecasts.temperature,
# MAGIC forecasts.temperatureUnit,
# MAGIC forecasts.probabilityOfPrecipitation,
# MAGIC cast(forecasts.startTime AS date) AS forecast_date,
# MAGIC zip_code.place_name
# MAGIC FROM leigh_robertson_demo.silver_noaa.forecasts_ss AS forecasts
# MAGIC INNER JOIN leigh_robertson_demo.bronze_noaa.zip_code AS zip_code 
# MAGIC   ON forecasts.post_code = zip_code.post_code 
# MAGIC WHERE zip_code.state_abbreviation = 'NY'
# MAGIC AND place_name = 'New York City'
# MAGIC

# COMMAND ----------

def generate_weather_forecast_data():
    """Generates random weather forecast data for NYC ZIPs"""
    import dbldatagen as dg
    from pyspark.sql.types import TimestampType, StringType, FloatType, IntegerType

    nyc_zip_codes = [
        10096, 10272, 10114, 10133, 10110, 10260, 10169, 10177,
        10203, 10212, 10012, 10039, 10124, 10179, 10104
    ]

    ds = (
        dg.DataGenerator(spark, name="nyc_weather_forecasts", rows=1000000, partitions=8)
        .withIdOutput()
        .withColumn("post_code", IntegerType(), values=nyc_zip_codes, random=True)
        .withColumn("startTime", TimestampType(), 
                   begin="2023-01-01 00:00:00", 
                   end="2023-12-31 23:59:59",
                   interval="1 hour",
                   uniqueValues=8760)  # 365 days * 24 hours
        .withColumn("temperature", FloatType(), 
                   minValue=-10.0, maxValue=100.0, 
                   random=True, precision=1)
        .withColumn("temperatureUnit", StringType(), 
                   values=['F'], random=True)
        .withColumn("probabilityOfPrecipitation", FloatType(),
                   minValue=0.0, maxValue=1.0, 
                   step=0.01, random=True)
    )
    
    return ds.build()

# Example writing logic (similar to taxi example)
import random
from pyspark.sql.functions import col, to_date
# max_num_files = dbutils.widgets.get("wNumberOfFiles")
# for i in range(int(max_num_files)):
#   df = generate_weather_forecast_data().drop("id").withColumn("forecast_date", to_date(col("startTime")))
#   file_name = f"{volume_base_path}/tmp/weather_data/forecasts_{random.randint(1, 1000000)}.json"
#   df.write.mode("append").json(file_name)
#   print(f"Wrote weather data to: '{file_name}'")


# COMMAND ----------

df = generate_weather_forecast_data().drop("id").withColumn("forecast_date", to_date(col("startTime")))
df.display()

# COMMAND ----------

df.display()

# COMMAND ----------

