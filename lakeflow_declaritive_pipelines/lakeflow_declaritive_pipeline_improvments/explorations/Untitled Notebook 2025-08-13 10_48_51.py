# Databricks notebook source
# MAGIC %pip install databricks-sdk

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import psycopg2

from databricks.sdk import WorkspaceClient
import uuid

w = WorkspaceClient()

instance_name = "fe_shared_demo"

instance = w.database.get_database_instance(name=instance_name)
cred = w.database.generate_database_credential(request_id=str(uuid.uuid4()), instance_names=[instance_name])

# Connection parameters
conn = psycopg2.connect(
    host = instance.read_write_dns,
    dbname = "databricks_postgres",
    user = "leigh.robertson@databricks.com",
    password = cred.token,
    sslmode = "require"
)

# Execute query
with conn.cursor() as cur:
    cur.execute("SELECT version()")
    version = cur.fetchone()[0]
    print(version)
conn.close()


# COMMAND ----------

import psycopg2
from databricks.sdk import WorkspaceClient
import uuid

w = WorkspaceClient()

instance_name = "fe_shared_demo"

# Assuming the correct method to get the database instance and credentials
instance = w.database.get_database_instance(name=instance_name)
cred = w.databases.generate_credential(
    request_id=str(uuid.uuid4()), 
    instance_names=[instance_name]
)

# Connection parameters
conn = psycopg2.connect(
    host=instance.read_write_dns,
    dbname="databricks_postgres",
    user="leigh.robertson@databricks.com",
    password=cred.token,
    sslmode="require"
)

# Execute query
with conn.cursor() as cur:
    cur.execute("SELECT version()")
    version = cur.fetchone()[0]
    print(version)
conn.close()

# COMMAND ----------

import psycopg2

password = "eyJraWQiOiJkZmJjOWVmMThjZTQ2ZTlhMDg2NWZmYzlkODkxYzJmMjg2NmFjMDM3MWZiNDlmOTdhMDg1MzBjNWYyODU3ZTg4IiwidHlwIjoiYXQrand0IiwiYWxnIjoiUlMyNTYifQ.eyJjbGllbnRfaWQiOiJkYXRhYnJpY2tzLXNlc3Npb24iLCJzY29wZSI6ImlhbS5jdXJyZW50LXVzZXI6cmVhZCBpYW0uZ3JvdXBzOnJlYWQgaWFtLnNlcnZpY2UtcHJpbmNpcGFsczpyZWFkIGlhbS51c2VyczpyZWFkIiwiaXNzIjoiaHR0cHM6Ly9lMi1kZW1vLWZpZWxkLWVuZy5jbG91ZC5kYXRhYnJpY2tzLmNvbS9vaWRjIiwiYXVkIjoiMTQ0NDgyODMwNTgxMDQ4NSIsInN1YiI6ImxlaWdoLnJvYmVydHNvbkBkYXRhYnJpY2tzLmNvbSIsImlhdCI6MTc1NTIwOTgwMCwiZXhwIjoxNzU1MjEzNDAwLCJqdGkiOiJmYjJhYWE1My0yYzg4LTQ0MzQtYjEyYS1mZmYyZGFlY2NlNDgifQ.J9bA-Puww_LxIwOYtKFzVkRVD3bNhOFzsyNIEMqTaOr6_F23H41cVapBB0qjhRF2pK9TpI0as6V2lsmJwjluthSIwWPMYLikDe0-4Yj0lQPSmjsaxbOx5hGGNC_V7SJr9f9A4C8rWE_SNamAGMqrigNzN7kQT1C-xcNumpePKv5d6P8HlWFsWH0IZfb7HAARThwEthZ71vG84ra02TEyfxjrD2R9QA6MoqMnftlF-Jt-0VGdzq1xB1gBiXjWpn7oIunzC-YXSVA16HJZ00u_utoHG6taOkqBkMd-3XzZEqroAOovqPrj_EZTg4KNhfNpOUllYzfb7LXRa0Yn_Z7Uqw"
host = "instance-0935a9d9-c151-4597-b2bc-77d7de43b2c5.database.cloud.databricks.com"
user = "leigh.robertson@databricks.com"
port = 5432


psql_conn_str = f"host={host} user={user} dbname=databricks_postgres port={port} sslmode=require password={password}"

conn = psycopg2.connect(psql_conn_str)

with conn.cursor() as cur:
    cur.execute("SELECT version()")
    version = cur.fetchone()[0]
    print(version)
conn.close()

# COMMAND ----------

import psycopg2

schema_ddl = """
CREATE SCHEMA IF NOT EXISTS leigh_robertson_demo;
"""

table_ddl = """
CREATE TABLE IF NOT EXISTS leigh_robertson_demo.weather_data_postgres (
    post_code VARCHAR,
    number BIGINT,
    name VARCHAR,
    startTime VARCHAR,
    endTime VARCHAR,
    isDaytime BOOLEAN,
    temperature BIGINT,
    temperatureUnit VARCHAR,
    temperatureTrend VARCHAR,
    probabilityOfPrecipitation BIGINT,
    dewpoint DOUBLE PRECISION,
    relativeHumidity BIGINT,
    windSpeed INTEGER,
    windDirection VARCHAR,
    icon VARCHAR,
    shortForecast VARCHAR,
    detailedForecast VARCHAR,
    audit_update_ts TIMESTAMP,
    timezoneOffset VARCHAR,
    startTimeUTC VARCHAR,
    endTimeUTC VARCHAR
);
CREATE INDEX IF NOT EXISTS idx_weather_post_code_startTime
    ON leigh_robertson_demo.weather_data_postgres (post_code, startTime);
"""

# Example data to insert (replace with your actual data)
data = [
    (
        "12345", 1, "Location A", "2025-08-14T08:00:00", "2025-08-14T14:00:00", True, 75, "F", "Rising", 10,
        55.5, 60, 12, "NW", "icon_url", "Sunny", "Clear skies", "2025-08-14 07:00:00", "-0400", "2025-08-14T12:00:00Z", "2025-08-14T18:00:00Z"
    )
]

insert_sql = """
INSERT INTO leigh_robertson_demo.weather_data_postgres (
    post_code, number, name, startTime, endTime, isDaytime, temperature, temperatureUnit, temperatureTrend,
    probabilityOfPrecipitation, dewpoint, relativeHumidity, windSpeed, windDirection, icon, shortForecast,
    detailedForecast, audit_update_ts, timezoneOffset, startTimeUTC, endTimeUTC
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
"""

conn = psycopg2.connect(psql_conn_str)
with conn:
    with conn.cursor() as cur:
        cur.execute(schema_ddl)
        cur.execute(table_ddl)
        cur.executemany(insert_sql, data)
conn.close()

# COMMAND ----------

import psycopg2

query_sql = "SELECT * FROM leigh_robertson_demo.weather_data_postgres"

conn = psycopg2.connect(psql_conn_str)
with conn:
    with conn.cursor() as cur:
        cur.execute(query_sql)
        rows = cur.fetchall()
        for row in rows:
            print(row)
conn.close()

# COMMAND ----------

df = spark.table("leigh_robertson_demo.silver_noaa.forecasts_expanded_ldp")

# import psycopg2

# insert_sql = """
# INSERT INTO leigh_robertson_demo.weather_data_postgres (
#     post_code, number, name, startTime, endTime, isDaytime, temperature, temperatureUnit, temperatureTrend,
#     probabilityOfPrecipitation, dewpoint, relativeHumidity, windSpeed, windDirection, icon, shortForecast,
#     detailedForecast, audit_update_ts, timezoneOffset, startTimeUTC, endTimeUTC
# ) VALUES (
#     %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
# )
# """

# data = df.select(
#     "post_code", "number", "name", "startTime", "endTime", "isDaytime", "temperature", "temperatureUnit",
#     "temperatureTrend", "probabilityOfPrecipitation", "dewpoint", "relativeHumidity", "windSpeed",
#     "windDirection", "icon", "shortForecast", "detailedForecast", "audit_update_ts", "timezoneOffset",
#     "startTimeUTC", "endTimeUTC"
# ).collect()
# data_tuples = [tuple(row) for row in data]

# conn = psycopg2.connect(psql_conn_str)
# with conn:
#     with conn.cursor() as cur:
#         cur.executemany(insert_sql, data_tuples)
# conn.close()

# COMMAND ----------

jdbc_host = f"jdbc:postgresql://leigh.robertson%40databricks.com:${password}@instance-0935a9d9-c151-4597-b2bc-77d7de43b2c5.database.cloud.databricks.com:5432/databricks_postgres?sslmode=require"
df.write \
  .format("jdbc") \
  .option("url", jdbc_host) \
  .option("dbtable", "databricks_postgres.leigh_robertson_demo.weather_data_postgres") \
  .option("user", user) \
  .option("password", password) \
  .option("batchsize", 10000) \
  .option("numPartitions", 4) \
  .mode("append") \
  .save()


# COMMAND ----------

jdbc_host = "jdbc:postgresql://instance-0935a9d9-c151-4597-b2bc-77d7de43b2c5.database.cloud.databricks.com:5432/databricks_postgres?sslmode=require"
df.write \
  .format("jdbc") \
  .option("url", jdbc_host) \
  .option("dbtable", "databricks_postgres.leigh_robertson_demo.weather_data_postgres") \
  .option("user", user) \
  .option("password", password) \
  .option("batchsize", 10000) \
  .option("numPartitions", 4) \
  .mode("append") \
  .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   forecasts.startTime,
# MAGIC   zip_code.place_name,
# MAGIC   zip_code.state,
# MAGIC   zip_code.post_code,
# MAGIC   forecasts.temperature,
# MAGIC   forecasts.probabilityOfPrecipitation,
# MAGIC   forecasts.windSpeed,
# MAGIC   CAST(forecasts.startTimeUTC AS DATE) AS forecast_date_utc,
# MAGIC   CAST(forecasts.startTime AS DATE) AS forecast_date_local_tz,
# MAGIC   current_timestamp() AS audit_update_ts 
# MAGIC FROM leigh_robertson_demo.silver_noaa.forecasts_expanded_ldp AS forecasts
# MAGIC INNER JOIN leigh_robertson_demo.bronze_noaa.zip_code AS zip_code
# MAGIC   ON forecasts.post_code = zip_code.post_code

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT ST_asText(ST_point(latitude, longitude))
# MAGIC --,ST_point(latitude, longitude)
# MAGIC ,* 
# MAGIC FROM leigh_robertson_demo.bronze_noaa.zip_code

# COMMAND ----------


