# Databricks notebook source
# MAGIC %md
# MAGIC # Weather Data Generator for Upsert Demo
# MAGIC ## Modified to generate data only on the hour for unique post_code + start_time combinations
# MAGIC
# MAGIC This script generates weather data with timestamps only on the hour
# MAGIC to ensure post_code + start_time make records unique for upsert scenarios

# COMMAND ----------

import random
import time
from datetime import datetime, timedelta
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, TimestampType, DoubleType
import json

# Configuration - matches your existing setup
source_table = 'leigh_robertson_demo.bronze_noaa.forecasts_streaming_demo_upsert'

# COMMAND ----------

def generate_hourly_weather_data_batch(postal_codes, batch_size=100, advanced_patterns=False, hours_ahead=24):
    """Generate simulated weather forecast data with timestamps only on the hour for unique keys"""
    from pyspark.sql.functions import rand, randn, when
    from datetime import datetime, timedelta
    
    # Define explicit schema matching your table structure
    weather_schema = StructType([
        StructField("post_code", StringType(), True),
        StructField("number", LongType(), True),
        StructField("name", StringType(), True),
        StructField("startTime", StringType(), True),
        StructField("endTime", StringType(), True),
        StructField("isDaytime", BooleanType(), True),
        StructField("temperature", LongType(), True),
        StructField("temperatureUnit", StringType(), True),
        StructField("temperatureTrend", StringType(), True),
        StructField("probabilityOfPrecipitation", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", LongType(), True)
        ]), True),
        StructField("dewpoint", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", DoubleType(), True)
        ]), True),
        StructField("relativeHumidity", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", LongType(), True)
        ]), True),
        StructField("windSpeed", StringType(), True),
        StructField("windDirection", StringType(), True),
        StructField("icon", StringType(), True),
        StructField("shortForecast", StringType(), True),
        StructField("detailedForecast", StringType(), True),
        StructField("audit_update_ts", TimestampType(), True)
    ])
    
    # Generate hourly timestamps for next N hours
    current_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
    hourly_timestamps = [current_hour + timedelta(hours=i) for i in range(hours_ahead)]
    
    # Create combinations of postal codes and hourly timestamps
    data = []
    record_num = 0
    
    for hour_time in hourly_timestamps[:batch_size]:  # Limit by batch size
        for postal_code in postal_codes[:max(1, batch_size // len(hourly_timestamps))]:
            record_num += 1
            
            # Simulate realistic weather patterns
            temperature = random.randint(-10, 100)
            humidity = random.randint(10, 100)
            precipitation_prob = min(100, max(0, random.randint(0, 100)))
            dewpoint_value = float(temperature - random.randint(10, 30))
            
            # Advanced weather patterns if requested
            if advanced_patterns:
                # Seasonal adjustments
                month = hour_time.month
                if month in [12, 1, 2]:  # Winter
                    temperature = random.randint(-20, 40)
                    precipitation_prob = random.randint(10, 70)
                elif month in [6, 7, 8]:  # Summer
                    temperature = random.randint(60, 100)
                    precipitation_prob = random.randint(5, 40)
                
                # Weather condition correlation
                if precipitation_prob > 70:
                    short_forecast = random.choice(['Rain', 'Thunderstorms', 'Snow'])
                    humidity = max(humidity, 60)
                elif precipitation_prob < 20:
                    short_forecast = 'Sunny'
                    humidity = min(humidity, 50)
                else:
                    short_forecast = random.choice(['Partly Cloudy', 'Cloudy'])
            else:
                short_forecast = random.choice(['Sunny', 'Partly Cloudy', 'Cloudy', 'Rain', 'Snow'])
            
            data.append({
                'post_code': str(postal_code),
                'number': record_num,
                'name': f'Hourly Weather Forecast {record_num}',
                'startTime': hour_time.strftime('%Y-%m-%dT%H:%M:%S-05:00'),  # Only on the hour
                'endTime': (hour_time + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S-05:00'),
                'isDaytime': hour_time.hour >= 6 and hour_time.hour <= 18,
                'temperature': temperature,
                'temperatureUnit': 'F',
                'temperatureTrend': random.choice([None, 'rising', 'falling']),
                'probabilityOfPrecipitation': {
                    'unitCode': 'wmoUnit:percent',
                    'value': precipitation_prob
                },
                'dewpoint': {
                    'unitCode': 'wmoUnit:degF', 
                    'value': dewpoint_value
                },
                'relativeHumidity': {
                    'unitCode': 'wmoUnit:percent',
                    'value': humidity
                },
                'windSpeed': f"{random.randint(5, 25)} mph",
                'windDirection': random.choice(['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']),
                'icon': 'https://api.weather.gov/icons/land/day/clear',
                'shortForecast': short_forecast,
                'detailedForecast': f'Temperature around {temperature}°F with {precipitation_prob}% chance of precipitation.',
                'audit_update_ts': datetime.now()
            })
            
            if len(data) >= batch_size:
                break
        if len(data) >= batch_size:
            break
    
    # Create DataFrame with explicit schema
    df = spark.createDataFrame(data, schema=weather_schema)
    
    return df

# COMMAND ----------

def upsert_weather_data(df, target_table):
    """Upsert weather data based on post_code and startTime keys"""
    
    # Create or replace temporary view for the source data
    df.createOrReplaceTempView("weather_updates")
    
    # Perform upsert using MERGE INTO statement
    merge_sql = f"""
    MERGE INTO {target_table} AS target
    USING weather_updates AS source
    ON target.post_code = source.post_code 
       AND target.startTime = source.startTime
    WHEN MATCHED THEN
      UPDATE SET
        target.number = source.number,
        target.name = source.name,
        target.endTime = source.endTime,
        target.isDaytime = source.isDaytime,
        target.temperature = source.temperature,
        target.temperatureUnit = source.temperatureUnit,
        target.temperatureTrend = source.temperatureTrend,
        target.probabilityOfPrecipitation = source.probabilityOfPrecipitation,
        target.dewpoint = source.dewpoint,
        target.relativeHumidity = source.relativeHumidity,
        target.windSpeed = source.windSpeed,
        target.windDirection = source.windDirection,
        target.icon = source.icon,
        target.shortForecast = source.shortForecast,
        target.detailedForecast = source.detailedForecast,
        target.audit_update_ts = source.audit_update_ts
    WHEN NOT MATCHED THEN
      INSERT (
        post_code, number, name, startTime, endTime, isDaytime,
        temperature, temperatureUnit, temperatureTrend, 
        probabilityOfPrecipitation, dewpoint, relativeHumidity,
        windSpeed, windDirection, icon, shortForecast, 
        detailedForecast, audit_update_ts
      )
      VALUES (
        source.post_code, source.number, source.name, source.startTime, source.endTime, source.isDaytime,
        source.temperature, source.temperatureUnit, source.temperatureTrend,
        source.probabilityOfPrecipitation, source.dewpoint, source.relativeHumidity,
        source.windSpeed, source.windDirection, source.icon, source.shortForecast,
        source.detailedForecast, source.audit_update_ts
      )
    """
    
    spark.sql(merge_sql)
    print(f"✅ Upserted {df.count()} records to {target_table}")

# COMMAND ----------

# Get postal codes from your existing data
postal_codes_df = spark.sql("""
    SELECT DISTINCT post_code 
    FROM leigh_robertson_demo.bronze_noaa.zip_code 
    WHERE state_abbreviation = 'NY'
    LIMIT 50
""")

postal_codes = [row.post_code for row in postal_codes_df.collect()]
print(f"Using {len(postal_codes)} postal codes for hourly generation")
print(f"Sample postal codes: {postal_codes[:5]}...")

# COMMAND ----------

def start_hourly_upsert_generation(duration_minutes=5, batch_interval_seconds=30, hours_ahead=24):
    """
    Start hourly weather data generation with upserts
    
    Args:
        duration_minutes: How long to run the generator
        batch_interval_seconds: Seconds between batches
        hours_ahead: How many hours ahead to generate forecasts
    """
    print(f"Starting hourly weather data generation with upserts for {duration_minutes} minutes")
    print(f"Batch interval: {batch_interval_seconds} seconds")
    print(f"Generating forecasts for next {hours_ahead} hours")
    
    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)
    batch_num = 0
    
    while time.time() < end_time:
        batch_num += 1
        print(f"\n--- Generating Hourly Batch {batch_num} ---")
        
        try:
            # Generate hourly weather data
            batch_size = min(len(postal_codes) * hours_ahead, 500)  # Reasonable batch size
            new_data = generate_hourly_weather_data_batch(
                postal_codes, 
                batch_size=batch_size, 
                advanced_patterns=True,
                hours_ahead=hours_ahead
            )
            
            # Upsert to bronze table
            upsert_weather_data(new_data, source_table)
            
            print(f"✅ Batch {batch_num}: {new_data.count()} records upserted to {source_table}")
            
            # Wait before next batch
            time.sleep(batch_interval_seconds)
            
        except Exception as e:
            print(f"❌ Error in batch {batch_num}: {e}")
            time.sleep(5)  # Short delay on error
    
    print(f"\n🏁 Hourly upsert generation completed. Generated {batch_num} batches.")

# COMMAND ----------

def generate_specific_hour_updates(target_hour_offset=1, temperature_change=10):
    """Generate updates for a specific hour to demonstrate upsert functionality"""
    
    print(f"Generating updates for hour +{target_hour_offset} with temperature change of {temperature_change}°F")
    
    # Get a specific hour timestamp
    target_hour = datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=target_hour_offset)
    
    # Create updates for a subset of postal codes
    update_data = []
    for i, postal_code in enumerate(postal_codes[:10]):  # Update first 10 postal codes
        temperature = random.randint(50, 90) + temperature_change  # Temperature with change
        
        update_data.append({
            'post_code': str(postal_code),
            'number': i + 1000,  # Different number to show update
            'name': f'UPDATED Weather Forecast {i}',
            'startTime': target_hour.strftime('%Y-%m-%dT%H:%M:%S-05:00'),
            'endTime': (target_hour + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S-05:00'),
            'isDaytime': target_hour.hour >= 6 and target_hour.hour <= 18,
            'temperature': temperature,
            'temperatureUnit': 'F',
            'temperatureTrend': 'rising',
            'probabilityOfPrecipitation': {
                'unitCode': 'wmoUnit:percent',
                'value': random.randint(0, 30)
            },
            'dewpoint': {
                'unitCode': 'wmoUnit:degF', 
                'value': float(temperature - 15)
            },
            'relativeHumidity': {
                'unitCode': 'wmoUnit:percent',
                'value': random.randint(40, 70)
            },
            'windSpeed': f"{random.randint(10, 20)} mph",
            'windDirection': random.choice(['N', 'E', 'S', 'W']),
            'icon': 'https://api.weather.gov/icons/land/day/clear',
            'shortForecast': 'Updated Sunny',
            'detailedForecast': f'UPDATED: Temperature around {temperature}°F (changed by {temperature_change}°F)',
            'audit_update_ts': datetime.now()
        })
    
    # Create schema and DataFrame
    weather_schema = StructType([
        StructField("post_code", StringType(), True),
        StructField("number", LongType(), True),
        StructField("name", StringType(), True),
        StructField("startTime", StringType(), True),
        StructField("endTime", StringType(), True),
        StructField("isDaytime", BooleanType(), True),
        StructField("temperature", LongType(), True),
        StructField("temperatureUnit", StringType(), True),
        StructField("temperatureTrend", StringType(), True),
        StructField("probabilityOfPrecipitation", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", LongType(), True)
        ]), True),
        StructField("dewpoint", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", DoubleType(), True)
        ]), True),
        StructField("relativeHumidity", StructType([
            StructField("unitCode", StringType(), True),
            StructField("value", LongType(), True)
        ]), True),
        StructField("windSpeed", StringType(), True),
        StructField("windDirection", StringType(), True),
        StructField("icon", StringType(), True),
        StructField("shortForecast", StringType(), True),
        StructField("detailedForecast", StringType(), True),
        StructField("audit_update_ts", TimestampType(), True)
    ])
    
    update_df = spark.createDataFrame(update_data, schema=weather_schema)
    
    # Perform upsert
    upsert_weather_data(update_df, source_table)
    
    print(f"✅ Generated updates for {len(update_data)} records at hour {target_hour}")

# COMMAND ----------

print("""
🌦️  HOURLY WEATHER DATA GENERATOR WITH UPSERTS READY!

✅ NEW FEATURES:
- Generates data only on the hour (post_code + startTime = unique keys)
- Upsert functionality using MERGE INTO statements
- Specific hour update generation for testing upserts
- Realistic hourly forecast generation

Usage Instructions:
1. Run hourly generation: start_hourly_upsert_generation()
2. Test upserts: generate_specific_hour_updates()
3. Monitor upserts in your pipeline

Key Functions:
- generate_hourly_weather_data_batch(): Creates hourly data
- upsert_weather_data(): Performs MERGE INTO upserts
- start_hourly_upsert_generation(): Continuous generation
- generate_specific_hour_updates(): Test specific updates

Example:
start_hourly_upsert_generation(duration_minutes=5, batch_interval_seconds=30, hours_ahead=24)
""")

# COMMAND ----------

# Example usage - uncomment to run
start_hourly_upsert_generation(duration_minutes=5, batch_interval_seconds=30, hours_ahead=12)

# COMMAND ----------


