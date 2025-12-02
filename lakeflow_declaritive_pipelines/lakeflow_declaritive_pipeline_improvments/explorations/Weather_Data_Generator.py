# Databricks notebook source
# MAGIC %md
# MAGIC # Weather Data Generator for Streaming Demo
# MAGIC ## Continuous data generation to simulate real-time weather updates
# MAGIC
# MAGIC This script generates realistic weather data and writes it to your bronze table
# MAGIC to provide continuous streaming data for the DSA_SS_Demo.py

# COMMAND ----------

"""
What I want to capture
- writing to multiple catalogs and schemas from one pipeline
- use variable for table name
- load to MV 
- write to standard delta with forEachBatch and delta sink
- modify weather data generator so that it only offers on the start of the hour so post code and start_time make a record unique but also provide the ability to upsert on these keys

"""

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- SELECT post_code,
# MAGIC -- startTime,
# MAGIC
# MAGIC -- FROM leigh_robertson_demo.bronze_noaa.forecasts_streaming_demo
# MAGIC
# MAGIC -- CREATE TABLE leigh_robertson_demo.gold_noaa.forecasts_high_low
# MAGIC -- AS SELECT *,
# MAGIC -- current_timestamp() as audit_update_ts
# MAGIC -- FROM leigh_robertson_demo.gold_noaa.ten_day_forecast
# MAGIC -- LIMIT 1;
# MAGIC
# MAGIC --leigh_robertson_demo.gold_noaa.ten_day_forecast
# MAGIC -- SELECT 
# MAGIC --   forecasts.startTime,
# MAGIC --   zip_code.place_name,
# MAGIC --   zip_code.state,
# MAGIC --   zip_code.post_code,
# MAGIC --   forecasts.temperature,
# MAGIC --   forecasts.probabilityOfPrecipitation,
# MAGIC --   forecasts.windSpeed,
# MAGIC --   CAST(forecasts.startTimeUTC AS DATE) AS forecast_date_utc,
# MAGIC --   CAST(forecasts.startTime AS DATE) AS forecast_date_local_tz
# MAGIC -- FROM leigh_robertson_demo.silver_noaa.forecasts_expanded_dlt AS forecasts
# MAGIC -- INNER JOIN leigh_robertson_demo.bronze_noaa.zip_code AS zip_code
# MAGIC --   ON forecasts.post_code = zip_code.post_code
# MAGIC -- WHERE CAST(forecasts.startTimeUTC AS DATE) BETWEEN 
# MAGIC --       current_date() AND 
# MAGIC --       DATE_ADD(current_date(), 10);

# COMMAND ----------

import random
import time
from datetime import datetime, timedelta
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, TimestampType, DoubleType
import json

# Configuration - matches your existing setup
source_table = 'leigh_robertson_demo.bronze_noaa.forecasts_streaming_demo'

# COMMAND ----------

def generate_weather_data_batch(postal_codes, batch_size=100, advanced_patterns=False):
    """Generate simulated weather forecast data for continuous streaming with explicit schema"""
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
    
    # Create base data with random weather patterns
    data = []
    for i in range(batch_size):
        postal_code = random.choice(postal_codes)
        base_time = datetime.now() + timedelta(hours=random.randint(0, 168))  # Next 7 days
        
        # Simulate realistic weather patterns
        temperature = random.randint(-10, 100)
        humidity = random.randint(10, 100)
        precipitation_prob = min(100, max(0, random.randint(0, 100)))
        dewpoint_value = float(temperature - random.randint(10, 30))
        
        # Advanced weather patterns if requested
        if advanced_patterns:
            # Seasonal adjustments
            month = base_time.month
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
            'number': i,
            'name': f'Weather Forecast {i}',
            'startTime': base_time.strftime('%Y-%m-%dT%H:%M:%S-05:00'),
            'endTime': (base_time + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S-05:00'),
            'isDaytime': base_time.hour >= 6 and base_time.hour <= 18,
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
    
    # Create DataFrame with explicit schema
    df = spark.createDataFrame(data, schema=weather_schema)
    
    return df

# COMMAND ----------

def generate_single_weather_record(record_index, postal_codes, advanced_patterns=False):
    """Generate a single weather record as JSON string for RDD processing"""
    import random
    from datetime import datetime, timedelta
    
    postal_code = random.choice(postal_codes)
    base_time = datetime.now() + timedelta(hours=random.randint(0, 168))  # Next 7 days
    
    # Simulate realistic weather patterns
    temperature = random.randint(-10, 100)
    humidity = random.randint(10, 100)
    precipitation_prob = min(100, max(0, random.randint(0, 100)))
    dewpoint_value = float(temperature - random.randint(10, 30))
    
    # Advanced weather patterns if requested
    if advanced_patterns:
        # Seasonal adjustments
        month = base_time.month
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
    
    weather_record = {
        'post_code': str(postal_code),
        'number': record_index,
        'name': f'Weather Forecast {record_index}',
        'startTime': base_time.strftime('%Y-%m-%dT%H:%M:%S-05:00'),
        'endTime': (base_time + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S-05:00'),
        'isDaytime': base_time.hour >= 6 and base_time.hour <= 18,
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
        'audit_update_ts': datetime.now().isoformat()
    }
    
    return json.dumps(weather_record)

def generate_weather_data_batch_rdd(postal_codes, batch_size=100, advanced_patterns=False):
    """Generate weather data using RDD for distributed processing across driver and worker nodes"""
    
    # Create RDD with distributed processing - similar to the original RDD example
    # This distributes the work across multiple partitions (worker nodes)
    num_partitions = min(100, batch_size // 10)  # Reasonable partition count
    
    weather_rdd = sc.parallelize(range(batch_size), num_partitions).map(
        lambda x: generate_single_weather_record(x, postal_codes, advanced_patterns)
    )
    
    # Convert RDD to DataFrame using spark.read.json (same pattern as original)
    weather_df = spark.read.json(weather_rdd)
    
    # Ensure the DataFrame has the correct schema and data types
    weather_df = weather_df.select(
        weather_df.post_code.cast("string"),
        weather_df.number.cast("long"),
        weather_df.name.cast("string"),
        weather_df.startTime.cast("string"),
        weather_df.endTime.cast("string"),
        weather_df.isDaytime.cast("boolean"),
        weather_df.temperature.cast("long"),
        weather_df.temperatureUnit.cast("string"),
        weather_df.temperatureTrend.cast("string"),
        weather_df.probabilityOfPrecipitation,
        weather_df.dewpoint,
        weather_df.relativeHumidity,
        weather_df.windSpeed.cast("string"),
        weather_df.windDirection.cast("string"),
        weather_df.icon.cast("string"),
        weather_df.shortForecast.cast("string"),
        weather_df.detailedForecast.cast("string"),
        weather_df.audit_update_ts.cast("timestamp")
    )
    
    return weather_df

# COMMAND ----------

# Get postal codes from your existing data
postal_codes_df = spark.sql("""
    SELECT DISTINCT post_code 
    FROM leigh_robertson_demo.bronze_noaa.zip_code 
    WHERE state_abbreviation = 'NY'
    LIMIT 1000
""")

postal_codes = [row.post_code for row in postal_codes_df.collect()]
print(f"Using {len(postal_codes)} postal codes for continuous generation")
print(f"Sample postal codes: {postal_codes[:5]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continuous Data Generation Functions

# COMMAND ----------

def start_continuous_generation(duration_minutes=2, batch_interval_seconds=10):
    """
    Start continuous weather data generation
    
    Args:
        duration_minutes: How long to run the generator
        batch_interval_seconds: Seconds between batches
    """
    print(f"Starting continuous weather data generation for {duration_minutes} minutes")
    print(f"Batch interval: {batch_interval_seconds} seconds")
    
    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)
    batch_num = 0
    
    while time.time() < end_time:
        batch_num += 1
        print(f"\n--- Generating Batch {batch_num} ---")
        
        try:
            # Generate weather data with some variability
            batch_size = random.randint(10000, 1000000)  # Variable batch sizes
            new_data = generate_weather_data_batch(postal_codes, batch_size, advanced_patterns=True)
            
            # Write to bronze table
            new_data.write.mode("append").saveAsTable(source_table)
            
            print(f"✅ Batch {batch_num}: {batch_size} records written to {source_table}")
            
            # Wait before next batch
            time.sleep(batch_interval_seconds)
            
        except Exception as e:
            print(f"❌ Error in batch {batch_num}: {e}")
            time.sleep(5)  # Short delay on error
    
    print(f"\n🏁 Continuous generation completed. Generated {batch_num} batches.")

# COMMAND ----------

def start_continuous_generation_rdd(duration_minutes=2, batch_interval_seconds=10):
    """
    Start continuous weather data generation using RDD for distributed processing
    
    Args:
        duration_minutes: How long to run the generator
        batch_interval_seconds: Seconds between batches
    """
    print(f"Starting RDD-based continuous weather data generation for {duration_minutes} minutes")
    print(f"Batch interval: {batch_interval_seconds} seconds")
    
    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)
    batch_num = 0
    
    while time.time() < end_time:
        batch_num += 1
        print(f"\n--- Generating RDD Batch {batch_num} ---")
        
        try:
            # Generate weather data with some variability using RDD
            batch_size = random.randint(10000, 1000000)  # Variable batch sizes
            new_data = generate_weather_data_batch_rdd(postal_codes, batch_size, advanced_patterns=True)
            
            # Write to bronze table
            new_data.write.mode("append").saveAsTable(source_table)
            
            print(f"✅ RDD Batch {batch_num}: {batch_size} records written to {source_table}")
            
            # Wait before next batch
            time.sleep(batch_interval_seconds)
            
        except Exception as e:
            print(f"❌ Error in RDD batch {batch_num}: {e}")
            time.sleep(5)  # Short delay on error
    
    print(f"\n🏁 RDD-based continuous generation completed. Generated {batch_num} batches.")

# COMMAND ----------

def generate_burst_data(num_batches=5, batch_size=100):
    """Generate burst of data for testing streaming performance"""
    print(f"Generating burst of {num_batches} batches with {batch_size} records each")
    
    for i in range(num_batches):
        print(f"Burst batch {i+1}/{num_batches}")
        
        # Generate and write data using the working function
        burst_data = generate_weather_data_batch(postal_codes, batch_size, advanced_patterns=True)
        burst_data.write.mode("append").saveAsTable(source_table)
        
        time.sleep(2)  # Short delay between burst batches
    
    print("Burst generation completed!")

# COMMAND ----------

def generate_burst_data_rdd(num_batches=5, batch_size=100):
    """Generate burst of data using RDD for testing streaming performance"""
    print(f"Generating RDD burst of {num_batches} batches with {batch_size} records each")
    
    for i in range(num_batches):
        print(f"RDD Burst batch {i+1}/{num_batches}")
        
        # Generate and write data using the RDD function
        burst_data = generate_weather_data_batch_rdd(postal_codes, batch_size, advanced_patterns=True)
        burst_data.write.mode("append").saveAsTable(source_table)
        
        time.sleep(2)  # Short delay between burst batches
    
    print("RDD Burst generation completed!")

# COMMAND ----------

def generate_skewed_data(num_records=500):
    """Generate data with intentional skew for testing skew handling"""
    print(f"Generating skewed data with {num_records} records")
    
    # Create skewed distribution - 80% of data goes to top 3 postal codes
    top_postal_codes = postal_codes[:3]
    skewed_codes = top_postal_codes * 40 + postal_codes[3:] * 2
    
    skewed_data = generate_weather_data_batch(skewed_codes, num_records, advanced_patterns=True)
    skewed_data.write.mode("append").saveAsTable(source_table)
    
    print("Skewed data generation completed!")

# COMMAND ----------

def generate_extreme_weather_data(num_records=200):
    """Generate extreme weather conditions for testing edge cases"""
    print(f"Generating extreme weather data with {num_records} records")
    
    # Override random patterns for extreme conditions
    data = []
    for i in range(num_records):
        postal_code = random.choice(postal_codes)
        base_time = datetime.now() + timedelta(hours=random.randint(0, 168))
        
        # Generate extreme conditions
        extreme_type = random.choice(['extreme_cold', 'extreme_heat', 'extreme_precip'])
        
        if extreme_type == 'extreme_cold':
            temperature = random.randint(-30, 10)
            precipitation_prob = random.randint(60, 95)
            short_forecast = 'Snow'
        elif extreme_type == 'extreme_heat':
            temperature = random.randint(95, 115)
            precipitation_prob = random.randint(0, 20)
            short_forecast = 'Sunny'
        else:  # extreme_precip
            temperature = random.randint(40, 80)
            precipitation_prob = random.randint(85, 100)
            short_forecast = 'Thunderstorms'
        
        humidity = random.randint(10, 100)
        dewpoint_value = float(temperature - random.randint(10, 30))
        
        data.append({
            'post_code': str(postal_code),
            'number': i,
            'name': f'Extreme Weather Forecast {i}',
            'startTime': base_time.strftime('%Y-%m-%dT%H:%M:%S-05:00'),
            'endTime': (base_time + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S-05:00'),
            'isDaytime': base_time.hour >= 6 and base_time.hour <= 18,
            'temperature': temperature,
            'temperatureUnit': 'F',
            'temperatureTrend': random.choice(['rising', 'falling']),
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
            'windSpeed': f"{random.randint(15, 45)} mph",
            'windDirection': random.choice(['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']),
            'icon': f'https://api.weather.gov/icons/land/day/{short_forecast.lower()}',
            'shortForecast': short_forecast,
            'detailedForecast': f'EXTREME CONDITIONS: {short_forecast} with temperature {temperature}°F and {precipitation_prob}% precipitation chance.',
            'audit_update_ts': datetime.now()
        })
    
    # Use same schema as the base function
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
    
    extreme_df = spark.createDataFrame(data, schema=weather_schema)
    extreme_df.write.mode("append").saveAsTable(source_table)
    
    print("Extreme weather data generation completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Examples

# COMMAND ----------

# # Example 1: Generate initial data batch
# print("Example 1: Generating initial batch of weather data...")
# initial_batch = generate_weather_data_batch(postal_codes, 200, advanced_patterns=True)
# initial_batch.write.mode("append").saveAsTable(source_table)
# print("✅ Initial batch generated")

# COMMAND ----------

# Example 2: Generate continuous data (uncomment to run)
start_continuous_generation(duration_minutes=5, batch_interval_seconds=3)
#start_continuous_generation_rdd(duration_minutes=2, batch_interval_seconds=3)

# COMMAND ----------

# Example 3: Generate burst data for performance testing (uncomment to run)
# generate_burst_data(num_batches=10, batch_size=150)

# COMMAND ----------

# Example 4: Generate skewed data for skew testing (uncomment to run)
# generate_skewed_data(num_records=1000)

# COMMAND ----------

# Example 5: Generate extreme weather data (uncomment to run)
# generate_extreme_weather_data(num_records=300)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions for Demo Support

# COMMAND ----------

def check_data_volume():
    """Check current data volume in bronze table"""
    count_df = spark.sql(f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT post_code) as unique_postal_codes,
            MIN(audit_update_ts) as earliest_record,
            MAX(audit_update_ts) as latest_record
        FROM {source_table}
    """)
    
    print("Current data volume:")
    count_df.display()

def check_data_distribution():
    """Check data distribution by postal code"""
    dist_df = spark.sql(f"""
        SELECT 
            post_code,
            COUNT(*) as record_count,
            AVG(temperature) as avg_temp,
            AVG(cast(probabilityOfPrecipitation.value as double)) as avg_precipitation
        FROM {source_table}
        GROUP BY post_code
        ORDER BY record_count DESC
        LIMIT 15
    """)
    
    print("Data distribution by postal code:")
    dist_df.display()

def check_weather_patterns():
    """Check weather patterns and extreme conditions"""
    patterns_df = spark.sql(f"""
        SELECT 
            shortForecast,
            COUNT(*) as count,
            AVG(temperature) as avg_temp,
            MIN(temperature) as min_temp,
            MAX(temperature) as max_temp,
            AVG(cast(probabilityOfPrecipitation.value as double)) as avg_precipitation
        FROM {source_table}
        GROUP BY shortForecast
        ORDER BY count DESC
    """)
    
    print("Weather patterns distribution:")
    patterns_df.display()

# COMMAND ----------

# # Check current state
# check_data_volume()
# check_data_distribution()

# COMMAND ----------

print("""
🌦️  WEATHER DATA GENERATOR READY! (Updated with Working Schema)

✅ NEW FEATURES:
- Uses the working generate_weather_data_batch function with explicit schema
- Added advanced weather patterns option
- New extreme weather data generation function
- Schema-safe data generation - no more type inference errors!

Usage Instructions:
1. Run the initial batch generation above
2. Uncomment and run continuous generation while running the DSA_SS_Demo.py
3. Use burst generation to test performance handling
4. Use skewed generation to test skew handling
5. NEW: Use extreme weather generation to test edge cases

Recommended workflow:
1. Start DSA_SS_Demo.py in another notebook
2. Run start_continuous_generation() here to provide streaming data
3. Observe how the streaming demo handles the continuous data flow

All functions now use the same working schema as DSA_SS_Demo.py!
""") 
