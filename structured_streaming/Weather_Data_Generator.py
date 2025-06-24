# Databricks notebook source
# MAGIC %md
# MAGIC # Weather Data Generator for Streaming Demo
# MAGIC ## Continuous data generation to simulate real-time weather updates
# MAGIC 
# MAGIC This script generates realistic weather data and writes it to your bronze table
# MAGIC to provide continuous streaming data for the DSA_SS_Demo.py

# COMMAND ----------

import random
import time
from datetime import datetime, timedelta
from pyspark.sql.functions import current_timestamp, lit

# Configuration - matches your existing setup
source_table = 'leigh_robertson_demo.bronze_noaa.forecasts'

# COMMAND ----------

def generate_realistic_weather_batch(postal_codes, batch_size=100):
    """Generate realistic weather forecast data for continuous streaming"""
    
    # Create base data with realistic weather patterns
    data = []
    for i in range(batch_size):
        postal_code = random.choice(postal_codes)
        
        # Generate realistic timestamps (next 7 days)
        base_time = datetime.now() + timedelta(hours=random.randint(0, 168))
        start_time = base_time.strftime('%Y-%m-%dT%H:%M:%S-05:00')
        end_time = (base_time + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S-05:00')
        
        # Generate realistic weather patterns
        temperature = random.randint(-10, 100)
        humidity = random.randint(10, 100)
        precipitation_prob = random.randint(0, 100)
        wind_speed = random.randint(5, 35)
        
        # Create realistic weather conditions
        weather_conditions = ['Sunny', 'Partly Cloudy', 'Cloudy', 'Rain', 'Snow', 'Thunderstorms', 'Fog']
        condition = random.choice(weather_conditions)
        
        # Adjust probability based on condition
        if condition in ['Rain', 'Thunderstorms', 'Snow']:
            precipitation_prob = max(precipitation_prob, 60)
        elif condition == 'Sunny':
            precipitation_prob = min(precipitation_prob, 20)
        
        data.append({
            'post_code': str(postal_code),
            'number': random.randint(1, 1000),
            'name': f'Weather Forecast {i}',
            'startTime': start_time,
            'endTime': end_time,
            'isDaytime': base_time.hour >= 6 and base_time.hour <= 18,
            'temperature': temperature,
            'temperatureUnit': 'F',
            'temperatureTrend': random.choice([None, 'rising', 'falling']),
            'probabilityOfPrecipitation': {'value': precipitation_prob},
            'dewpoint': {'value': float(temperature - random.randint(10, 30))},
            'relativeHumidity': {'value': humidity},
            'windSpeed': f"{wind_speed} mph",
            'windDirection': random.choice(['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']),
            'icon': f'https://api.weather.gov/icons/land/day/{condition.lower()}',
            'shortForecast': condition,
            'detailedForecast': f'Temperature around {temperature}°F with {precipitation_prob}% chance of precipitation. {condition} conditions expected.'
        })
    
    df = spark.createDataFrame(data)
    return df.withColumn('audit_update_ts', current_timestamp())

# COMMAND ----------

# Get postal codes from your existing data
postal_codes_df = spark.sql("""
    SELECT DISTINCT post_code 
    FROM leigh_robertson_demo.bronze_noaa.zip_code 
    WHERE state_abbreviation = 'NY'
    LIMIT 25
""")

postal_codes = [row.post_code for row in postal_codes_df.collect()]
print(f"Using {len(postal_codes)} postal codes for continuous generation")
print(f"Postal codes: {postal_codes}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continuous Data Generation Functions

# COMMAND ----------

def start_continuous_generation(duration_minutes=30, batch_interval_seconds=10):
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
            batch_size = random.randint(20, 50)  # Variable batch sizes
            new_data = generate_realistic_weather_batch(postal_codes, batch_size)
            
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

def generate_burst_data(num_batches=5, batch_size=100):
    """Generate burst of data for testing streaming performance"""
    print(f"Generating burst of {num_batches} batches with {batch_size} records each")
    
    for i in range(num_batches):
        print(f"Burst batch {i+1}/{num_batches}")
        
        # Generate and write data
        burst_data = generate_realistic_weather_batch(postal_codes, batch_size)
        burst_data.write.mode("append").saveAsTable(source_table)
        
        time.sleep(2)  # Short delay between burst batches
    
    print("Burst generation completed!")

# COMMAND ----------

def generate_skewed_data(num_records=500):
    """Generate data with intentional skew for testing skew handling"""
    print(f"Generating skewed data with {num_records} records")
    
    # Create skewed distribution - 80% of data goes to top 3 postal codes
    top_postal_codes = postal_codes[:3]
    skewed_codes = top_postal_codes * 40 + postal_codes[3:] * 2
    
    skewed_data = generate_realistic_weather_batch(skewed_codes, num_records)
    skewed_data.write.mode("append").saveAsTable(source_table)
    
    print("Skewed data generation completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Examples

# COMMAND ----------

# Example 1: Generate initial data batch
print("Example 1: Generating initial batch of weather data...")
initial_batch = generate_realistic_weather_batch(postal_codes, 200)
initial_batch.write.mode("append").saveAsTable(source_table)
print("✅ Initial batch generated")

# COMMAND ----------

# Example 2: Generate continuous data (uncomment to run)
# start_continuous_generation(duration_minutes=15, batch_interval_seconds=5)

# COMMAND ----------

# Example 3: Generate burst data for performance testing (uncomment to run)
# generate_burst_data(num_batches=10, batch_size=150)

# COMMAND ----------

# Example 4: Generate skewed data for skew testing (uncomment to run)
# generate_skewed_data(num_records=1000)

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

# COMMAND ----------

# Check current state
check_data_volume()
check_data_distribution()

# COMMAND ----------

print("""
🌦️  WEATHER DATA GENERATOR READY!

Usage Instructions:
1. Run the initial batch generation above
2. Uncomment and run continuous generation while running the DSA_SS_Demo.py
3. Use burst generation to test performance handling
4. Use skewed generation to test skew handling

Recommended workflow:
1. Start DSA_SS_Demo.py in another notebook
2. Run start_continuous_generation() here to provide streaming data
3. Observe how the streaming demo handles the continuous data flow
""") 