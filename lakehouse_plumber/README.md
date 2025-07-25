# Weather Data Pipeline - Lakehouse Plumber Framework

This project implements a weather data pipeline using the Lakehouse Plumber framework to transform data from bronze to silver and gold layers in Databricks.

## Project Overview

The pipeline processes weather forecast data through three layers:

- **Bronze Layer**: Raw weather data from `forecasts_streaming_demo` table
- **Silver Layer**: Cleaned and standardized weather data with timezone normalization
- **Gold Layer**: Daily aggregated metrics and regional summaries for analytics

## Architecture

```
Bronze Layer (forecasts_streaming_demo)
    ↓
Silver Layer (weather_forecasts) - SCD Type 1
    ↓  
Gold Layer (daily_weather_metrics, regional_weather_summary)
```

## Prerequisites

1. **Install Lakehouse Plumber**:
   ```bash
   pip install lakehouse-plumber
   ```

2. **Databricks Environment**:
   - Databricks workspace with Lakeflow Declarative Pipelines (DLT) enabled
   - Access to `leigh_robertson_demo` catalog
   - Existing bronze table: `leigh_robertson_demo.bronze_noaa.forecasts_streaming_demo`

## Project Structure

```
lakehouse_plumber/
├── lhp.yaml                           # Main project configuration
├── substitutions/                     # Environment-specific variables
│   ├── dev.yaml                      #   Development environment
│   ├── staging.yaml                  #   Staging environment  
│   └── prod.yaml                     #   Production environment
├── presets/                          # Reusable configuration templates
│   ├── silver_layer.yaml            #   Silver layer standards
│   └── gold_layer.yaml              #   Gold layer standards
├── pipelines/                        # Pipeline definitions
│   └── weather_data_pipeline_combined.yaml  # Combined Bronze → Silver → Gold pipeline
└── generated/                        # Auto-generated Python code (created by LHP)
```

## Pipeline Details

### Combined Weather Data Pipeline (`weather_data_pipeline_combined.yaml`)

**Complete Bronze → Silver → Gold flow in a single DLT pipeline:**

#### Silver Layer Transformations:
- Loads bronze weather data with CDC (Change Data Capture)
- Extracts timezone information from timestamps and converts to UTC
- Flattens nested JSON structures (dewpoint, precipitation, humidity)
- Extracts numeric wind speed from string values
- Implements SCD Type 1 for weather forecast updates
- Data quality validations (temperature range, humidity bounds)

#### Gold Layer Aggregations:
- Daily aggregations with **one-day lag** for analytics stability
- Temperature statistics (min, max, avg) by postal code and date
- Weather condition analysis (sunny, rainy, cloudy, snow percentages)
- Extreme weather tracking (heat waves, freezing, high precipitation)
- Regional summaries for dashboard consumption
- Materialized view with scheduled daily refresh (2 AM)

**Outputs:**
- `{catalog}.{silver_schema}.weather_forecasts` (Silver Streaming Table)
- `{catalog}.{gold_schema}.daily_weather_metrics` (Gold Materialized View)
- `{catalog}.{gold_schema}.regional_weather_summary` (Gold Streaming Table)

## Usage

### 1. Initialize and Validate

```bash
# Navigate to the lakehouse_plumber directory
cd lakehouse_plumber

# Validate configuration
lhp validate --env dev

# Check pipeline structure
lhp validate --env dev --verbose
```

### 2. Generate Python Code

```bash
# Generate production-ready Python code
lhp generate --env dev --cleanup

# For other environments
lhp generate --env staging --cleanup
lhp generate --env prod --cleanup
```

### 3. Deploy with Databricks Asset Bundles

```bash
# Deploy to development
databricks bundle deploy --target dev

# Deploy to staging  
databricks bundle deploy --target staging

# Deploy to production
databricks bundle deploy --target prod
```

### 4. Monitor Pipeline

The generated code creates DLT pipelines that can be monitored through:
- Databricks DLT Pipeline UI
- System tables for data lineage
- Data quality metrics dashboard

## Environment Configuration

### Development (`dev.yaml`)
- Schema: `silver_noaa`, `gold_noaa`  
- Checkpoints: `/tmp/checkpoints/weather_pipeline`
- Retention: 30 days

### Staging (`staging.yaml`)
- Schema: `silver_noaa_staging`, `gold_noaa_staging`
- Checkpoints: `/tmp/checkpoints/weather_pipeline_staging`
- Retention: 60 days

### Production (`prod.yaml`)
- Schema: `silver_noaa_prod`, `gold_noaa_prod`
- Checkpoints: `/mnt/production/checkpoints/weather_pipeline`
- Retention: 365 days

## Data Quality Features

### Silver Layer Expectations
- Temperature range validation (-50°F to 150°F)
- Humidity bounds (0-100%)
- Non-null postal code enforcement
- Wind speed validity checks

### Gold Layer Expectations  
- Valid aggregation dates
- Logical temperature relationships (min ≤ max)
- Data completeness checks

## Key Benefits

1. **Declarative Configuration**: Pipeline logic defined in YAML, not code
2. **Environment Consistency**: Same logic across dev/staging/prod with variable substitution
3. **Auto-Generated Code**: Production-ready Python DLT code generated automatically
4. **Data Quality**: Built-in expectations and monitoring
5. **Incremental Processing**: Streaming tables with SCD Type 1 support
6. **Performance Optimization**: Auto-compaction, optimize write, clustering

## Integration with Weather Data Generator

This pipeline is designed to work with the `Weather_Data_Generator.py` which provides:
- Continuous data generation for testing streaming pipelines
- Realistic weather patterns and seasonal adjustments
- Configurable batch sizes and generation patterns

Start the generator to provide continuous data:
```python
# In Weather_Data_Generator.py
start_continuous_generation(duration_minutes=10, batch_interval_seconds=3)
```

## Troubleshooting

### Common Issues

1. **Schema Not Found**: Ensure the target schemas exist or have proper permissions
2. **Checkpoint Conflicts**: Clear checkpoint locations if switching between environments  
3. **Table Permissions**: Verify read/write access to bronze source tables

### Validation Commands

```bash
# Check YAML syntax
lhp validate --env dev

# Dry run generation
lhp generate --env dev --dry-run

# Check dependencies
lhp dependencies --env dev
```

## Next Steps

1. **Extend Gold Layer**: Add weekly/monthly aggregations
2. **Add Alerts**: Implement data quality alert notifications  
3. **Dashboard Integration**: Connect gold tables to BI tools
4. **ML Features**: Create feature stores for weather prediction models
5. **Cost Optimization**: Implement table maintenance schedules

## References

- [Lakehouse Plumber Documentation](https://lakehouse-plumber.readthedocs.io)
- [Databricks Lakeflow Declarative Pipelines](https://docs.databricks.com/en/delta-live-tables/index.html)
- [Weather Data Generator](../code_examples/noaa_weather_ingest/Weather_Data_Generator.py) 