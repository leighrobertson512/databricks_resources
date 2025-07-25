# Quick Start Guide - Weather Data Pipeline

Get your weather data pipeline running in 5 minutes with Lakehouse Plumber!

## 🚀 Quick Setup

1. **Run the setup script**:
   ```bash
   cd lakehouse_plumber
   ./setup.sh
   ```

2. **Activate the environment**:
   ```bash
   source venv/bin/activate
   ```

3. **Generate the pipeline code**:
   ```bash
   lhp generate --env dev --cleanup
   ```

4. **Check the generated files**:
   ```bash
   ls -la generated/
   ```

## 📊 What Gets Created

### Silver Layer
- **Table**: `leigh_robertson_demo.silver_noaa.weather_forecasts`
- **Features**: 
  - Timezone normalization
  - Flattened JSON structures
  - SCD Type 1 updates
  - Data quality validations

### Gold Layer
- **Materialized View**: `leigh_robertson_demo.gold_noaa.daily_weather_metrics`
- **Summary Table**: `leigh_robertson_demo.gold_noaa.regional_weather_summary`
- **Features**:
  - Daily aggregations with 1-day lag
  - Temperature min/max/avg by postal code
  - Weather condition percentages
  - Extreme weather tracking

## 🔄 Testing the Pipeline

1. **Start the weather data generator** (in another notebook):
   ```python
   # In Weather_Data_Generator.py
   start_continuous_generation(duration_minutes=5, batch_interval_seconds=10)
   ```

2. **Deploy and run the pipelines**:
   ```bash
   databricks bundle deploy --target dev
   ```

3. **Monitor in Databricks**:
   - Go to DLT Pipelines in your Databricks workspace
   - Look for `weather_data_pipeline_dev`
   - Start the pipeline to process your streaming data through Bronze → Silver → Gold layers

## 📁 Generated Files Structure

```
generated/
├── weather_data_pipeline/
│   └── weather_data_processing.py # Combined Bronze → Silver → Gold DLT code
└── bundles/
    └── databricks.yml             # Asset bundle configuration
```

## 🔧 Customization

### Modify Pipeline Logic
Edit `pipelines/weather_data_pipeline_combined.yaml` and regenerate:

```bash
lhp generate --env dev --cleanup
```

### Change Environment Settings
Edit `substitutions/dev.yaml` for development settings.

### Add New Transformations
1. Create new YAML files in `pipelines/`
2. Reference them in your pipeline configurations
3. Regenerate with `lhp generate`

## 🎯 Next Steps

- **Add monitoring**: Set up alerts on pipeline failures
- **Extend gold layer**: Add weekly/monthly aggregations
- **Connect BI tools**: Use gold tables in dashboards
- **Add ML features**: Create feature stores for weather predictions

## 🆘 Need Help?

- Check the full [README.md](README.md) for detailed documentation
- Validate your configuration: `lhp validate --env dev --verbose`
- View generated code before deployment: `lhp generate --env dev --dry-run`

Happy data engineering! 🌦️📊 