# Spark Declarative Pipeline Examples

This repository contains a declarative pipeline example for ingesting and processing NOAA weather data using Databricks. The pipeline demonstrates best practices for data ingestion, transformation, and management using Delta Lake tables with primary keys and foreign keys.

## Overview

The pipeline consists of four main notebooks that work together to:
1. Configure centralized variables for the entire pipeline
2. Set up database schemas and tables with proper constraints
3. Load zip code reference data from the Zippopotam.us API
4. Ingest weather forecast data from the NOAA API

## Prerequisites

- Databricks workspace access
- Python environment with required packages:
  - `requests` (for API calls)
  - `pandas` (for data manipulation)
  - `noaa_sdk` (for NOAA weather data - installed via `%pip install` in notebook)
- Access to the `sql_ingestion_framework/utils_file` notebook for utility functions

## Setup Instructions

### Step 1: Configure Variables

First, update the configuration variables in `spark_declaritive_pipeline_examples/explorations/00_variables.py`:

- **Catalog and Schema Configuration**: Update `catalog`, `bronze_schema`, and `silver_schema` to match your Databricks environment
- **Table Names**: Configure base table names if needed
- **Zip Code Range**: Set `start_zip` and `end_zip` to define the range of zip codes to load (default: "00210" to "99950" for US zip codes)
- **Weather Configuration**: Set `default_state` and `default_country_code` for weather data ingestion
- **Workspace Host**: Update `workspace_host` with your Databricks workspace URL

### Step 2: Create Database Schema and Tables

Run `spark_declaritive_pipeline_examples/explorations/01_set_up_work.py` to:
- Create the catalog and schemas (bronze and silver layers)
- Create the `zip_code` table in the bronze schema
- Create the `forecasts` table in the bronze schema with primary key and foreign key constraints
- Create the `forecasts_expanded` table in the silver schema with primary key and foreign key constraints

**Note**: The DDL statements are commented out by default. Uncomment the `spark.sql()` calls to execute the table creation statements.

### Step 3: Load Zip Code Reference Data

Run `spark_declaritive_pipeline_examples/explorations/02_zip_code.py` to populate zip code data from the Zippopotam.us API.

**Important Notes**:
- This notebook will iterate through all zip codes in the configured range
- The process includes a 5-second delay between API calls to respect rate limits
- **This backfill process can take a significant amount of time** depending on the zip code range configured
- The notebook uses MERGE statements to handle updates and inserts, so it can be safely re-run

### Step 4: Load Weather Forecast Data

Run `spark_declaritive_pipeline_examples/explorations/03_weather_ingest.py` to load weather forecast data for a specific state.

**Important Notes**:
- This notebook uses a widget to accept a state parameter (defaults to the value in `00_variables.py`)
- The notebook queries all zip codes for the specified state and loads forecasts for each
- **NOAA API Rate Limits**: The NOAA API enforces rate limits on requests. While the default limits are reasonably high, **registration with NOAA is required** to access higher rate limits and ensure reliable access to the API
- After loading forecasts, the notebook automatically runs OPTIMIZE and VACUUM operations on the forecast table
- This notebook can be run continuously or scheduled as a job to keep weather data up-to-date

## Pipeline Architecture

### Bronze Layer
- **zip_code**: Reference table containing zip code information with geographic coordinates
- **forecasts**: Raw forecast data from NOAA API with nested structures preserved

### Silver Layer
- **forecasts_expanded**: Transformed forecast data with flattened structures and additional computed fields (timezone offsets, UTC timestamps, etc.)

## Transformation Patterns

This repository demonstrates several common data transformation and loading patterns using Databricks Delta Live Tables (DLT) and streaming tables. These patterns are the core value of this repository and show how to handle different data loading scenarios.

### Append-Only Streaming Table

**File**: `transformations/append_only_example_streaming_table.sql`

Demonstrates how to create a streaming table with append-only semantics using SQL. This pattern is ideal for:
- Event-driven data where records are never updated
- High-throughput data ingestion
- Real-time data processing pipelines

**Key Features**:
- Uses `CREATE OR REFRESH STREAMING TABLE` syntax
- Includes data quality constraints (expectations) that drop invalid rows
- Transforms nested structures and extracts timezone information
- Automatically handles streaming data from source tables

### Append-Only to Standard Delta Table

**File**: `transformations/append_to_standard_delta_table.py`

Shows how to load data into a standard Delta table using an append-only pattern with SDP. This pattern is useful when:
- You need a standard Delta table (not a streaming table) for downstream consumption
- You want to use SDP's declarative syntax for transformations
- You need append-only semantics but require standard table access patterns

**Key Features**:
- Uses DLT's `create_sink()` function to create a Delta table target
- Implements `@dlt.append_flow()` decorator for append-only operations
- Transforms data during the append process
- Maintains data quality through transformations

### SCD Type 2 from Snapshot Load

**File**: `transformations/snapshot_load_scd2.py`

Demonstrates how to create a Slowly Changing Dimension Type 2 (SCD2) table from a snapshot source. This pattern is essential for:
- Tracking historical changes to dimension data
- Maintaining full audit trails of data changes
- Supporting time-based queries and point-in-time analysis

**Key Features**:
- Uses `dp.create_auto_cdc_from_snapshot_flow()` to automatically detect changes
- Creates SCD Type 2 records with effective dates
- Handles inserts, updates, and deletes automatically
- Maintains historical versions of changed records

### SCD Type 1 with Upsert Handling

**File**: `transformations/upsert_example_scd1.py`

Shows how to handle upserts at the source and correctly process them using SCD Type 1 semantics. This pattern is ideal for:
- Sources that send updates to existing records
- When you need the latest version of data (no history required)
- Change Data Feed (CDF) enabled source tables
- Real-time updates to dimension tables

**Key Features**:
- Reads from Change Data Feed using `readChangeFeed` option
- Uses `dp.create_auto_cdc_flow()` with `stored_as_scd_type=1` for upsert semantics
- Includes data quality expectations using `@dp.expect()` to filter invalid records
- Transforms nested structures and adds audit columns
- Sequences changes by timestamp to handle out-of-order updates

## Utility Functions

The pipeline relies on utility functions from `sql_ingestion_framework/utils_file.py`:
- `generate_match_insert_columns()`: Generates column lists for MERGE operations
- `dynamic_merge_sql()`: Creates dynamic MERGE SQL statements for upsert operations

## Data Sources

- **Zippopotam.us API**: Provides zip code and location data
  - API Documentation: https://www.zippopotam.us/
- **NOAA Weather API**: Provides weather forecast data
  - Requires registration for production use
  - Access via `noaa_sdk` Python package

## Notes

- All tables use `CLUSTER BY AUTO` for automatic clustering optimization
- Tables include `audit_update_ts` timestamp columns for tracking data freshness
- The pipeline uses MERGE operations to handle both inserts and updates idempotently
- Foreign key constraints ensure referential integrity between zip codes and forecasts

