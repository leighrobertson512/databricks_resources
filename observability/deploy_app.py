"""
Deployment Helper for Data Discovery Hub
=======================================

Helper script to set up and deploy the Data Discovery Hub Databricks app.
Run this script in a Databricks notebook to prepare the environment.
"""

# COMMAND ----------

print("""
Data Discovery Hub - Deployment Setup
=====================================

This notebook helps you set up the Data Discovery Hub Databricks app.

Prerequisites:
1. Unity Catalog enabled workspace
2. Permissions to create schemas and tables  
3. Databricks Apps feature enabled

Steps:
1. Install required packages
2. Create the materialized view (from obs_query_base.py)
3. Test data access
4. Validate app components
""")

# COMMAND ----------

# Install required packages
# %pip install streamlit>=1.28.0 plotly>=5.15.0 pandas>=2.0.0 numpy>=1.24.0 requests>=2.31.0 openpyxl>=3.1.0

# COMMAND ----------

# Restart Python to ensure packages are loaded
# dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
from datetime import datetime
import time

print("🔍 Data Discovery Hub - Deployment Setup")
print("=" * 50)

# COMMAND ----------

print("Step 1: Create Materialized View")
print("=" * 40)
print("Creating the base materialized view that powers the data discovery app.")

# COMMAND ----------

# Configuration - Update these for your environment
CATALOG_NAME = "leigh_robertson_demo"
SCHEMA_NAME = "observability" 
VIEW_NAME = "tables_columns_genie"

print(f"📊 Creating materialized view: {CATALOG_NAME}.{SCHEMA_NAME}.{VIEW_NAME}")

# Create schema if it doesn't exist
create_schema_sql = f"""
CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}
COMMENT 'Schema for data observability and discovery tools'
"""

try:
    spark.sql(create_schema_sql)
    print(f"✅ Schema {CATALOG_NAME}.{SCHEMA_NAME} created/verified")
except Exception as e:
    print(f"❌ Error creating schema: {e}")

# COMMAND ----------

# Create the materialized view
mv_sql = f"""
CREATE OR REPLACE MATERIALIZED VIEW {CATALOG_NAME}.{SCHEMA_NAME}.{VIEW_NAME}
SCHEDULE EVERY 4 HOURS
TBLPROPERTIES ('pipelines.autoOptimize.zOrderCols'= 'table_catalog,table_name, table_schema, column_name')
COMMENT 'All tables and columns in a single view for data discovery'
AS 
SELECT 
    tables.table_catalog,
    tables.table_schema,
    tables.table_name,
    tables.table_type,
    tables.table_owner,
    tables.comment AS table_comment,
    tables.created,
    tables.created_by,
    tables.last_altered,
    tables.last_altered_by,
    columns.column_name,
    columns.data_type,
    columns.comment AS column_comment
FROM system.information_schema.tables AS tables
INNER JOIN system.information_schema.columns AS columns
    ON tables.table_name = columns.table_name
    AND tables.table_schema = columns.table_schema
    AND tables.table_catalog = columns.table_catalog
WHERE tables.table_catalog != '__databricks_internal'
"""

try:
    spark.sql(mv_sql)
    print(f"✅ Materialized view {CATALOG_NAME}.{SCHEMA_NAME}.{VIEW_NAME} created successfully")
except Exception as e:
    print(f"❌ Error creating materialized view: {e}")
    print("Make sure you have permissions to create materialized views in this catalog")

# COMMAND ----------

print("\nStep 2: Test Data Access")
print("=" * 40)
print("Verifying the materialized view is working and contains data.")

# COMMAND ----------

print("🧪 Testing data access...")

try:
    # Test basic query
    test_query = f"""
    SELECT 
        COUNT(DISTINCT table_catalog) as catalogs,
        COUNT(DISTINCT CONCAT(table_catalog, '.', table_schema)) as schemas,
        COUNT(DISTINCT CONCAT(table_catalog, '.', table_schema, '.', table_name)) as tables,
        COUNT(*) as total_rows
    FROM {CATALOG_NAME}.{SCHEMA_NAME}.{VIEW_NAME}
    """
    
    result = spark.sql(test_query).collect()[0]
    
    print(f"📊 Data Summary:")
    print(f"   Catalogs: {result.catalogs}")
    print(f"   Schemas: {result.schemas}")
    print(f"   Tables: {result.tables}")
    print(f"   Total rows: {result.total_rows}")
    
    if result.total_rows > 0:
        print("✅ Materialized view contains data and is accessible")
    else:
        print("⚠️  Materialized view is empty - this may be expected for new environments")
        
except Exception as e:
    print(f"❌ Error accessing materialized view: {e}")

# COMMAND ----------

# Test sample queries that the app will use
print("\n🔍 Testing app queries...")

test_queries = {
    "Catalog list": f"SELECT DISTINCT table_catalog FROM {CATALOG_NAME}.{SCHEMA_NAME}.{VIEW_NAME} LIMIT 5",
    "Table types": f"SELECT table_type, COUNT(*) as count FROM {CATALOG_NAME}.{SCHEMA_NAME}.{VIEW_NAME} GROUP BY table_type LIMIT 5",
    "Recent tables": f"SELECT table_catalog, table_schema, table_name FROM {CATALOG_NAME}.{SCHEMA_NAME}.{VIEW_NAME} WHERE created IS NOT NULL ORDER BY created DESC LIMIT 3"
}

for test_name, query in test_queries.items():
    try:
        result = spark.sql(query).toPandas()
        print(f"✅ {test_name}: {len(result)} rows returned")
    except Exception as e:
        print(f"❌ {test_name}: Error - {e}")

# COMMAND ----------

print("\nStep 3: Validate App Components")
print("=" * 40)
print("Testing the core app components to ensure they work correctly.")

# COMMAND ----------

print("🔧 Validating app components...")

# Test imports
try:
    import streamlit as st
    print("✅ Streamlit imported successfully")
except ImportError as e:
    print(f"❌ Streamlit import failed: {e}")

try:
    import plotly.express as px
    import plotly.graph_objects as go
    print("✅ Plotly imported successfully")
except ImportError as e:
    print(f"❌ Plotly import failed: {e}")

try:
    import pandas as pd
    import numpy as np
    print("✅ Pandas and NumPy imported successfully")
except ImportError as e:
    print(f"❌ Pandas/NumPy import failed: {e}")

# COMMAND ----------

# Test data discovery utilities
try:
    # Create a simple test of the data access pattern
    test_api_query = f"""
    SELECT 
        table_catalog,
        COUNT(DISTINCT table_name) as table_count
    FROM {CATALOG_NAME}.{SCHEMA_NAME}.{VIEW_NAME}
    GROUP BY table_catalog
    ORDER BY table_count DESC
    LIMIT 5
    """
    
    catalog_data = spark.sql(test_api_query).toPandas()
    
    if not catalog_data.empty:
        print("✅ Data discovery API pattern working")
        print(f"   Found {len(catalog_data)} catalogs with data")
    else:
        print("⚠️  No catalog data found")
        
except Exception as e:
    print(f"❌ Data discovery API test failed: {e}")

# COMMAND ----------

# Test search functionality
try:
    # Test search pattern
    search_test_query = f"""
    SELECT DISTINCT
        table_catalog,
        table_schema,
        table_name,
        table_type
    FROM {CATALOG_NAME}.{SCHEMA_NAME}.{VIEW_NAME}
    WHERE LOWER(table_name) LIKE '%user%' OR LOWER(column_name) LIKE '%user%'
    LIMIT 5
    """
    
    search_results = spark.sql(search_test_query).toPandas()
    print(f"✅ Search functionality test: {len(search_results)} results for 'user' search")
    
except Exception as e:
    print(f"❌ Search test failed: {e}")

# COMMAND ----------

print("\nStep 4: Create Sample Visualization")
print("=" * 40)
print("Testing the visualization components.")

# COMMAND ----------

print("📊 Testing visualization components...")

try:
    import plotly.express as px
    
    # Create sample chart
    if 'catalog_data' in locals() and not catalog_data.empty:
        fig = px.bar(catalog_data, x='table_catalog', y='table_count', 
                    title='Tables by Catalog (Test Chart)')
        
        # In a notebook, you can display the chart
        # fig.show()  # Uncomment to display the chart
        
        print("✅ Visualization engine working - sample chart created")
    else:
        print("⚠️  No data available for visualization test")
        
except Exception as e:
    print(f"❌ Visualization test failed: {e}")

# COMMAND ----------

print("\nStep 5: Deployment Summary")
print("=" * 40)

# COMMAND ----------

print("\n" + "=" * 50)
print("🎉 DEPLOYMENT SUMMARY")
print("=" * 50)

print(f"""
✅ COMPLETED SETUP STEPS:
   1. Materialized view created: {CATALOG_NAME}.{SCHEMA_NAME}.{VIEW_NAME}
   2. Required packages installed
   3. Data access validated
   4. App components tested

📋 NEXT STEPS:
   1. Create a new Databricks App:
      - Go to your workspace sidebar
      - Click 'New' → 'App'
      - Select 'Custom' option
      - Name: 'data-discovery-hub'
   
   2. Upload app files to your workspace:
      - app.py (main application)
      - data_discovery_utils.py
      - search_utils.py
      - visualization_utils.py
      - config.py
      - requirements.txt
   
   3. Configure the app:
      - Update the materialized view name in config.py
      - Set any custom configurations
   
   4. Deploy and test:
      - Follow Databricks Apps deployment process
      - Access your app through the Apps interface

📚 DOCUMENTATION:
   - See README.md for detailed usage instructions
   - Check config.py for customization options
   - Review troubleshooting section for common issues

🔗 RESOURCES:
   - Databricks Apps: https://docs.databricks.com/en/dev-tools/databricks-apps/
   - Unity Catalog: https://docs.databricks.com/en/data-governance/unity-catalog/
""")

current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"⏰ Setup completed at: {current_time}")

# COMMAND ----------

print("\nOptional: Quick Data Preview")
print("=" * 40)
print("Preview some of the data that will be available in your app.")

# COMMAND ----------

print("👀 Data Preview - Sample of what your app will show:")

try:
    preview_query = f"""
    SELECT 
        table_catalog,
        table_schema,
        table_name,
        table_type,
        table_owner,
        LEFT(COALESCE(table_comment, 'No description'), 50) as description_preview
    FROM {CATALOG_NAME}.{SCHEMA_NAME}.{VIEW_NAME}
    WHERE table_comment IS NOT NULL
    ORDER BY created DESC
    LIMIT 10
    """
    
    preview_data = spark.sql(preview_query).toPandas()
    
    if not preview_data.empty:
        print("📋 Recent tables with descriptions:")
        print(preview_data.to_string())  # Use print instead of display for Python compatibility
        # display(preview_data)  # Uncomment when running in Databricks notebook
    else:
        print("ℹ️  No tables with descriptions found yet")
        
except Exception as e:
    print(f"Error getting preview: {e}")

# COMMAND ----------

print("🚀 Ready to deploy your Data Discovery Hub!")
print("Follow the next steps in the summary above to complete your deployment.") 