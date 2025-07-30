# Databricks notebook source
"""
https://medium.com/@rahulgosavi.94/databricks-unity-catalog-analysis-a-comprehensive-api-driven-approach-dae8c9b121dd
"""

# COMMAND ----------

"""
Databricks Unity Catalog Complete Implementation
===============================================

A comprehensive solution for Databricks catalog analysis with minimal verbose output.
Covers all scenarios: catalog inventory, data access, export, and analysis.

Author: Rahul Gosavi
Date: 2025-06-16
"""

import requests
import pandas as pd
from typing import List, Dict, Any, Optional, Union
import json
from datetime import datetime
import time
import os
import concurrent.futures
from dataclasses import dataclass


@dataclass
class CatalogConfig:
    """Configuration for catalog analysis"""
    workspace_url: str
    access_token: str
    target_catalog: str = None
    max_workers: int = 5
    timeout: int = 30


class DatabricksAPIException(Exception):
    """Custom exception for Databricks API errors"""
    pass


class DatabricksCatalogAPI:
    """
    Comprehensive Databricks Unity Catalog API client
    Optimized for production use with minimal verbose output
    """
    
    def __init__(self, config: CatalogConfig):
        self.config = config
        self.workspace_url = config.workspace_url.rstrip('/')
        self.access_token = config.access_token
        self.headers = {
            'Authorization': f'Bearer {config.access_token}',
            'Content-Type': 'application/json'
        }
        self.base_url = f"{self.workspace_url}/api/2.1/unity-catalog"
        self.sql_api_base = f"{self.workspace_url}/api/2.0/sql"
        self._validate_connection()
    
    def _validate_connection(self) -> None:
        """Validate API connection and permissions"""
        try:
            response = self._make_request("catalogs")
            if not response or 'catalogs' not in response:
                raise DatabricksAPIException("Invalid API response or insufficient permissions")
        except Exception as e:
            raise DatabricksAPIException(f"Connection validation failed: {e}")
    
    def _make_request(self, endpoint: str, params: Dict = None, max_retries: int = 3) -> Optional[Dict]:
        """Make HTTP request with retry logic"""
        url = f"{self.base_url}/{endpoint}"
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.headers, params=params or {}, 
                                      timeout=self.config.timeout)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    raise DatabricksAPIException(f"API request failed after {max_retries} attempts: {e}")
                time.sleep(2 ** attempt)
        return None
    
    def get_catalogs(self) -> pd.DataFrame:
        """Get all accessible catalogs"""
        response = self._make_request("catalogs")
        if not response:
            return pd.DataFrame()
        
        catalogs_data = []
        for catalog in response.get('catalogs', []):
            catalogs_data.append({
                'object_type': 'CATALOG',
                'catalog_name': catalog.get('name'),
                'object_name': catalog.get('name'),
                'full_name': catalog.get('name'),
                'owner': catalog.get('owner'),
                'created_at': catalog.get('created_at'),
                'updated_at': catalog.get('updated_at'),
                'comment': catalog.get('comment', ''),
                'metastore_id': catalog.get('metastore_id'),
                'storage_root': catalog.get('storage_root', '')
            })
        
        return pd.DataFrame(catalogs_data)
    
    def get_schemas(self, catalog_name: str) -> pd.DataFrame:
        """Get all schemas in a catalog"""
        response = self._make_request("schemas", {'catalog_name': catalog_name})
        if not response:
            return pd.DataFrame()
        
        schemas_data = []
        for schema in response.get('schemas', []):
            schemas_data.append({
                'object_type': 'SCHEMA',
                'catalog_name': catalog_name,
                'schema_name': schema.get('name'),
                'object_name': schema.get('name'),
                'full_name': schema.get('full_name'),
                'owner': schema.get('owner'),
                'created_at': schema.get('created_at'),
                'updated_at': schema.get('updated_at'),
                'comment': schema.get('comment', ''),
                'storage_root': schema.get('storage_root', '')
            })
        
        return pd.DataFrame(schemas_data)
    
    def get_tables(self, catalog_name: str, schema_name: str = None) -> pd.DataFrame:
        """Get all tables and views in a catalog or specific schema"""
        if schema_name:
            schemas_to_process = [schema_name]
        else:
            schemas_df = self.get_schemas(catalog_name)
            schemas_to_process = schemas_df['schema_name'].tolist() if len(schemas_df) > 0 else []
        
        all_tables_data = []
        
        for schema in schemas_to_process:
            try:
                response = self._make_request("tables", {
                    'catalog_name': catalog_name,
                    'schema_name': schema
                })
                
                if response:
                    for table in response.get('tables', []):
                        all_tables_data.append({
                            'object_type': table.get('table_type', 'TABLE'),
                            'catalog_name': catalog_name,
                            'schema_name': schema,
                            'object_name': table.get('name'),
                            'full_name': table.get('full_name'),
                            'owner': table.get('owner'),
                            'created_at': table.get('created_at'),
                            'updated_at': table.get('updated_at'),
                            'comment': table.get('comment', ''),
                            'data_source_format': table.get('data_source_format', ''),
                            'storage_location': table.get('storage_location', '')
                        })
            except Exception:
                continue  # Skip schemas with access issues
        
        return pd.DataFrame(all_tables_data)
    
    def get_functions(self, catalog_name: str, schema_name: str = None) -> pd.DataFrame:
        """Get all functions in a catalog or specific schema"""
        if schema_name:
            schemas_to_process = [schema_name]
        else:
            schemas_df = self.get_schemas(catalog_name)
            schemas_to_process = schemas_df['schema_name'].tolist() if len(schemas_df) > 0 else []
        
        all_functions_data = []
        
        for schema in schemas_to_process:
            try:
                response = self._make_request("functions", {
                    'catalog_name': catalog_name,
                    'schema_name': schema
                })
                
                if response:
                    for function in response.get('functions', []):
                        all_functions_data.append({
                            'object_type': 'FUNCTION',
                            'catalog_name': catalog_name,
                            'schema_name': schema,
                            'object_name': function.get('name'),
                            'full_name': function.get('full_name'),
                            'owner': function.get('owner'),
                            'created_at': function.get('created_at'),
                            'updated_at': function.get('updated_at'),
                            'comment': function.get('comment', ''),
                            'routine_body': function.get('routine_body', ''),
                            'routine_definition': function.get('routine_definition', '')
                        })
            except Exception:
                continue
        
        return pd.DataFrame(all_functions_data)
    
    def get_volumes(self, catalog_name: str, schema_name: str = None) -> pd.DataFrame:
        """Get all volumes in a catalog or specific schema"""
        if schema_name:
            schemas_to_process = [schema_name]
        else:
            schemas_df = self.get_schemas(catalog_name)
            schemas_to_process = schemas_df['schema_name'].tolist() if len(schemas_df) > 0 else []
        
        all_volumes_data = []
        
        for schema in schemas_to_process:
            try:
                response = self._make_request("volumes", {
                    'catalog_name': catalog_name,
                    'schema_name': schema
                })
                
                if response:
                    for volume in response.get('volumes', []):
                        all_volumes_data.append({
                            'object_type': f"VOLUME_{volume.get('volume_type', 'MANAGED')}",
                            'catalog_name': catalog_name,
                            'schema_name': schema,
                            'object_name': volume.get('name'),
                            'full_name': volume.get('full_name'),
                            'owner': volume.get('owner'),
                            'created_at': volume.get('created_at'),
                            'updated_at': volume.get('updated_at'),
                            'comment': volume.get('comment', ''),
                            'volume_type': volume.get('volume_type', ''),
                            'storage_location': volume.get('storage_location', '')
                        })
            except Exception:
                continue
        
        return pd.DataFrame(all_volumes_data)
    
    def get_complete_catalog_inventory(self, catalog_name: str) -> pd.DataFrame:
        """Get complete inventory of all objects in a catalog"""
        # Validate catalog exists
        catalogs_df = self.get_catalogs()
        if catalog_name not in catalogs_df['catalog_name'].values:
            raise DatabricksAPIException(f"Catalog '{catalog_name}' not found or not accessible")
        
        # Get all object types
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {
                'schemas': executor.submit(self.get_schemas, catalog_name),
                'tables': executor.submit(self.get_tables, catalog_name),
                'functions': executor.submit(self.get_functions, catalog_name),
                'volumes': executor.submit(self.get_volumes, catalog_name)
            }
            
            results = {}
            for object_type, future in futures.items():
                try:
                    results[object_type] = future.result()
                except Exception:
                    results[object_type] = pd.DataFrame()
        
        # Combine all DataFrames
        all_dfs = [df for df in results.values() if len(df) > 0]
        
        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            combined_df['analysis_timestamp'] = datetime.now().isoformat()
            return combined_df
        else:
            return pd.DataFrame()


class DatabricksDataAPI:
    """
    SQL Execution API client for data access
    """
    
    def __init__(self, config: CatalogConfig):
        self.config = config
        self.workspace_url = config.workspace_url.rstrip('/')
        self.access_token = config.access_token
        self.headers = {
            'Authorization': f'Bearer {config.access_token}',
            'Content-Type': 'application/json'
        }
        self.sql_api_base = f"{self.workspace_url}/api/2.0/sql"
        self._warehouse_id = None
    
    def _get_warehouses(self) -> List[Dict]:
        """Get available SQL warehouses"""
        try:
            response = requests.get(f"{self.sql_api_base}/warehouses", headers=self.headers)
            response.raise_for_status()
            return response.json().get('warehouses', [])
        except Exception:
            return []
    
    def _get_warehouse_id(self) -> Optional[str]:
        """Get first available warehouse ID"""
        if not self._warehouse_id:
            warehouses = self._get_warehouses()
            if warehouses:
                self._warehouse_id = warehouses[0]['id']
        return self._warehouse_id
    
    def execute_query(self, query: str, warehouse_id: str = None) -> Optional[pd.DataFrame]:
        """Execute SQL query and return DataFrame"""
        if not warehouse_id:
            warehouse_id = self._get_warehouse_id()
            if not warehouse_id:
                return None
        
        payload = {
            "statement": query,
            "warehouse_id": warehouse_id,
            "format": "JSON_ARRAY",
            "wait_timeout": f"{self.config.timeout}s"
        }
        
        try:
            response = requests.post(f"{self.sql_api_base}/statements", 
                                   json=payload, headers=self.headers)
            response.raise_for_status()
            result = response.json()
            
            if result.get('status', {}).get('state') == 'SUCCEEDED':
                data = result.get('result', {}).get('data_array', [])
                if data:
                    return pd.DataFrame(data)
            return None
        except Exception:
            return None
    
    def get_table_info(self, table_full_name: str) -> Dict[str, Any]:
        """Get comprehensive table information"""
        info = {'table_name': table_full_name}
        
        # Get schema
        schema_df = self.execute_query(f"DESCRIBE {table_full_name}")
        if schema_df is not None:
            schema_df.columns = ['column_name', 'data_type', 'comment']
            info['schema'] = schema_df
            info['column_count'] = len(schema_df)
            info['columns'] = schema_df['column_name'].tolist()
        
        # Get row count
        count_df = self.execute_query(f"SELECT COUNT(*) as row_count FROM {table_full_name}")
        if count_df is not None:
            info['row_count'] = count_df.iloc[0, 0]
        
        # Get sample data
        sample_df = self.execute_query(f"SELECT * FROM {table_full_name} LIMIT 20")
        if sample_df is not None and 'columns' in info:
            sample_df.columns = info['columns']
            info['sample_data'] = sample_df
            info['sample_size'] = len(sample_df)
        
        return info


class CatalogAnalyzer:
    """
    High-level analyzer that combines catalog and data APIs
    """
    
    def __init__(self, config: CatalogConfig):
        self.config = config
        self.catalog_api = DatabricksCatalogAPI(config)
        self.data_api = DatabricksDataAPI(config)
    
    def analyze_catalog(self, catalog_name: str) -> Dict[str, Any]:
        """Perform comprehensive catalog analysis"""
        print(f"Analyzing catalog: {catalog_name}")
        
        # Get complete inventory
        inventory_df = self.catalog_api.get_complete_catalog_inventory(catalog_name)
        
        if len(inventory_df) == 0:
            return {'error': f"No objects found in catalog '{catalog_name}'"}
        
        # Generate analysis summary
        analysis = {
            'catalog_name': catalog_name,
            'analysis_date': datetime.now().isoformat(),
            'total_objects': len(inventory_df),
            'inventory': inventory_df
        }
        
        # Object type breakdown
        type_counts = inventory_df['object_type'].value_counts()
        analysis['object_breakdown'] = type_counts.to_dict()
        
        # Schema breakdown
        schema_counts = inventory_df['schema_name'].value_counts()
        analysis['schema_breakdown'] = schema_counts.to_dict()
        
        # Owner analysis
        owner_counts = inventory_df['owner'].value_counts()
        analysis['owner_breakdown'] = owner_counts.head(10).to_dict()
        
        # Tables analysis
        tables_df = inventory_df[inventory_df['object_type'].str.contains('TABLE|MANAGED|EXTERNAL')]
        if len(tables_df) > 0:
            analysis['table_count'] = len(tables_df)
            analysis['tables_by_schema'] = tables_df['schema_name'].value_counts().to_dict()
        
        return analysis
    
    def analyze_table(self, table_full_name: str) -> Dict[str, Any]:
        """Analyze specific table"""
        print(f"Analyzing table: {table_full_name}")
        return self.data_api.get_table_info(table_full_name)
    
    def display_catalog_summary(self, analysis: Dict[str, Any]) -> None:
        """Display catalog analysis summary"""
        if 'error' in analysis:
            print(f"❌ {analysis['error']}")
            return
        
        print(f"\n📊 Catalog Analysis Summary")
        print(f"Catalog: {analysis['catalog_name']}")
        print(f"Total Objects: {analysis['total_objects']:,}")
        print(f"Analysis Date: {analysis['analysis_date']}")
        
        print(f"\n📈 Object Breakdown:")
        for obj_type, count in analysis['object_breakdown'].items():
            print(f"  {obj_type}: {count:,}")
        
        print(f"\n📂 Schema Breakdown:")
        for schema, count in analysis['schema_breakdown'].items():
            print(f"  {schema}: {count:,} objects")
        
        print(f"\n👥 Top 5 Owners:")
        for owner, count in list(analysis['owner_breakdown'].items())[:5]:
            print(f"  {owner}: {count:,} objects")
    
    def display_table_info(self, table_info: Dict[str, Any]) -> None:
        """Display table information"""
        print(f"\n📋 Table: {table_info['table_name']}")
        
        if 'row_count' in table_info:
            print(f"Rows: {table_info['row_count']:,}")
        
        if 'column_count' in table_info:
            print(f"Columns: {table_info['column_count']}")
        
        # Display schema
        if 'schema' in table_info:
            print(f"\n📊 Schema:")
            print(table_info['schema'].to_string(index=False))
        
        # Display sample data
        if 'sample_data' in table_info:
            print(f"\n📄 Sample Data ({table_info['sample_size']} rows):")
            print(table_info['sample_data'].to_string(index=False, max_rows=10))
    
    def export_analysis(self, analysis: Dict[str, Any], export_formats: List[str] = None) -> Dict[str, str]:
        """Export analysis results"""
        if 'error' in analysis or 'inventory' not in analysis:
            return {'error': 'No valid analysis data to export'}
        
        export_formats = export_formats or ['csv', 'excel']
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        catalog_name = analysis['catalog_name']
        results = {}
        
        try:
            inventory_df = analysis['inventory']
            
            if 'csv' in export_formats:
                csv_filename = f"{catalog_name}_analysis_{timestamp}.csv"
                inventory_df.to_csv(csv_filename, index=False)
                results['csv'] = csv_filename
            
            if 'excel' in export_formats:
                excel_filename = f"{catalog_name}_analysis_{timestamp}.xlsx"
                with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
                    # All objects
                    inventory_df.to_excel(writer, sheet_name='All_Objects', index=False)
                    
                    # By object type
                    for obj_type in inventory_df['object_type'].unique():
                        safe_name = obj_type.replace('_', '')[:31]
                        type_df = inventory_df[inventory_df['object_type'] == obj_type]
                        type_df.to_excel(writer, sheet_name=safe_name, index=False)
                
                results['excel'] = excel_filename
            
            if 'json' in export_formats:
                json_filename = f"{catalog_name}_analysis_{timestamp}.json"
                export_data = {
                    'metadata': {
                        'catalog_name': analysis['catalog_name'],
                        'analysis_date': analysis['analysis_date'],
                        'total_objects': analysis['total_objects'],
                        'object_breakdown': analysis['object_breakdown'],
                        'schema_breakdown': analysis['schema_breakdown']
                    },
                    'inventory': inventory_df.to_dict('records')
                }
                
                with open(json_filename, 'w') as f:
                    json.dump(export_data, f, indent=2, default=str)
                
                results['json'] = json_filename
            
            return results
            
        except Exception as e:
            return {'error': f"Export failed: {str(e)}"}


def main():
    """
    Main execution function demonstrating all capabilities
    """
    # Configuration
    config = CatalogConfig(
        workspace_url=dbutils.secrets.get(scope="leigh_robertson_secrets", key="databricks_aws_e2_url"),
        access_token=dbutils.secrets.get(scope="leigh_robertson_secrets", key="databricks_cli_value"),
        target_catalog="leigh_robertson_demo"
    )
    
    try:
        # Initialize analyzer
        analyzer = CatalogAnalyzer(config)
        print("✅ Databricks catalog analyzer initialized")
        
        # Scenario 1: Complete catalog analysis
        print(f"\n{'='*50}")
        print("SCENARIO 1: Complete Catalog Analysis")
        print(f"{'='*50}")
        
        catalog_analysis = analyzer.analyze_catalog(config.target_catalog)
        analyzer.display_catalog_summary(catalog_analysis)
        
        # Display inventory sample
        if 'inventory' in catalog_analysis:
            print(f"\n📋 Inventory Sample (first 10 objects):")
            sample_columns = ['object_type', 'schema_name', 'object_name', 'owner', 'created_at']
            print(catalog_analysis['inventory'][sample_columns].head(10).to_string(index=False))
        
        # Scenario 2: Table-specific analysis
        print(f"\n{'='*50}")
        print("SCENARIO 2: Table Analysis")
        print(f"{'='*50}")
        
        target_table = "sample_inct_catalog.bronze.airport_master"
        table_info = analyzer.analyze_table(target_table)
        analyzer.display_table_info(table_info)
        
        # Scenario 3: Export results
        print(f"\n{'='*50}")
        print("SCENARIO 3: Export Analysis")
        print(f"{'='*50}")
        
        export_results = analyzer.export_analysis(catalog_analysis, ['csv', 'excel', 'json'])
        
        if 'error' not in export_results:
            print("✅ Export completed:")
            for format_type, filename in export_results.items():
                print(f"  📁 {format_type.upper()}: {filename}")
        else:
            print(f"❌ Export failed: {export_results['error']}")
        
        # Scenario 4: Multiple table analysis
        print(f"\n{'='*50}")
        print("SCENARIO 4: Multiple Table Analysis")
        print(f"{'='*50}")
        
        if 'inventory' in catalog_analysis:
            # Get bronze schema tables
            inventory_df = catalog_analysis['inventory']
            bronze_tables = inventory_df[
                (inventory_df['schema_name'] == 'bronze') & 
                (inventory_df['object_type'].str.contains('TABLE|MANAGED|EXTERNAL'))
            ]
            
            if len(bronze_tables) > 0:
                print(f"Found {len(bronze_tables)} tables in bronze schema")
                print("Analyzing first 3 tables:")
                
                for _, table_row in bronze_tables.head(3).iterrows():
                    table_name = table_row['full_name']
                    table_info = analyzer.analyze_table(table_name)
                    
                    print(f"\n📊 {table_name}:")
                    if 'row_count' in table_info:
                        print(f"  Rows: {table_info['row_count']:,}")
                    if 'column_count' in table_info:
                        print(f"  Columns: {table_info['column_count']}")
            else:
                print("No tables found in bronze schema")
        
        print(f"\n🎉 Analysis completed successfully!")
        print(f"📅 Completion time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except DatabricksAPIException as e:
        print(f"❌ Databricks API Error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


if __name__ == "__main__":
    main()

# COMMAND ----------

# The code above outputs the CSV file to the current working directory of the Databricks cluster.
# The filename is generated as: f"{catalog_name}_analysis_{timestamp}.csv"
# Example: "leigh_robertson_demo_analysis_20250729_153045.csv"
# You can check the working directory with:
import os
os.getcwd()

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * 
# MAGIC FROM sample_inct_catalog.bronze.airport_master

# COMMAND ----------

df = spark.read.csv('/Workspace/Users/leigh.robertson@databricks.com/observability/leigh_robertson_demo_analysis_20250729_204403.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# Ensure the file path is correct and the file exists
file_path = "dbfs:/Workspace/Users/leigh.robertson@databricks.com/observability/leigh_robertson_demo_analysis_20250729_204403.csv"

# Read the CSV file into a DataFrame
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Display the DataFrame
display(df)

# COMMAND ----------


