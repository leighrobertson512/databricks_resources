"""
Data Discovery Utilities
========================

Core data access utilities for the Data Discovery App.
Leverages the materialized view created in obs_query_base.py and Unity Catalog APIs.
"""

import pandas as pd
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import numpy as np
import random

class DataDiscoveryAPI:
    """
    Core API for accessing Unity Catalog metadata and table information.
    Uses the materialized view from obs_query_base.py as the primary data source.
    """
    
    def __init__(self):
        """Initialize the Data Discovery API"""
        self.base_view = "leigh_robertson_demo.observability.tables_columns_genie"
        self._cache = {}
        self._cache_timeout = 300  # 5 minutes
        
    def _execute_query(self, query: str) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame"""
        try:
            # Import spark context
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark is None:
                spark = SparkSession.builder.appName("DataDiscoveryApp").getOrCreate()
            
            # Use Databricks SQL execution
            return spark.sql(query).toPandas()
        except Exception as e:
            print(f"Query execution error: {e}")
            return pd.DataFrame()
    
    def _get_cached_or_execute(self, cache_key: str, query: str) -> pd.DataFrame:
        """Get cached result or execute query and cache it"""
        current_time = datetime.now()
        
        if (cache_key in self._cache and 
            current_time - self._cache[cache_key]['timestamp'] < timedelta(seconds=self._cache_timeout)):
            return self._cache[cache_key]['data']
        
        result = self._execute_query(query)
        self._cache[cache_key] = {
            'data': result,
            'timestamp': current_time
        }
        return result
    
    def get_available_catalogs(self) -> List[str]:
        """Get list of available catalogs"""
        query = f"""
        SELECT DISTINCT table_catalog
        FROM {self.base_view}
        WHERE table_catalog IS NOT NULL
        ORDER BY table_catalog
        """
        
        result = self._get_cached_or_execute("catalogs", query)
        return result['table_catalog'].tolist() if not result.empty else []
    
    def get_schemas(self, catalog: str) -> List[str]:
        """Get schemas for a specific catalog"""
        query = f"""
        SELECT DISTINCT table_schema
        FROM {self.base_view}
        WHERE table_catalog = '{catalog}'
        AND table_schema IS NOT NULL
        ORDER BY table_schema
        """
        
        result = self._execute_query(query)
        return result['table_schema'].tolist() if not result.empty else []
    
    def get_quick_stats(self) -> Dict[str, int]:
        """Get quick statistics for the sidebar"""
        query = f"""
        SELECT 
            COUNT(DISTINCT CONCAT(table_catalog, '.', table_schema, '.', table_name)) as total_tables,
            COUNT(DISTINCT column_name) as total_columns,
            COUNT(DISTINCT table_catalog) as active_catalogs
        FROM {self.base_view}
        """
        
        result = self._get_cached_or_execute("quick_stats", query)
        if not result.empty:
            return result.iloc[0].to_dict()
        return {'total_tables': 0, 'total_columns': 0, 'active_catalogs': 0}
    
    def get_detailed_stats(self) -> Dict[str, int]:
        """Get detailed statistics for the overview page"""
        query = f"""
        SELECT 
            COUNT(DISTINCT table_catalog) as catalogs,
            COUNT(DISTINCT CONCAT(table_catalog, '.', table_schema)) as schemas,
            COUNT(DISTINCT CONCAT(table_catalog, '.', table_schema, '.', table_name)) as tables,
            COUNT(DISTINCT CONCAT(table_catalog, '.', table_schema, '.', table_name, '.', column_name)) as columns
        FROM {self.base_view}
        """
        
        result = self._get_cached_or_execute("detailed_stats", query)
        if not result.empty:
            return result.iloc[0].to_dict()
        return {'catalogs': 0, 'schemas': 0, 'tables': 0, 'columns': 0}
    
    def get_tables_by_catalog(self) -> pd.DataFrame:
        """Get table count by catalog for visualization"""
        query = f"""
        SELECT 
            table_catalog as catalog,
            COUNT(DISTINCT CONCAT(table_schema, '.', table_name)) as table_count
        FROM {self.base_view}
        GROUP BY table_catalog
        ORDER BY table_count DESC
        """
        
        return self._execute_query(query)
    
    def get_table_types_distribution(self) -> pd.DataFrame:
        """Get distribution of table types"""
        query = f"""
        SELECT 
            table_type,
            COUNT(DISTINCT CONCAT(table_catalog, '.', table_schema, '.', table_name)) as count
        FROM {self.base_view}
        WHERE table_type IS NOT NULL
        GROUP BY table_type
        ORDER BY count DESC
        """
        
        return self._execute_query(query)
    
    def get_recent_tables(self, limit: int = 10) -> pd.DataFrame:
        """Get recently created or modified tables"""
        query = f"""
        SELECT DISTINCT
            table_catalog,
            table_schema,
            table_name,
            table_type,
            table_owner,
            created,
            last_altered
        FROM {self.base_view}
        WHERE created IS NOT NULL
        ORDER BY COALESCE(last_altered, created) DESC
        LIMIT {limit}
        """
        
        return self._execute_query(query)
    
    def get_table_details(self, catalog: str, schema: str, table: str) -> Dict[str, Any]:
        """Get detailed information about a specific table"""
        query = f"""
        SELECT DISTINCT
            table_catalog,
            table_schema,
            table_name,
            table_type,
            table_owner,
            table_comment,
            created,
            created_by,
            last_altered,
            last_altered_by
        FROM {self.base_view}
        WHERE table_catalog = '{catalog}'
        AND table_schema = '{schema}'
        AND table_name = '{table}'
        """
        
        result = self._execute_query(query)
        if not result.empty:
            row = result.iloc[0]
            return {
                'full_name': f"{catalog}.{schema}.{table}",
                'table_type': row.get('table_type'),
                'owner': row.get('table_owner'),
                'comment': row.get('table_comment'),
                'created': row.get('created'),
                'created_by': row.get('created_by'),
                'last_altered': row.get('last_altered'),
                'last_altered_by': row.get('last_altered_by')
            }
        return {}
    
    def get_table_columns(self, catalog: str, schema: str, table: str) -> pd.DataFrame:
        """Get columns for a specific table"""
        query = f"""
        SELECT 
            column_name,
            data_type,
            column_comment as comment
        FROM {self.base_view}
        WHERE table_catalog = '{catalog}'
        AND table_schema = '{schema}'
        AND table_name = '{table}'
        ORDER BY column_name
        """
        
        return self._execute_query(query)
    
    def get_table_preview(self, catalog: str, schema: str, table: str, limit: int = 10) -> pd.DataFrame:
        """Get a preview of table data"""
        try:
            full_table_name = f"{catalog}.{schema}.{table}"
            query = f"SELECT * FROM {full_table_name} LIMIT {limit}"
            return self._execute_query(query)
        except Exception as e:
            print(f"Unable to preview table {catalog}.{schema}.{table}: {e}")
            return pd.DataFrame()
    
    def get_random_interesting_tables(self, limit: int = 5) -> pd.DataFrame:
        """Get random interesting tables for discovery"""
        query = f"""
        SELECT DISTINCT
            table_catalog,
            table_schema,
            table_name,
            table_type,
            table_owner,
            table_comment,
            created
        FROM {self.base_view}
        WHERE table_comment IS NOT NULL
        AND table_comment != ''
        ORDER BY RAND()
        LIMIT {limit}
        """
        
        return self._execute_query(query)
    
    def search_tables_and_columns(self, search_term: str, catalog_filter: Optional[str] = None, 
                                 schema_filter: Optional[str] = None) -> pd.DataFrame:
        """Search across table names, column names, and descriptions"""
        search_term = search_term.lower()
        
        # Build WHERE clause
        where_conditions = [
            f"(LOWER(table_name) LIKE '%{search_term}%' OR " +
            f"LOWER(column_name) LIKE '%{search_term}%' OR " +
            f"LOWER(COALESCE(table_comment, '')) LIKE '%{search_term}%' OR " +
            f"LOWER(COALESCE(column_comment, '')) LIKE '%{search_term}%')"
        ]
        
        if catalog_filter:
            where_conditions.append(f"table_catalog = '{catalog_filter}'")
        if schema_filter:
            where_conditions.append(f"table_schema = '{schema_filter}'")
        
        where_clause = " AND ".join(where_conditions)
        
        query = f"""
        SELECT DISTINCT
            table_catalog,
            table_schema,
            table_name,
            table_type,
            table_owner,
            table_comment,
            'table_search' as match_type
        FROM {self.base_view}
        WHERE {where_clause}
        ORDER BY table_name
        LIMIT 50
        """
        
        return self._execute_query(query)
    
    def calculate_data_quality_score(self) -> float:
        """Calculate an overall data quality score based on documentation"""
        query = f"""
        SELECT 
            COUNT(DISTINCT CASE WHEN table_comment IS NOT NULL AND table_comment != '' 
                  THEN CONCAT(table_catalog, '.', table_schema, '.', table_name) END) * 100.0 / 
            COUNT(DISTINCT CONCAT(table_catalog, '.', table_schema, '.', table_name)) as table_doc_pct,
            
            COUNT(CASE WHEN column_comment IS NOT NULL AND column_comment != '' THEN 1 END) * 100.0 /
            COUNT(*) as column_doc_pct
        FROM {self.base_view}
        """
        
        result = self._execute_query(query)
        if not result.empty:
            row = result.iloc[0]
            table_score = row.get('table_doc_pct', 0)
            column_score = row.get('column_doc_pct', 0)
            # Weighted average: 60% table documentation, 40% column documentation
            return round((table_score * 0.6 + column_score * 0.4), 1)
        return 0.0
    
    def get_documentation_stats(self) -> Dict[str, int]:
        """Get documentation coverage statistics"""
        query = f"""
        SELECT 
            COUNT(DISTINCT CASE WHEN table_comment IS NOT NULL AND table_comment != '' 
                  THEN CONCAT(table_catalog, '.', table_schema, '.', table_name) END) as tables_with_comments,
            COUNT(DISTINCT CASE WHEN table_comment IS NULL OR table_comment = '' 
                  THEN CONCAT(table_catalog, '.', table_schema, '.', table_name) END) as tables_without_comments,
            COUNT(CASE WHEN column_comment IS NOT NULL AND column_comment != '' THEN 1 END) as columns_with_comments
        FROM {self.base_view}
        """
        
        result = self._execute_query(query)
        if not result.empty:
            return result.iloc[0].to_dict()
        return {'tables_with_comments': 0, 'tables_without_comments': 0, 'columns_with_comments': 0}
    
    def get_usage_patterns(self) -> pd.DataFrame:
        """Get usage patterns (placeholder - would need query history data)"""
        # This is a placeholder since we don't have access to query history in the base view
        # In a real implementation, this would query system.query.history or similar
        dates = pd.date_range(start=datetime.now() - timedelta(days=30), 
                            end=datetime.now(), freq='D')
        
        # Generate sample data for demonstration
        data = []
        for date in dates:
            data.append({
                'date': date.strftime('%Y-%m-%d'),
                'query_count': random.randint(50, 500)
            })
        
        return pd.DataFrame(data)
    
    def get_tables_by_owner(self) -> pd.DataFrame:
        """Get table counts by owner"""
        query = f"""
        SELECT 
            table_owner,
            COUNT(DISTINCT CONCAT(table_catalog, '.', table_schema, '.', table_name)) as table_count
        FROM {self.base_view}
        WHERE table_owner IS NOT NULL
        GROUP BY table_owner
        ORDER BY table_count DESC
        LIMIT 20
        """
        
        return self._execute_query(query)
    
    def get_schema_summary(self, catalog: str, schema: str) -> Dict[str, Any]:
        """Get summary information for a specific schema"""
        query = f"""
        SELECT 
            COUNT(DISTINCT table_name) as table_count,
            COUNT(DISTINCT column_name) as column_count,
            COUNT(DISTINCT table_type) as table_types,
            COUNT(DISTINCT table_owner) as owners
        FROM {self.base_view}
        WHERE table_catalog = '{catalog}'
        AND table_schema = '{schema}'
        """
        
        result = self._execute_query(query)
        if not result.empty:
            return result.iloc[0].to_dict()
        return {'table_count': 0, 'column_count': 0, 'table_types': 0, 'owners': 0} 