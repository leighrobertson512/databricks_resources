"""
Search Utilities
================

Smart search and recommendation engine for the Data Discovery App.
Provides intelligent search capabilities and table recommendations.
"""

import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
import re
from datetime import datetime
import numpy as np
from collections import Counter

class SmartSearch:
    """
    Intelligent search engine for data discovery.
    Provides fuzzy matching, semantic search, and relevance scoring.
    """
    
    def __init__(self):
        """Initialize the SmartSearch engine"""
        self.base_view = "leigh_robertson_demo.observability.tables_columns_genie"
        self.search_cache = {}
        
    def _execute_query(self, query: str) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame"""
        try:
            return spark.sql(query).toPandas()
        except Exception as e:
            print(f"Search query execution error: {e}")
            return pd.DataFrame()
    
    def _normalize_search_term(self, term: str) -> str:
        """Normalize search terms for better matching"""
        # Remove special characters, convert to lowercase
        term = re.sub(r'[^\w\s]', ' ', term.lower())
        # Remove extra whitespace
        term = ' '.join(term.split())
        return term
    
    def _calculate_relevance_score(self, row: pd.Series, search_terms: List[str]) -> float:
        """Calculate relevance score for a search result"""
        score = 0.0
        
        # Text fields to search with their weights
        text_fields = {
            'table_name': 3.0,
            'column_name': 2.0,
            'table_comment': 1.5,
            'column_comment': 1.0,
            'table_schema': 0.5
        }
        
        for field, weight in text_fields.items():
            field_value = str(row.get(field, '')).lower()
            if field_value:
                for term in search_terms:
                    if term in field_value:
                        # Exact match gets full weight
                        if term == field_value:
                            score += weight * 2
                        # Word boundary match gets high weight
                        elif re.search(r'\b' + re.escape(term) + r'\b', field_value):
                            score += weight * 1.5
                        # Partial match gets normal weight
                        else:
                            score += weight
        
        return score
    
    def _get_search_suggestions(self, term: str) -> List[str]:
        """Get search suggestions based on common patterns"""
        suggestions = []
        
        # Common data domain keywords
        data_domains = {
            'customer': ['user', 'client', 'account', 'profile'],
            'sales': ['revenue', 'order', 'transaction', 'purchase'],
            'product': ['item', 'catalog', 'inventory', 'goods'],
            'financial': ['payment', 'billing', 'invoice', 'cost'],
            'analytics': ['metric', 'kpi', 'measurement', 'stat'],
            'log': ['event', 'audit', 'tracking', 'history']
        }
        
        term_lower = term.lower()
        for domain, related_terms in data_domains.items():
            if term_lower in domain or any(t in term_lower for t in related_terms):
                suggestions.extend([domain] + related_terms)
        
        return list(set(suggestions))[:5]  # Return top 5 unique suggestions
    
    def perform_search(self, query: str, search_type: str = "smart_search", 
                      catalog_filter: Optional[str] = None, 
                      schema_filter: Optional[str] = None) -> pd.DataFrame:
        """
        Perform intelligent search across tables and columns
        
        Args:
            query: Search query string
            search_type: Type of search (smart_search, table_names, column_names, descriptions, all)
            catalog_filter: Optional catalog filter
            schema_filter: Optional schema filter
        """
        if not query.strip():
            return pd.DataFrame()
        
        normalized_query = self._normalize_search_term(query)
        search_terms = normalized_query.split()
        
        # Build the base WHERE clause based on search type
        search_conditions = []
        
        if search_type == "table_names":
            search_conditions.append(f"LOWER(table_name) LIKE '%{normalized_query}%'")
        elif search_type == "column_names":
            search_conditions.append(f"LOWER(column_name) LIKE '%{normalized_query}%'")
        elif search_type == "descriptions":
            search_conditions.append(f"(LOWER(COALESCE(table_comment, '')) LIKE '%{normalized_query}%' OR " +
                                   f"LOWER(COALESCE(column_comment, '')) LIKE '%{normalized_query}%')")
        else:  # smart_search or all
            # Multi-field search with OR conditions
            or_conditions = []
            for term in search_terms:
                or_conditions.append(f"(LOWER(table_name) LIKE '%{term}%' OR " +
                                   f"LOWER(column_name) LIKE '%{term}%' OR " +
                                   f"LOWER(COALESCE(table_comment, '')) LIKE '%{term}%' OR " +
                                   f"LOWER(COALESCE(column_comment, '')) LIKE '%{term}%')")
            
            if or_conditions:
                search_conditions.append("(" + " OR ".join(or_conditions) + ")")
        
        # Add filters
        if catalog_filter and catalog_filter != "All Catalogs":
            search_conditions.append(f"table_catalog = '{catalog_filter}'")
        if schema_filter and schema_filter != "All Schemas":
            search_conditions.append(f"table_schema = '{schema_filter}'")
        
        if not search_conditions:
            return pd.DataFrame()
        
        where_clause = " AND ".join(search_conditions)
        
        # Execute search query
        sql_query = f"""
        SELECT DISTINCT
            table_catalog,
            table_schema,
            table_name,
            table_type,
            table_owner,
            table_comment,
            column_name,
            data_type,
            column_comment,
            created,
            last_altered
        FROM {self.base_view}
        WHERE {where_clause}
        ORDER BY table_name, column_name
        LIMIT 100
        """
        
        results = self._execute_query(sql_query)
        
        if results.empty:
            return pd.DataFrame()
        
        # Calculate relevance scores and add to results
        if search_type == "smart_search":
            results['relevance_score'] = results.apply(
                lambda row: self._calculate_relevance_score(row, search_terms), axis=1
            )
            # Sort by relevance score
            results = results.sort_values(['relevance_score', 'table_name'], ascending=[False, True])
        
        # Group by table for cleaner presentation
        table_results = []
        for table_key, group in results.groupby(['table_catalog', 'table_schema', 'table_name']):
            table_row = group.iloc[0].copy()
            
            # Add matched columns info
            matched_columns = group['column_name'].tolist()
            table_row['matched_columns'] = ', '.join(matched_columns[:5])  # Show first 5 matches
            table_row['total_matched_columns'] = len(matched_columns)
            
            table_results.append(table_row)
        
        return pd.DataFrame(table_results)
    
    def get_search_suggestions(self, partial_query: str) -> List[str]:
        """Get search suggestions based on partial query"""
        if len(partial_query) < 2:
            return []
        
        suggestions = self._get_search_suggestions(partial_query)
        
        # Add suggestions from actual table/column names
        query = f"""
        SELECT DISTINCT table_name as suggestion, 'table' as type
        FROM {self.base_view}
        WHERE LOWER(table_name) LIKE '%{partial_query.lower()}%'
        UNION ALL
        SELECT DISTINCT column_name as suggestion, 'column' as type
        FROM {self.base_view}
        WHERE LOWER(column_name) LIKE '%{partial_query.lower()}%'
        ORDER BY suggestion
        LIMIT 10
        """
        
        db_suggestions = self._execute_query(query)
        if not db_suggestions.empty:
            suggestions.extend(db_suggestions['suggestion'].tolist())
        
        return list(set(suggestions))[:10]  # Return top 10 unique suggestions


class TableRecommendations:
    """
    Recommendation engine for suggesting relevant tables to users.
    Uses collaborative filtering and content-based recommendations.
    """
    
    def __init__(self):
        """Initialize the recommendation engine"""
        self.base_view = "leigh_robertson_demo.observability.tables_columns_genie"
        
    def _execute_query(self, query: str) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame"""
        try:
            return spark.sql(query).toPandas()
        except Exception as e:
            print(f"Recommendation query execution error: {e}")
            return pd.DataFrame()
    
    def get_recommendations(self, user_context: Dict[str, Any], limit: int = 5) -> pd.DataFrame:
        """
        Get table recommendations based on user context
        
        Args:
            user_context: Dictionary containing user context (recent_searches, etc.)
            limit: Maximum number of recommendations to return
        """
        recommendations = []
        
        # Get recent searches for context
        recent_searches = user_context.get('recent_searches', [])
        
        if recent_searches:
            # Content-based recommendations based on recent searches
            search_based_recs = self._get_search_based_recommendations(recent_searches, limit)
            recommendations.extend(search_based_recs)
        
        # Popular tables recommendation
        popular_recs = self._get_popular_tables(limit)
        recommendations.extend(popular_recs)
        
        # Well-documented tables recommendation
        documented_recs = self._get_well_documented_tables(limit)
        recommendations.extend(documented_recs)
        
        # Recently updated tables
        recent_recs = self._get_recently_updated_tables(limit)
        recommendations.extend(recent_recs)
        
        # Remove duplicates and limit results
        seen_tables = set()
        unique_recommendations = []
        
        for rec in recommendations:
            table_key = f"{rec['table_catalog']}.{rec['table_schema']}.{rec['table_name']}"
            if table_key not in seen_tables:
                seen_tables.add(table_key)
                unique_recommendations.append(rec)
                
                if len(unique_recommendations) >= limit:
                    break
        
        return pd.DataFrame(unique_recommendations)
    
    def _get_search_based_recommendations(self, recent_searches: List[str], limit: int) -> List[Dict]:
        """Get recommendations based on recent searches"""
        if not recent_searches:
            return []
        
        # Extract keywords from recent searches
        keywords = []
        for search in recent_searches:
            if search and search != "Random Discovery":
                keywords.extend(search.lower().split())
        
        if not keywords:
            return []
        
        # Find tables related to search keywords
        keyword_conditions = []
        for keyword in keywords[:5]:  # Use top 5 keywords
            keyword_conditions.append(f"(LOWER(table_name) LIKE '%{keyword}%' OR " +
                                    f"LOWER(COALESCE(table_comment, '')) LIKE '%{keyword}%' OR " +
                                    f"LOWER(column_name) LIKE '%{keyword}%')")
        
        if not keyword_conditions:
            return []
        
        query = f"""
        SELECT DISTINCT
            table_catalog,
            table_schema,
            table_name,
            table_type,
            table_owner,
            table_comment,
            'Based on your recent searches' as reason
        FROM {self.base_view}
        WHERE ({' OR '.join(keyword_conditions)})
        AND table_comment IS NOT NULL
        AND table_comment != ''
        ORDER BY table_name
        LIMIT {limit}
        """
        
        result = self._execute_query(query)
        return result.to_dict('records') if not result.empty else []
    
    def _get_popular_tables(self, limit: int) -> List[Dict]:
        """Get popular tables based on number of columns (proxy for complexity/importance)"""
        query = f"""
        SELECT 
            table_catalog,
            table_schema,
            table_name,
            table_type,
            table_owner,
            table_comment,
            COUNT(DISTINCT column_name) as column_count,
            'Popular table with rich schema' as reason
        FROM {self.base_view}
        WHERE table_comment IS NOT NULL
        AND table_comment != ''
        GROUP BY table_catalog, table_schema, table_name, table_type, table_owner, table_comment
        HAVING COUNT(DISTINCT column_name) >= 5
        ORDER BY column_count DESC
        LIMIT {limit}
        """
        
        result = self._execute_query(query)
        return result.to_dict('records') if not result.empty else []
    
    def _get_well_documented_tables(self, limit: int) -> List[Dict]:
        """Get tables with good documentation"""
        query = f"""
        SELECT DISTINCT
            table_catalog,
            table_schema,
            table_name,
            table_type,
            table_owner,
            table_comment,
            'Well-documented and described' as reason
        FROM {self.base_view}
        WHERE table_comment IS NOT NULL
        AND LENGTH(table_comment) > 50
        AND table_comment NOT LIKE '%test%'
        AND table_comment NOT LIKE '%temp%'
        ORDER BY LENGTH(table_comment) DESC
        LIMIT {limit}
        """
        
        result = self._execute_query(query)
        return result.to_dict('records') if not result.empty else []
    
    def _get_recently_updated_tables(self, limit: int) -> List[Dict]:
        """Get recently updated tables"""
        query = f"""
        SELECT DISTINCT
            table_catalog,
            table_schema,
            table_name,
            table_type,
            table_owner,
            table_comment,
            last_altered,
            'Recently updated content' as reason
        FROM {self.base_view}
        WHERE last_altered IS NOT NULL
        AND table_comment IS NOT NULL
        AND table_comment != ''
        ORDER BY last_altered DESC
        LIMIT {limit}
        """
        
        result = self._execute_query(query)
        return result.to_dict('records') if not result.empty else []
    
    def get_similar_tables(self, catalog: str, schema: str, table: str, limit: int = 5) -> pd.DataFrame:
        """Find tables similar to the given table"""
        # Get columns of the source table
        columns_query = f"""
        SELECT DISTINCT column_name, data_type
        FROM {self.base_view}
        WHERE table_catalog = '{catalog}'
        AND table_schema = '{schema}'
        AND table_name = '{table}'
        """
        
        source_columns = self._execute_query(columns_query)
        if source_columns.empty:
            return pd.DataFrame()
        
        source_column_names = set(source_columns['column_name'].str.lower())
        
        # Find tables with similar column names
        similarity_query = f"""
        SELECT 
            table_catalog,
            table_schema,
            table_name,
            table_type,
            table_owner,
            table_comment,
            COUNT(DISTINCT column_name) as total_columns
        FROM {self.base_view}
        WHERE NOT (table_catalog = '{catalog}' AND table_schema = '{schema}' AND table_name = '{table}')
        GROUP BY table_catalog, table_schema, table_name, table_type, table_owner, table_comment
        ORDER BY total_columns DESC
        LIMIT 20
        """
        
        candidates = self._execute_query(similarity_query)
        if candidates.empty:
            return pd.DataFrame()
        
        # Calculate similarity scores
        similar_tables = []
        
        for _, candidate in candidates.iterrows():
            # Get columns for this candidate
            candidate_columns_query = f"""
            SELECT DISTINCT column_name
            FROM {self.base_view}
            WHERE table_catalog = '{candidate['table_catalog']}'
            AND table_schema = '{candidate['table_schema']}'
            AND table_name = '{candidate['table_name']}'
            """
            
            candidate_columns = self._execute_query(candidate_columns_query)
            if not candidate_columns.empty:
                candidate_column_names = set(candidate_columns['column_name'].str.lower())
                
                # Calculate Jaccard similarity
                intersection = len(source_column_names.intersection(candidate_column_names))
                union = len(source_column_names.union(candidate_column_names))
                similarity = intersection / union if union > 0 else 0
                
                if similarity > 0.1:  # At least 10% similarity
                    candidate_dict = candidate.to_dict()
                    candidate_dict['similarity_score'] = similarity
                    candidate_dict['common_columns'] = intersection
                    similar_tables.append(candidate_dict)
        
        # Sort by similarity and return top results
        similar_df = pd.DataFrame(similar_tables)
        if not similar_df.empty:
            similar_df = similar_df.sort_values('similarity_score', ascending=False).head(limit)
        
        return similar_df 