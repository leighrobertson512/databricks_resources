"""
Data Discovery App - Databricks Custom App
==========================================

A comprehensive data discovery application for Databricks Unity Catalog.
Provides business-friendly interface for exploring catalogs, schemas, tables, and columns.

Based on observability patterns and data discoverability best practices.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
from typing import Dict, List, Any, Optional
import re

# Import our custom modules with error handling
try:
    from data_discovery_utils import DataDiscoveryAPI
    from visualization_utils import create_summary_charts, create_data_lineage_viz
    from search_utils import SmartSearch, TableRecommendations
except ImportError as e:
    st.error(f"Import error: {e}")
    st.info("Make sure all utility files are uploaded to the same directory as app.py")
    st.stop()

# Configure Streamlit page
st.set_page_config(
    page_title="Data Discovery Hub",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f4e79;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #007acc;
        margin: 0.5rem 0;
    }
    .search-box {
        background-color: #ffffff;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 2rem;
    }
    .table-card {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #e0e0e0;
        margin: 0.5rem 0;
    }
    .tag {
        display: inline-block;
        padding: 0.2rem 0.5rem;
        margin: 0.1rem;
        background-color: #e3f2fd;
        color: #1976d2;
        border-radius: 15px;
        font-size: 0.8rem;
    }
</style>
""", unsafe_allow_html=True)

class DataDiscoveryApp:
    def __init__(self):
        """Initialize the Data Discovery App"""
        self.api = DataDiscoveryAPI()
        self.search = SmartSearch()
        self.recommendations = TableRecommendations()
        
    def initialize_session_state(self):
        """Initialize session state variables"""
        if 'selected_catalog' not in st.session_state:
            st.session_state.selected_catalog = None
        if 'selected_schema' not in st.session_state:
            st.session_state.selected_schema = None
        if 'search_results' not in st.session_state:
            st.session_state.search_results = pd.DataFrame()
        if 'last_search' not in st.session_state:
            st.session_state.last_search = ""
            
    def render_header(self):
        """Render the main header"""
        st.markdown('<h1 class="main-header">🔍 Data Discovery Hub</h1>', unsafe_allow_html=True)
        st.markdown("""
        <div style="text-align: center; color: #666; margin-bottom: 2rem;">
            Discover, explore, and understand your data assets across Unity Catalog
        </div>
        """, unsafe_allow_html=True)
        
    def render_sidebar(self):
        """Render the sidebar with navigation and filters"""
        with st.sidebar:
            st.header("🎛️ Navigation")
            
            # Page selection
            page = st.selectbox(
                "Choose a view:",
                ["🏠 Overview", "🔍 Search & Discover", "📊 Catalog Analytics", 
                 "📈 Usage Insights", "🔗 Data Lineage", "⚙️ Settings"]
            )
            
            st.divider()
            
            # Catalog filter
            catalogs = self.api.get_available_catalogs()
            selected_catalog = st.selectbox(
                "Filter by Catalog:",
                ["All Catalogs"] + catalogs,
                key="catalog_filter"
            )
            
            if selected_catalog != "All Catalogs":
                st.session_state.selected_catalog = selected_catalog
                
                # Schema filter
                schemas = self.api.get_schemas(selected_catalog)
                selected_schema = st.selectbox(
                    "Filter by Schema:",
                    ["All Schemas"] + schemas,
                    key="schema_filter"
                )
                
                if selected_schema != "All Schemas":
                    st.session_state.selected_schema = selected_schema
            
            st.divider()
            
            # Quick stats
            st.header("📊 Quick Stats")
            stats = self.api.get_quick_stats()
            
            st.metric("Total Tables", f"{stats.get('total_tables', 0):,}")
            st.metric("Total Columns", f"{stats.get('total_columns', 0):,}")
            st.metric("Active Catalogs", f"{stats.get('active_catalogs', 0):,}")
            
            return page
            
    def render_overview_page(self):
        """Render the overview dashboard page"""
        st.header("📊 Data Landscape Overview")
        
        # Key metrics row
        col1, col2, col3, col4 = st.columns(4)
        stats = self.api.get_detailed_stats()
        
        with col1:
            st.markdown("""
            <div class="metric-card">
                <h3>📚 Catalogs</h3>
                <h2>{}</h2>
                <p>Active data catalogs</p>
            </div>
            """.format(stats.get('catalogs', 0)), unsafe_allow_html=True)
            
        with col2:
            st.markdown("""
            <div class="metric-card">
                <h3>🗂️ Schemas</h3>
                <h2>{}</h2>
                <p>Database schemas</p>
            </div>
            """.format(stats.get('schemas', 0)), unsafe_allow_html=True)
            
        with col3:
            st.markdown("""
            <div class="metric-card">
                <h3>📋 Tables</h3>
                <h2>{}</h2>
                <p>Total tables & views</p>
            </div>
            """.format(stats.get('tables', 0)), unsafe_allow_html=True)
            
        with col4:
            st.markdown("""
            <div class="metric-card">
                <h3>📝 Columns</h3>
                <h2>{}</h2>
                <p>Total columns</p>
            </div>
            """.format(stats.get('columns', 0)), unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Charts row
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 Tables by Catalog")
            catalog_data = self.api.get_tables_by_catalog()
            if not catalog_data.empty:
                fig = px.bar(catalog_data, x='catalog', y='table_count', 
                           title="Table Distribution Across Catalogs")
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("📈 Table Types Distribution")
            type_data = self.api.get_table_types_distribution()
            if not type_data.empty:
                fig = px.pie(type_data, values='count', names='table_type',
                           title="Distribution of Table Types")
                st.plotly_chart(fig, use_container_width=True)
        
        # Recent activity
        st.subheader("🕒 Recent Activity")
        recent_tables = self.api.get_recent_tables(limit=10)
        if not recent_tables.empty:
            st.dataframe(recent_tables, use_container_width=True)
    
    def render_search_page(self):
        """Render the search and discovery page"""
        st.header("🔍 Search & Discover Data Assets")
        
        # Search interface
        st.markdown('<div class="search-box">', unsafe_allow_html=True)
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            search_query = st.text_input(
                "Search for tables, columns, or descriptions:",
                placeholder="e.g., customer data, sales metrics, user behavior...",
                key="main_search"
            )
            
        with col2:
            search_type = st.selectbox(
                "Search Type:",
                ["Smart Search", "Table Names", "Column Names", "Descriptions", "All"]
            )
        
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            if st.button("🔍 Search", type="primary"):
                if search_query:
                    with st.spinner("Searching..."):
                        results = self.search.perform_search(
                            query=search_query,
                            search_type=search_type.lower().replace(" ", "_"),
                            catalog_filter=st.session_state.get('selected_catalog'),
                            schema_filter=st.session_state.get('selected_schema')
                        )
                        st.session_state.search_results = results
                        st.session_state.last_search = search_query
        
        with col2:
            if st.button("🎲 Surprise Me!"):
                random_results = self.api.get_random_interesting_tables(limit=5)
                st.session_state.search_results = random_results
                st.session_state.last_search = "Random Discovery"
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Display search results
        if not st.session_state.search_results.empty:
            st.subheader(f"🎯 Search Results for: '{st.session_state.last_search}'")
            
            results_df = st.session_state.search_results
            
            # Results summary
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Results Found", len(results_df))
            with col2:
                unique_catalogs = results_df['table_catalog'].nunique() if 'table_catalog' in results_df.columns else 0
                st.metric("Catalogs", unique_catalogs)
            with col3:
                unique_schemas = results_df['table_schema'].nunique() if 'table_schema' in results_df.columns else 0
                st.metric("Schemas", unique_schemas)
            
            # Display results
            for idx, row in results_df.iterrows():
                self.render_table_card(row)
        
        # Recommendations
        st.subheader("💡 Recommended for You")
        recommendations = self.recommendations.get_recommendations(
            user_context={"recent_searches": [st.session_state.last_search]},
            limit=5
        )
        
        if not recommendations.empty:
            for idx, rec in recommendations.iterrows():
                with st.expander(f"📊 {rec.get('table_name', 'Unknown Table')} - {rec.get('reason', '')}"):
                    col1, col2 = st.columns([2, 1])
                    with col1:
                        st.write(f"**Catalog:** {rec.get('table_catalog', 'N/A')}")
                        st.write(f"**Schema:** {rec.get('table_schema', 'N/A')}")
                        st.write(f"**Description:** {rec.get('table_comment', 'No description available')}")
                    with col2:
                        if st.button(f"Explore", key=f"explore_{idx}"):
                            self.show_table_details(rec.get('table_catalog'), rec.get('table_schema'), rec.get('table_name'))
    
    def render_table_card(self, table_row):
        """Render a card for a single table"""
        table_name = table_row.get('table_name', 'Unknown')
        catalog = table_row.get('table_catalog', 'N/A')
        schema = table_row.get('table_schema', 'N/A')
        table_type = table_row.get('table_type', 'TABLE')
        owner = table_row.get('table_owner', 'Unknown')
        comment = table_row.get('table_comment', 'No description available')
        
        with st.container():
            st.markdown(f"""
            <div class="table-card">
                <h4>📊 {table_name}</h4>
                <p><strong>Location:</strong> {catalog}.{schema}</p>
                <p><strong>Type:</strong> <span class="tag">{table_type}</span> <strong>Owner:</strong> {owner}</p>
                <p><strong>Description:</strong> {comment}</p>
            </div>
            """, unsafe_allow_html=True)
            
            col1, col2, col3 = st.columns([1, 1, 2])
            with col1:
                if st.button(f"🔍 Explore", key=f"explore_{catalog}_{schema}_{table_name}"):
                    self.show_table_details(catalog, schema, table_name)
            with col2:
                if st.button(f"📊 Preview", key=f"preview_{catalog}_{schema}_{table_name}"):
                    self.show_table_preview(catalog, schema, table_name)
    
    def show_table_details(self, catalog: str, schema: str, table: str):
        """Show detailed information about a table"""
        with st.expander(f"🔍 Detailed View: {catalog}.{schema}.{table}", expanded=True):
            table_info = self.api.get_table_details(catalog, schema, table)
            
            if table_info:
                col1, col2 = st.columns([1, 1])
                
                with col1:
                    st.subheader("📋 Table Information")
                    st.write(f"**Full Name:** {table_info.get('full_name', 'N/A')}")
                    st.write(f"**Type:** {table_info.get('table_type', 'N/A')}")
                    st.write(f"**Owner:** {table_info.get('owner', 'N/A')}")
                    st.write(f"**Created:** {table_info.get('created', 'N/A')}")
                    st.write(f"**Last Modified:** {table_info.get('last_altered', 'N/A')}")
                
                with col2:
                    st.subheader("📊 Schema")
                    columns_df = self.api.get_table_columns(catalog, schema, table)
                    if not columns_df.empty:
                        st.dataframe(columns_df[['column_name', 'data_type', 'comment']], use_container_width=True)
    
    def show_table_preview(self, catalog: str, schema: str, table: str):
        """Show a preview of table data"""
        with st.expander(f"📊 Data Preview: {catalog}.{schema}.{table}", expanded=True):
            preview_data = self.api.get_table_preview(catalog, schema, table, limit=10)
            if not preview_data.empty:
                st.dataframe(preview_data, use_container_width=True)
            else:
                st.warning("Unable to preview table data. Check permissions or table accessibility.")
    
    def render_analytics_page(self):
        """Render catalog analytics page"""
        st.header("📊 Catalog Analytics")
        
        # Data quality metrics
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📈 Data Quality Score")
            quality_score = self.api.calculate_data_quality_score()
            
            fig = go.Figure(go.Indicator(
                mode = "gauge+number+delta",
                value = quality_score,
                domain = {'x': [0, 1], 'y': [0, 1]},
                title = {'text': "Overall Quality Score"},
                delta = {'reference': 80},
                gauge = {'axis': {'range': [None, 100]},
                        'bar': {'color': "darkblue"},
                        'steps': [
                            {'range': [0, 50], 'color': "lightgray"},
                            {'range': [50, 80], 'color': "gray"}],
                        'threshold': {'line': {'color': "red", 'width': 4},
                                    'thickness': 0.75, 'value': 90}}))
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("📊 Documentation Coverage")
            doc_stats = self.api.get_documentation_stats()
            
            fig = px.bar(
                x=['Tables with Comments', 'Columns with Comments', 'Tables without Comments'],
                y=[doc_stats.get('tables_with_comments', 0), 
                   doc_stats.get('columns_with_comments', 0),
                   doc_stats.get('tables_without_comments', 0)],
                title="Documentation Coverage"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Usage patterns
        st.subheader("🔄 Usage Patterns")
        usage_data = self.api.get_usage_patterns()
        
        if not usage_data.empty:
            fig = px.line(usage_data, x='date', y='query_count', 
                         title="Daily Query Volume")
            st.plotly_chart(fig, use_container_width=True)
    
    def run(self):
        """Main app runner"""
        self.initialize_session_state()
        self.render_header()
        
        # Sidebar navigation
        selected_page = self.render_sidebar()
        
        # Main content based on selected page
        if selected_page == "🏠 Overview":
            self.render_overview_page()
        elif selected_page == "🔍 Search & Discover":
            self.render_search_page()
        elif selected_page == "📊 Catalog Analytics":
            self.render_analytics_page()
        elif selected_page == "📈 Usage Insights":
            st.header("📈 Usage Insights")
            st.info("Usage insights coming soon! This will show query patterns, popular tables, and access trends.")
        elif selected_page == "🔗 Data Lineage":
            st.header("🔗 Data Lineage")
            st.info("Data lineage visualization coming soon! This will show data flow and dependencies.")
        elif selected_page == "⚙️ Settings":
            st.header("⚙️ Settings")
            st.info("Settings panel coming soon! Configure your data discovery preferences here.")

def main():
    """Main entry point for the Databricks app"""
    try:
        app = DataDiscoveryApp()
        app.run()
    except Exception as e:
        st.error(f"Application error: {str(e)}")
        st.info("Please check your configuration and try again.")

if __name__ == "__main__":
    main() 