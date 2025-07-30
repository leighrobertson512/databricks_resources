"""
Configuration settings for Data Discovery Hub
===========================================

Centralized configuration for the Databricks data discovery app.
Modify these settings to customize the app for your environment.
"""

# Data Source Configuration
MATERIALIZED_VIEW = "leigh_robertson_demo.observability.tables_columns_genie"

# App Configuration
APP_TITLE = "Data Discovery Hub"
APP_ICON = "🔍"
APP_DESCRIPTION = "Discover, explore, and understand your data assets across Unity Catalog"

# Search Configuration
SEARCH_RESULT_LIMIT = 50
SEARCH_CACHE_TIMEOUT = 300  # 5 minutes in seconds
DEFAULT_SEARCH_TYPE = "smart_search"

# Visualization Configuration
CHART_COLOR_PALETTE = [
    '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
    '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'
]

CHART_THEME = {
    'layout': {
        'font': {'family': 'Arial, sans-serif', 'size': 12},
        'plot_bgcolor': 'rgba(0,0,0,0)',
        'paper_bgcolor': 'rgba(0,0,0,0)',
        'margin': dict(l=20, r=20, t=40, b=20)
    }
}

# Recommendation Configuration
RECOMMENDATION_LIMIT = 5
POPULAR_TABLE_MIN_COLUMNS = 5
WELL_DOCUMENTED_MIN_LENGTH = 50

# UI Configuration
SIDEBAR_WIDTH = 300
MAIN_CONTENT_WIDTH = 800
ITEMS_PER_PAGE = 10

# Performance Configuration
CACHE_TIMEOUT = 300  # 5 minutes
MAX_WORKERS = 5
QUERY_TIMEOUT = 30

# Feature Flags
ENABLE_DATA_PREVIEW = True
ENABLE_RECOMMENDATIONS = True
ENABLE_ANALYTICS_DASHBOARD = True
ENABLE_USAGE_INSIGHTS = False  # Placeholder for future feature
ENABLE_DATA_LINEAGE = False    # Placeholder for future feature

# Navigation Menu
NAVIGATION_PAGES = [
    "🏠 Overview",
    "🔍 Search & Discover", 
    "📊 Catalog Analytics",
    "📈 Usage Insights",
    "🔗 Data Lineage",
    "⚙️ Settings"
]

# Search Suggestions - Common data domain keywords
DATA_DOMAIN_KEYWORDS = {
    'customer': ['user', 'client', 'account', 'profile', 'person'],
    'sales': ['revenue', 'order', 'transaction', 'purchase', 'invoice'],
    'product': ['item', 'catalog', 'inventory', 'goods', 'merchandise'],
    'financial': ['payment', 'billing', 'invoice', 'cost', 'expense'],
    'analytics': ['metric', 'kpi', 'measurement', 'stat', 'dimension'],
    'log': ['event', 'audit', 'tracking', 'history', 'activity'],
    'marketing': ['campaign', 'lead', 'prospect', 'conversion', 'engagement'],
    'operational': ['process', 'workflow', 'pipeline', 'job', 'task'],
    'security': ['access', 'permission', 'role', 'authentication', 'authorization'],
    'location': ['address', 'geography', 'region', 'country', 'city']
}

# Data Quality Scoring Weights
QUALITY_WEIGHTS = {
    'table_documentation': 0.6,  # 60% weight for table comments
    'column_documentation': 0.4  # 40% weight for column comments
}

# Quality Score Thresholds
QUALITY_THRESHOLDS = {
    'excellent': 90,
    'good': 75,
    'fair': 50,
    'poor': 0
}

# CSS Styling
CUSTOM_CSS = """
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
    .success-tag {
        background-color: #e8f5e8;
        color: #2e7d32;
    }
    .warning-tag {
        background-color: #fff3e0;
        color: #f57c00;
    }
    .error-tag {
        background-color: #ffebee;
        color: #d32f2f;
    }
</style>
"""

# Error Messages
ERROR_MESSAGES = {
    'no_data': "No data available. Please check your connection and permissions.",
    'search_failed': "Search failed. Please try again or contact support.",
    'table_not_found': "Table not found or not accessible.",
    'permission_denied': "Permission denied. Contact your administrator.",
    'connection_error': "Connection error. Please check your network and try again.",
    'invalid_query': "Invalid query format. Please check your search terms."
}

# Success Messages
SUCCESS_MESSAGES = {
    'data_loaded': "Data loaded successfully!",
    'search_completed': "Search completed successfully!",
    'table_found': "Table information retrieved successfully!",
    'recommendations_generated': "Recommendations generated based on your activity!"
}

# Default Values
DEFAULTS = {
    'catalog_filter': "All Catalogs",
    'schema_filter': "All Schemas",
    'search_type': "Smart Search",
    'results_per_page': 10,
    'chart_height': 400,
    'table_preview_rows': 10
} 