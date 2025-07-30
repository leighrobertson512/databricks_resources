# Data Discovery Hub - Databricks Custom App

A comprehensive data discovery application for Databricks Unity Catalog that provides business-friendly interface for exploring catalogs, schemas, tables, and columns.

## Features

🔍 **Smart Search & Discovery**
- Intelligent search across table names, column names, and descriptions
- Fuzzy matching and relevance scoring
- Search suggestions and recommendations

📊 **Interactive Analytics**
- Catalog overview dashboards
- Data quality scoring
- Documentation coverage analysis
- Usage patterns and trends

🎯 **Table Recommendations**
- Personalized table suggestions
- Similar table discovery
- Popular and well-documented table highlights

📈 **Visualization Engine**
- Interactive charts and graphs
- Data quality gauges
- Activity heatmaps
- Distribution analyses

## Architecture

The app is built using:
- **Frontend**: Streamlit for interactive web interface
- **Backend**: Databricks Unity Catalog APIs
- **Data Source**: Materialized view from `obs_query_base.py`
- **Visualizations**: Plotly for interactive charts
- **Search Engine**: Custom intelligent search with relevance scoring

## Prerequisites

1. **Databricks Workspace** with Unity Catalog enabled
2. **Materialized View** created from `obs_query_base.py`:
   ```sql
   -- Run the obs_query_base.py notebook first to create the base materialized view
   -- leigh_robertson_demo.observability.tables_columns_genie
   ```
3. **Permissions** to read from Unity Catalog metadata
4. **Databricks Apps** feature enabled in your workspace

## Quick Start

### 1. Create the Databricks App

Following the [Databricks custom app documentation](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/create-custom-app):

1. In your Databricks workspace sidebar, click **New** → **App**
2. Select **Custom** option
3. Enter app name: `data-discovery-hub`
4. Click **Create app**

### 2. Upload the Code

Upload all files from the `observability/` folder to your Databricks workspace:

```
observability/
├── app.py                     # Main Streamlit application
├── data_discovery_utils.py    # Core data access utilities
├── search_utils.py           # Smart search and recommendations
├── visualization_utils.py    # Chart and visualization engine
├── obs_query_base.py         # Materialized view creation (run first)
├── requirements.txt          # Python dependencies
└── README.md                # This file
```

### 3. Install Dependencies

In a Databricks notebook, install the required packages:

```python
%pip install streamlit>=1.28.0 plotly>=5.15.0 pandas>=2.0.0 numpy>=1.24.0
```

### 4. Configure the App

Update the materialized view name in the utility files if needed:
- Check `data_discovery_utils.py` line 23
- Check `search_utils.py` lines 25 and 234

Ensure the view name matches your setup:
```python
self.base_view = "leigh_robertson_demo.observability.tables_columns_genie"
```

### 5. Deploy the App

1. Run the `obs_query_base.py` notebook first to create the materialized view
2. In your app's details page, follow the sync instructions to upload your code
3. Deploy the app using the provided deployment commands

### 6. Access the App

Once deployed, access your app through the Databricks Apps interface. The app will be available at your workspace URL.

## Configuration

### Environment Variables

The app reads configuration from several sources:

1. **Materialized View**: Update the view name in utility files
2. **Databricks Secrets**: Used for external API access (if needed)
3. **Streamlit Config**: Customizable in `app.py`

### Customization

You can customize the app by modifying:

- **Color schemes**: Edit `visualization_utils.py` color palettes
- **Search algorithms**: Modify `search_utils.py` relevance scoring
- **UI layout**: Update `app.py` Streamlit components
- **Data sources**: Extend `data_discovery_utils.py` for additional data

## Usage Guide

### Search & Discovery
1. Use the search box to find tables by name, column, or description
2. Try different search types (Smart Search, Table Names, etc.)
3. Use catalog and schema filters to narrow results
4. Click "Surprise Me!" for random table discovery

### Analytics Dashboard
1. View overview metrics on the main dashboard
2. Explore catalog distribution charts
3. Check data quality scores
4. Monitor documentation coverage

### Table Exploration
1. Click "Explore" on any table card for detailed view
2. Use "Preview" to see sample data
3. View column schemas and data types
4. Get recommendations for similar tables

## Development

### Local Development

For local development (outside Databricks):

```bash
# Install dependencies
pip install -r requirements.txt

# Run the app (mock Databricks environment needed)
streamlit run app.py
```

### Testing

The app includes error handling and graceful degradation:
- Empty data states are handled with informative messages
- API failures show user-friendly error messages
- Search returns empty results gracefully

### Extending the App

To add new features:

1. **New data sources**: Extend `DataDiscoveryAPI` class
2. **New visualizations**: Add methods to `DataVisualizationEngine`
3. **New search types**: Extend `SmartSearch` class
4. **New pages**: Add to the main app navigation

## Troubleshooting

### Common Issues

1. **Materialized view not found**
   - Run `obs_query_base.py` first
   - Check view name in configuration
   - Verify permissions to read the view

2. **Search returns no results**
   - Check materialized view has data
   - Verify search term spelling
   - Try different search types

3. **Charts not loading**
   - Check data availability
   - Verify Plotly installation
   - Check browser console for errors

4. **Permission errors**
   - Ensure Unity Catalog read permissions
   - Check workspace access rights
   - Verify app deployment permissions

### Performance Optimization

- **Caching**: The app uses built-in caching for expensive queries
- **Pagination**: Search results are limited to prevent overload
- **Lazy loading**: Charts load only when data is available

## Contributing

To contribute to this project:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request with detailed description

## License

This project is designed for internal use within Databricks environments. Please ensure compliance with your organization's data governance policies.

## Support

For support and questions:

1. Check the troubleshooting section above
2. Review Databricks Apps documentation
3. Contact your Databricks administrator
4. Create an issue in the project repository

---

**Built with ❤️ for data discovery and collaboration**

Inspired by observability patterns from the [Databricks SME repository](https://github.com/CodyAustinDavis/dbsql_sme) and designed to make data assets more discoverable and accessible to business users. 