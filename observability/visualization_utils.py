"""
Visualization Utilities
=======================

Chart and visualization creation utilities for the Data Discovery App.
Provides reusable visualization components using Plotly.
"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta

class DataVisualizationEngine:
    """
    Core visualization engine for creating interactive charts and dashboards.
    """
    
    def __init__(self):
        """Initialize the visualization engine"""
        self.color_palette = [
            '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
            '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'
        ]
        self.theme_config = {
            'layout': {
                'font': {'family': 'Arial, sans-serif', 'size': 12},
                'plot_bgcolor': 'rgba(0,0,0,0)',
                'paper_bgcolor': 'rgba(0,0,0,0)',
                'margin': dict(l=20, r=20, t=40, b=20)
            }
        }
    
    def create_catalog_overview_chart(self, catalog_data: pd.DataFrame) -> go.Figure:
        """Create overview chart showing catalogs and their table counts"""
        if catalog_data.empty:
            return self._create_empty_chart("No catalog data available")
        
        fig = px.bar(
            catalog_data, 
            x='catalog', 
            y='table_count',
            title='Tables by Catalog',
            color='table_count',
            color_continuous_scale='Blues'
        )
        
        fig.update_layout(
            **self.theme_config['layout'],
            xaxis_title="Catalog",
            yaxis_title="Number of Tables",
            title_x=0.5
        )
        
        return fig
    
    def create_table_type_distribution(self, type_data: pd.DataFrame) -> go.Figure:
        """Create pie chart showing distribution of table types"""
        if type_data.empty:
            return self._create_empty_chart("No table type data available")
        
        fig = px.pie(
            type_data,
            values='count',
            names='table_type',
            title='Table Types Distribution',
            color_discrete_sequence=self.color_palette
        )
        
        fig.update_layout(
            **self.theme_config['layout'],
            title_x=0.5
        )
        
        fig.update_traces(
            textposition='inside',
            textinfo='percent+label'
        )
        
        return fig
    
    def create_data_quality_gauge(self, quality_score: float) -> go.Figure:
        """Create gauge chart for data quality score"""
        color = 'red' if quality_score < 50 else 'orange' if quality_score < 75 else 'green'
        
        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=quality_score,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "Data Quality Score"},
            delta={'reference': 80},
            gauge={
                'axis': {'range': [None, 100]},
                'bar': {'color': color},
                'steps': [
                    {'range': [0, 50], 'color': "lightgray"},
                    {'range': [50, 80], 'color': "gray"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 90
                }
            }
        ))
        
        fig.update_layout(**self.theme_config['layout'])
        return fig
    
    def create_documentation_coverage_chart(self, doc_stats: Dict[str, int]) -> go.Figure:
        """Create chart showing documentation coverage statistics"""
        categories = ['Tables with Comments', 'Tables without Comments', 'Columns with Comments']
        values = [
            doc_stats.get('tables_with_comments', 0),
            doc_stats.get('tables_without_comments', 0),
            doc_stats.get('columns_with_comments', 0)
        ]
        
        colors = ['#2ca02c', '#d62728', '#1f77b4']
        
        fig = go.Figure(data=[
            go.Bar(
                x=categories,
                y=values,
                marker_color=colors,
                text=values,
                textposition='auto'
            )
        ])
        
        fig.update_layout(
            **self.theme_config['layout'],
            title='Documentation Coverage',
            title_x=0.5,
            xaxis_title="Category",
            yaxis_title="Count"
        )
        
        return fig
    
    def create_usage_trends_chart(self, usage_data: pd.DataFrame) -> go.Figure:
        """Create line chart showing usage trends over time"""
        if usage_data.empty:
            return self._create_empty_chart("No usage data available")
        
        fig = px.line(
            usage_data,
            x='date',
            y='query_count',
            title='Query Volume Trends',
            markers=True
        )
        
        fig.update_layout(
            **self.theme_config['layout'],
            title_x=0.5,
            xaxis_title="Date",
            yaxis_title="Query Count"
        )
        
        fig.update_traces(
            line_color='#1f77b4',
            line_width=3
        )
        
        return fig
    
    def create_schema_comparison_chart(self, schema_data: pd.DataFrame) -> go.Figure:
        """Create horizontal bar chart comparing schemas by table count"""
        if schema_data.empty:
            return self._create_empty_chart("No schema data available")
        
        # Sort by table count for better visualization
        schema_data_sorted = schema_data.sort_values('table_count', ascending=True)
        
        fig = go.Figure(go.Bar(
            x=schema_data_sorted['table_count'],
            y=schema_data_sorted['schema_name'],
            orientation='h',
            marker_color='lightblue',
            text=schema_data_sorted['table_count'],
            textposition='auto'
        ))
        
        fig.update_layout(
            **self.theme_config['layout'],
            title='Tables by Schema',
            title_x=0.5,
            xaxis_title="Number of Tables",
            yaxis_title="Schema"
        )
        
        return fig
    
    def create_owner_distribution_chart(self, owner_data: pd.DataFrame) -> go.Figure:
        """Create chart showing table ownership distribution"""
        if owner_data.empty:
            return self._create_empty_chart("No ownership data available")
        
        # Take top 10 owners
        top_owners = owner_data.head(10)
        
        fig = px.bar(
            top_owners,
            x='table_owner',
            y='table_count',
            title='Top Table Owners',
            color='table_count',
            color_continuous_scale='Viridis'
        )
        
        fig.update_layout(
            **self.theme_config['layout'],
            title_x=0.5,
            xaxis_title="Owner",
            yaxis_title="Number of Tables",
            xaxis={'tickangle': 45}
        )
        
        return fig
    
    def create_column_type_distribution(self, column_data: pd.DataFrame) -> go.Figure:
        """Create chart showing distribution of column data types"""
        if column_data.empty:
            return self._create_empty_chart("No column data available")
        
        # Group by data type and count
        type_counts = column_data['data_type'].value_counts().head(15)  # Top 15 types
        
        fig = px.bar(
            x=type_counts.index,
            y=type_counts.values,
            title='Column Data Types Distribution',
            color=type_counts.values,
            color_continuous_scale='plasma'
        )
        
        fig.update_layout(
            **self.theme_config['layout'],
            title_x=0.5,
            xaxis_title="Data Type",
            yaxis_title="Count",
            xaxis={'tickangle': 45}
        )
        
        return fig
    
    def create_activity_heatmap(self, activity_data: pd.DataFrame) -> go.Figure:
        """Create heatmap showing activity patterns"""
        if activity_data.empty:
            return self._create_empty_chart("No activity data available")
        
        # This is a placeholder implementation
        # In a real scenario, you'd process query logs or access patterns
        days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        hours = list(range(24))
        
        # Generate sample activity data
        np.random.seed(42)
        activity_matrix = np.random.randint(0, 100, size=(len(days), len(hours)))
        
        fig = go.Figure(data=go.Heatmap(
            z=activity_matrix,
            x=hours,
            y=days,
            colorscale='Blues',
            showscale=True
        ))
        
        fig.update_layout(
            **self.theme_config['layout'],
            title='Database Activity Heatmap (Sample)',
            title_x=0.5,
            xaxis_title="Hour of Day",
            yaxis_title="Day of Week"
        )
        
        return fig
    
    def create_search_results_summary(self, search_results: pd.DataFrame) -> go.Figure:
        """Create summary visualization for search results"""
        if search_results.empty:
            return self._create_empty_chart("No search results to visualize")
        
        # Create subplots
        fig = make_subplots(
            rows=1, cols=2,
            specs=[[{"type": "bar"}, {"type": "pie"}]],
            subplot_titles=("Results by Catalog", "Results by Table Type")
        )
        
        # Results by catalog
        catalog_counts = search_results['table_catalog'].value_counts()
        fig.add_trace(
            go.Bar(x=catalog_counts.index, y=catalog_counts.values, name="Catalog"),
            row=1, col=1
        )
        
        # Results by table type
        type_counts = search_results['table_type'].value_counts()
        fig.add_trace(
            go.Pie(labels=type_counts.index, values=type_counts.values, name="Type"),
            row=1, col=2
        )
        
        fig.update_layout(
            **self.theme_config['layout'],
            title_text="Search Results Analysis",
            title_x=0.5,
            showlegend=False
        )
        
        return fig
    
    def create_table_relationship_network(self, relationships: pd.DataFrame) -> go.Figure:
        """Create network graph showing table relationships (placeholder)"""
        # This is a placeholder for more advanced lineage visualization
        # In a real implementation, this would show data lineage and dependencies
        
        fig = go.Figure()
        
        # Add sample nodes and edges
        fig.add_trace(go.Scatter(
            x=[1, 2, 3, 4, 5],
            y=[1, 3, 2, 4, 3],
            mode='markers+text',
            marker=dict(size=20, color='lightblue'),
            text=['Table A', 'Table B', 'Table C', 'Table D', 'Table E'],
            textposition="middle center",
            name="Tables"
        ))
        
        # Add sample connections
        for i in range(4):
            fig.add_trace(go.Scatter(
                x=[i+1, i+2],
                y=[i%3+1, (i+1)%3+2],
                mode='lines',
                line=dict(color='gray', width=2),
                showlegend=False
            ))
        
        fig.update_layout(
            **self.theme_config['layout'],
            title='Table Relationships (Sample)',
            title_x=0.5,
            xaxis={'showgrid': False, 'zeroline': False, 'showticklabels': False},
            yaxis={'showgrid': False, 'zeroline': False, 'showticklabels': False}
        )
        
        return fig
    
    def _create_empty_chart(self, message: str) -> go.Figure:
        """Create an empty chart with a message"""
        fig = go.Figure()
        
        fig.add_annotation(
            x=0.5, y=0.5,
            text=message,
            showarrow=False,
            font=dict(size=16, color="gray"),
            xref="paper", yref="paper"
        )
        
        fig.update_layout(
            **self.theme_config['layout'],
            xaxis={'showgrid': False, 'zeroline': False, 'showticklabels': False},
            yaxis={'showgrid': False, 'zeroline': False, 'showticklabels': False}
        )
        
        return fig


# Convenience functions for easy access
def create_summary_charts(data: Dict[str, pd.DataFrame]) -> Dict[str, go.Figure]:
    """Create a set of summary charts from provided data"""
    viz_engine = DataVisualizationEngine()
    charts = {}
    
    if 'catalog_data' in data:
        charts['catalog_overview'] = viz_engine.create_catalog_overview_chart(data['catalog_data'])
    
    if 'type_data' in data:
        charts['type_distribution'] = viz_engine.create_table_type_distribution(data['type_data'])
    
    if 'owner_data' in data:
        charts['owner_distribution'] = viz_engine.create_owner_distribution_chart(data['owner_data'])
    
    if 'usage_data' in data:
        charts['usage_trends'] = viz_engine.create_usage_trends_chart(data['usage_data'])
    
    return charts


def create_data_lineage_viz(lineage_data: pd.DataFrame) -> go.Figure:
    """Create data lineage visualization"""
    viz_engine = DataVisualizationEngine()
    return viz_engine.create_table_relationship_network(lineage_data)


def create_search_analytics(search_results: pd.DataFrame) -> go.Figure:
    """Create analytics visualization for search results"""
    viz_engine = DataVisualizationEngine()
    return viz_engine.create_search_results_summary(search_results)


def create_quality_dashboard(quality_data: Dict[str, Any]) -> Dict[str, go.Figure]:
    """Create data quality dashboard charts"""
    viz_engine = DataVisualizationEngine()
    dashboard = {}
    
    if 'quality_score' in quality_data:
        dashboard['quality_gauge'] = viz_engine.create_data_quality_gauge(quality_data['quality_score'])
    
    if 'doc_stats' in quality_data:
        dashboard['documentation'] = viz_engine.create_documentation_coverage_chart(quality_data['doc_stats'])
    
    if 'activity_data' in quality_data:
        dashboard['activity_heatmap'] = viz_engine.create_activity_heatmap(quality_data['activity_data'])
    
    return dashboard 