"""
Data fetcher service for querying normalized data.
Provides high-level interface for asset + metric queries.
"""

from typing import List, Optional, Tuple, Dict, Any
from datetime import date, datetime
import pandas as pd

from ..warehouse import DataWarehouse
from ..utils.logger import get_logger


class DataFetcherService:
    """
    Service for fetching and aggregating normalized data.
    Handles multi-source aggregation and time-series formatting.
    """
    
    def __init__(self, warehouse: Optional[DataWarehouse] = None):
        """
        Initialize data fetcher service.
        
        Args:
            warehouse: DataWarehouse instance (creates new if not provided)
        """
        self.warehouse = warehouse or DataWarehouse()
        self.logger = get_logger()
    
    def fetch_data(
        self,
        asset: str,
        metrics: List[str],
        date_range: Optional[Tuple[date, date]] = None,
        sources: Optional[List[str]] = None,
        aggregation: str = 'latest'  # 'latest', 'average', 'sum', 'all'
    ) -> Dict[str, Any]:
        """
        Fetch data for given asset and metrics.
        
        Args:
            asset: Asset identifier (e.g., 'BTC', 'ETH', 'ALL')
            metrics: List of metric names to fetch
            date_range: Optional tuple of (start_date, end_date)
            sources: Optional list of source names to filter
            aggregation: How to handle multiple sources for same metric
                - 'latest': Use most recent value
                - 'average': Average across sources
                - 'sum': Sum across sources
                - 'all': Keep all sources (show breakdown)
        
        Returns:
            Dictionary with:
            - 'data': DataFrame with time-series data
            - 'metadata': Dict with query info, sources used, etc.
        """
        # Query warehouse
        query_result = self.warehouse.query(
            asset=asset,
            metrics=metrics,
            date_range=date_range,
            sources=sources
        )
        
        if query_result.empty:
            return {
                'data': pd.DataFrame(),
                'metadata': {
                    'asset': asset,
                    'metrics': metrics,
                    'sources_used': [],
                    'total_points': 0,
                    'date_range': None,
                    'warnings': ['No data found for the specified query']
                }
            }
        
        # Process and aggregate data
        if aggregation == 'all':
            # Keep all sources, show breakdown
            processed_data = self._format_with_source_breakdown(query_result, metrics)
        else:
            # Aggregate multi-source metrics
            processed_data = self._aggregate_multi_source(query_result, metrics, aggregation)
        
        # Get metadata
        sources_used = sorted(query_result['source'].unique().tolist())
        date_range_actual = (
            query_result['date'].min(),
            query_result['date'].max()
        ) if not query_result.empty else None
        
        metadata = {
            'asset': asset,
            'metrics': metrics,
            'sources_used': sources_used,
            'total_points': len(query_result),
            'date_range': date_range_actual,
            'aggregation_method': aggregation,
            'warnings': []
        }
        
        # Add warnings for multi-source metrics
        for metric in metrics:
            metric_data = query_result[query_result['metric'] == metric]
            source_count = metric_data['source'].nunique()
            if source_count > 1:
                metadata['warnings'].append(
                    f"{metric}: Data from {source_count} sources ({', '.join(metric_data['source'].unique())})"
                )
        
        return {
            'data': processed_data,
            'metadata': metadata
        }
    
    def _format_with_source_breakdown(
        self,
        df: pd.DataFrame,
        metrics: List[str]
    ) -> pd.DataFrame:
        """
        Format data showing source breakdown for each metric.
        
        Args:
            df: Query result DataFrame
            metrics: List of metrics
            
        Returns:
            Formatted DataFrame with source columns
        """
        if df.empty:
            return pd.DataFrame()
        
        # Group by timestamp and metric, then pivot sources
        result_rows = []
        
        for timestamp in df['timestamp'].unique():
            timestamp_data = df[df['timestamp'] == timestamp]
            row = {'timestamp': timestamp, 'date': timestamp_data['date'].iloc[0]}
            
            for metric in metrics:
                metric_data = timestamp_data[timestamp_data['metric'] == metric]
                if not metric_data.empty:
                    # Add value_usd (preferred) or value
                    for idx, point in metric_data.iterrows():
                        source = point['source']
                        value = point.get('value_usd') if pd.notna(point.get('value_usd')) else point['value']
                        row[f"{metric}_{source}"] = value
                        row[f"{metric}_source"] = source
            
            result_rows.append(row)
        
        return pd.DataFrame(result_rows).sort_values('timestamp', ascending=False)
    
    def _aggregate_multi_source(
        self,
        df: pd.DataFrame,
        metrics: List[str],
        aggregation: str
    ) -> pd.DataFrame:
        """
        Aggregate data when same metric comes from multiple sources.
        
        Args:
            df: Query result DataFrame
            metrics: List of metrics
            aggregation: Aggregation method ('latest', 'average', 'sum')
            
        Returns:
            Aggregated DataFrame
        """
        if df.empty:
            return pd.DataFrame()
        
        result_rows = []
        
        # Group by timestamp
        for timestamp in df['timestamp'].unique():
            timestamp_data = df[df['timestamp'] == timestamp]
            row = {
                'timestamp': timestamp,
                'date': timestamp_data['date'].iloc[0]
            }
            
            # Process each metric
            for metric in metrics:
                metric_data = timestamp_data[timestamp_data['metric'] == metric]
                
                if not metric_data.empty:
                    # Prefer value_usd, fallback to value
                    values = []
                    for idx, point in metric_data.iterrows():
                        value = point.get('value_usd') if pd.notna(point.get('value_usd')) else point['value']
                        if pd.notna(value):
                            values.append(value)
                    
                    if values:
                        if aggregation == 'latest':
                            # Use first (most recent) value
                            row[metric] = values[0]
                        elif aggregation == 'average':
                            row[metric] = sum(values) / len(values)
                        elif aggregation == 'sum':
                            row[metric] = sum(values)
                        
                        # Store source info
                        sources = metric_data['source'].unique().tolist()
                        row[f"{metric}_sources"] = ', '.join(sources)
                        row[f"{metric}_source_count"] = len(sources)
            
            result_rows.append(row)
        
        result_df = pd.DataFrame(result_rows)
        
        # Sort by timestamp descending
        if not result_df.empty:
            result_df = result_df.sort_values('timestamp', ascending=False)
        
        return result_df
    
    def get_available_assets(self) -> List[str]:
        """Get list of available assets."""
        return self.warehouse.get_available_assets()
    
    def get_available_metrics(self, asset: Optional[str] = None) -> List[str]:
        """Get list of available metrics."""
        return self.warehouse.get_available_metrics(asset)
    
    def get_data_coverage(self, asset: Optional[str] = None, metric: Optional[str] = None) -> Dict[str, Any]:
        """Get data coverage information."""
        return self.warehouse.get_data_coverage(asset, metric)
