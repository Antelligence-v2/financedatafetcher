"""
Data warehouse for storing and querying normalized data points.
Uses in-memory pandas DataFrame for MVP, designed for future PostgreSQL migration.
"""

from typing import List, Optional, Tuple, Dict, Any, Union
from pathlib import Path
from datetime import date, datetime
import pandas as pd

from ..normalizer import (
    NormalizedDataPoint,
    CoinglassTransformer,
    InvezzTransformer,
    CoinGeckoTransformer,
    DuneTransformer,
    TheBlockTransformer,
)
from ..utils.logger import get_logger


class DataWarehouse:
    """
    In-memory data warehouse for normalized data points.
    Stores all data in a pandas DataFrame for fast queries.
    """
    
    # Source to transformer mapping
    TRANSFORMERS = {
        'coinglass': CoinglassTransformer,
        'invezz': InvezzTransformer,
        'coingecko': CoinGeckoTransformer,
        'dune': DuneTransformer,
        'theblock': TheBlockTransformer,
    }
    
    def __init__(self):
        """Initialize empty warehouse."""
        self.logger = get_logger()
        # Store data points as DataFrame
        self._data: Optional[pd.DataFrame] = None
        self._initialize_empty()
    
    def _initialize_empty(self):
        """Initialize empty DataFrame with correct schema."""
        self._data = pd.DataFrame({
            'id': pd.Series(dtype='str'),
            'source': pd.Series(dtype='str'),
            'asset': pd.Series(dtype='str'),
            'metric': pd.Series(dtype='str'),
            'timestamp': pd.Series(dtype='datetime64[ns]'),
            'date': pd.Series(dtype='datetime64[ns]'),
            'value': pd.Series(dtype='float64'),
            'value_usd': pd.Series(dtype='float64'),
            'value_btc': pd.Series(dtype='float64'),
            'unit': pd.Series(dtype='str'),
            'confidence': pd.Series(dtype='int64'),
            'data_type': pd.Series(dtype='str'),
            'category': pd.Series(dtype='str'),
            'raw_source_field': pd.Series(dtype='str'),
            'metadata': pd.Series(dtype='str'),
        })
    
    def add_data_point(self, point: NormalizedDataPoint) -> None:
        """
        Add a single normalized data point to the warehouse.
        
        Args:
            point: NormalizedDataPoint to add
        """
        point_dict = point.to_dict()
        # Convert metadata dict to string for DataFrame storage
        point_dict['metadata'] = str(point_dict['metadata'])
        
        new_row = pd.DataFrame([point_dict])
        
        if self._data is None or self._data.empty:
            self._data = new_row
        else:
            # Check for duplicates (same id)
            if point.id not in self._data['id'].values:
                # Ensure both DataFrames have the same columns
                all_columns = set(self._data.columns) | set(new_row.columns)
                for col in all_columns:
                    if col not in self._data.columns:
                        self._data[col] = None
                    if col not in new_row.columns:
                        new_row[col] = None
                
                # Reorder columns to match
                new_row = new_row[self._data.columns]
                self._data = pd.concat([self._data, new_row], ignore_index=True)
            else:
                self.logger.debug(f"Duplicate data point skipped: {point.id}")
    
    def add_data_points(self, points: List[NormalizedDataPoint]) -> None:
        """
        Add multiple normalized data points to the warehouse.
        
        Args:
            points: List of NormalizedDataPoint objects
        """
        for point in points:
            self.add_data_point(point)
    
    def load_from_excel(
        self,
        file_path: Union[Path, str],
        source: str
    ) -> int:
        """
        Load and normalize data from an Excel file.
        
        Args:
            file_path: Path to Excel file
            source: Source name ('coinglass', 'invezz', 'coingecko', 'dune', 'theblock')
            
        Returns:
            Number of data points added
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Excel file not found: {file_path}")
        
        # Get transformer for source
        if source not in self.TRANSFORMERS:
            raise ValueError(f"Unknown source: {source}. Available: {list(self.TRANSFORMERS.keys())}")
        
        transformer_class = self.TRANSFORMERS[source]
        transformer = transformer_class()
        
        # Load Excel file
        self.logger.info(f"Loading Excel file: {file_path} (source: {source})")
        df = pd.read_excel(file_path)
        
        # Transform to normalized data points
        normalized_points = transformer.transform(df)
        
        # Add to warehouse
        self.add_data_points(normalized_points)
        
        self.logger.info(f"Loaded {len(normalized_points)} data points from {source}")
        return len(normalized_points)
    
    def load_from_dataframes(
        self,
        dataframes: Dict[str, pd.DataFrame],
        source_mapping: Dict[str, str]
    ) -> int:
        """
        Load and normalize data from in-memory DataFrames.
        
        Args:
            dataframes: Dictionary mapping identifier (e.g., site_id) to DataFrame
            source_mapping: Dictionary mapping identifier to source name
                          (e.g., {'coinglass_btc_overview': 'coinglass'})
        
        Returns:
            Total number of data points added
        """
        total_points = 0
        
        for identifier, df in dataframes.items():
            if df.empty:
                self.logger.warning(f"Empty DataFrame for identifier: {identifier}")
                continue
            
            # Get source name from mapping
            source = source_mapping.get(identifier)
            if not source:
                # Try to infer from identifier
                identifier_lower = identifier.lower()
                if 'coinglass' in identifier_lower:
                    source = 'coinglass'
                elif 'invezz' in identifier_lower:
                    source = 'invezz'
                elif 'coingecko' in identifier_lower:
                    source = 'coingecko'
                elif 'dune' in identifier_lower:
                    source = 'dune'
                elif 'theblock' in identifier_lower:
                    source = 'theblock'
                else:
                    self.logger.warning(f"Could not determine source for identifier: {identifier}")
                    continue
            
            # Get transformer for source
            if source not in self.TRANSFORMERS:
                self.logger.warning(f"Unknown source: {source} for identifier: {identifier}")
                continue
            
            try:
                transformer_class = self.TRANSFORMERS[source]
                transformer = transformer_class()
                
                # Transform to normalized data points
                normalized_points = transformer.transform(df)
                
                # Add to warehouse
                self.add_data_points(normalized_points)
                total_points += len(normalized_points)
                
                self.logger.info(
                    f"Loaded {len(normalized_points)} data points from {source} "
                    f"(identifier: {identifier})"
                )
            except Exception as e:
                self.logger.error(f"Error loading DataFrame for {identifier} ({source}): {str(e)}")
                import traceback
                self.logger.error(traceback.format_exc())
                continue
        
        self.logger.info(f"Total loaded: {total_points} data points from {len(dataframes)} DataFrames")
        return total_points
    
    def query(
        self,
        asset: Optional[str] = None,
        metrics: Optional[List[str]] = None,
        date_range: Optional[Tuple[date, date]] = None,
        sources: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Query data points by asset, metrics, date range, and sources.
        
        Args:
            asset: Asset filter (e.g., 'BTC', 'ETH', 'ALL')
            metrics: List of metric names to filter
            date_range: Tuple of (start_date, end_date)
            sources: List of source names to filter
            
        Returns:
            DataFrame with matching data points
        """
        if self._data is None or self._data.empty:
            return pd.DataFrame()
        
        result = self._data.copy()
        
        # Filter by asset
        if asset:
            result = result[result['asset'] == asset]
        
        # Filter by metrics
        if metrics:
            result = result[result['metric'].isin(metrics)]
        
        # Filter by date range
        if date_range:
            start_date, end_date = date_range
            result = result[
                (result['date'] >= pd.Timestamp(start_date)) &
                (result['date'] <= pd.Timestamp(end_date))
            ]
        
        # Filter by sources
        if sources:
            result = result[result['source'].isin(sources)]
        
        # Sort by timestamp descending (latest first)
        result = result.sort_values('timestamp', ascending=False)
        
        return result.reset_index(drop=True)
    
    def get_available_assets(self) -> List[str]:
        """
        Get list of available assets in the warehouse.
        
        Returns:
            Sorted list of asset identifiers
        """
        if self._data is None or self._data.empty:
            return []
        
        assets = self._data['asset'].unique().tolist()
        return sorted(assets)
    
    def get_available_metrics(self, asset: Optional[str] = None) -> List[str]:
        """
        Get list of available metrics, optionally filtered by asset.
        
        Args:
            asset: Optional asset filter
            
        Returns:
            Sorted list of metric names
        """
        if self._data is None or self._data.empty:
            return []
        
        result = self._data.copy()
        if asset:
            result = result[result['asset'] == asset]
        
        metrics = result['metric'].unique().tolist()
        return sorted(metrics)
    
    def get_available_sources(self) -> List[str]:
        """
        Get list of available data sources.
        
        Returns:
            Sorted list of source names
        """
        if self._data is None or self._data.empty:
            return []
        
        sources = self._data['source'].unique().tolist()
        return sorted(sources)
    
    def get_data_coverage(
        self,
        asset: Optional[str] = None,
        metric: Optional[str] = None
    ) -> Dict[str, any]:
        """
        Get data coverage information (date ranges, counts, etc.).
        
        Args:
            asset: Optional asset filter
            metric: Optional metric filter
            
        Returns:
            Dictionary with coverage information
        """
        if self._data is None or self._data.empty:
            return {
                'total_points': 0,
                'date_range': None,
                'sources': [],
                'assets': [],
                'metrics': []
            }
        
        result = self._data.copy()
        if asset:
            result = result[result['asset'] == asset]
        if metric:
            result = result[result['metric'] == metric]
        
        if result.empty:
            return {
                'total_points': 0,
                'date_range': None,
                'sources': [],
                'assets': [],
                'metrics': []
            }
        
        return {
            'total_points': len(result),
            'date_range': (
                result['date'].min(),
                result['date'].max()
            ),
            'sources': sorted(result['source'].unique().tolist()),
            'assets': sorted(result['asset'].unique().tolist()),
            'metrics': sorted(result['metric'].unique().tolist()),
        }
    
    def get_dataframe(self) -> pd.DataFrame:
        """
        Get the full warehouse DataFrame.
        
        Returns:
            Complete DataFrame with all data points
        """
        if self._data is None:
            self._initialize_empty()
        return self._data.copy()
    
    def clear(self) -> None:
        """Clear all data from the warehouse."""
        self._initialize_empty()
        self.logger.info("Warehouse cleared")
    
    def get_stats(self) -> Dict[str, any]:
        """
        Get warehouse statistics.
        
        Returns:
            Dictionary with statistics
        """
        if self._data is None or self._data.empty:
            return {
                'total_points': 0,
                'sources': {},
                'assets': {},
                'metrics': {},
            }
        
        return {
            'total_points': len(self._data),
            'sources': self._data['source'].value_counts().to_dict(),
            'assets': self._data['asset'].value_counts().to_dict(),
            'metrics': self._data['metric'].value_counts().to_dict(),
        }
