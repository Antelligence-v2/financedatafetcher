"""
Invezz data transformer.
Transforms Invezz liquidations data into normalized data points.
"""

from typing import List
import pandas as pd

from .data_normalizer import BaseTransformer, NormalizedDataPoint


class InvezzTransformer(BaseTransformer):
    """
    Transformer for Invezz liquidations data.
    Maps Invezz fields to normalized schema.
    """
    
    def __init__(self):
        super().__init__('invezz')
    
    def transform(self, df: pd.DataFrame) -> List[NormalizedDataPoint]:
        """
        Transform Invezz DataFrame to normalized data points.
        
        Expected columns:
        - timestamp
        - total_liquidations_24h
        - long_liquidations
        - short_liquidations
        
        Args:
            df: Invezz DataFrame
            
        Returns:
            List of NormalizedDataPoint objects
        """
        if df.empty:
            return []
        
        normalized_points = []
        
        # Get timestamp
        timestamp = pd.to_datetime(df['timestamp'].iloc[0]) if 'timestamp' in df.columns else pd.Timestamp.now()
        date = timestamp.normalize()
        
        # Get BTC price for conversions (if available)
        btc_price = self._get_btc_price(df)
        
        # 1. Total Liquidations 24h
        if 'total_liquidations_24h' in df.columns and len(df) > 0:
            value = df['total_liquidations_24h'].iloc[0]
            if pd.notna(value) and value > 0:
                value_float = float(value)
                normalized_points.append(
                    NormalizedDataPoint(
                        id=self._generate_id('ALL', 'liquidations_24h_total', timestamp),
                        source=self.source_name,
                        asset='ALL',
                        metric='liquidations_24h_total',
                        timestamp=timestamp,
                        date=date,
                        value=value_float,
                        value_usd=value_float,
                        value_btc=self._convert_to_btc(value_float, 'USD', btc_price),
                        unit='USD',
                        confidence=90,
                        data_type='aggregate',
                        category='liquidations',
                        raw_source_field='total_liquidations_24h',
                        metadata={
                            'long_liquidations': float(df['long_liquidations'].iloc[0]) if 'long_liquidations' in df.columns and len(df) > 0 else None,
                            'short_liquidations': float(df['short_liquidations'].iloc[0]) if 'short_liquidations' in df.columns and len(df) > 0 else None,
                        }
                    )
                )
        
        # 2. Long Liquidations
        if 'long_liquidations' in df.columns and len(df) > 0:
            value = df['long_liquidations'].iloc[0]
            if pd.notna(value) and value > 0:
                value_float = float(value)
                normalized_points.append(
                    NormalizedDataPoint(
                        id=self._generate_id('ALL', 'liquidations_24h_long', timestamp),
                        source=self.source_name,
                        asset='ALL',
                        metric='liquidations_24h_long',
                        timestamp=timestamp,
                        date=date,
                        value=value_float,
                        value_usd=value_float,
                        value_btc=self._convert_to_btc(value_float, 'USD', btc_price),
                        unit='USD',
                        confidence=88,
                        data_type='aggregate',
                        category='liquidations',
                        raw_source_field='long_liquidations',
                    )
                )
        
        # 3. Short Liquidations
        if 'short_liquidations' in df.columns and len(df) > 0:
            value = df['short_liquidations'].iloc[0]
            if pd.notna(value) and value > 0:
                value_float = float(value)
                normalized_points.append(
                    NormalizedDataPoint(
                        id=self._generate_id('ALL', 'liquidations_24h_short', timestamp),
                        source=self.source_name,
                        asset='ALL',
                        metric='liquidations_24h_short',
                        timestamp=timestamp,
                        date=date,
                        value=value_float,
                        value_usd=value_float,
                        value_btc=self._convert_to_btc(value_float, 'USD', btc_price),
                        unit='USD',
                        confidence=88,
                        data_type='aggregate',
                        category='liquidations',
                        raw_source_field='short_liquidations',
                    )
                )
        
        self.logger.info(f"Transformed {len(normalized_points)} data points from Invezz")
        return normalized_points
