"""
The Block data transformer.
Transforms The Block market intelligence data into normalized data points.
"""

from typing import List
import pandas as pd

from .data_normalizer import BaseTransformer, NormalizedDataPoint


class TheBlockTransformer(BaseTransformer):
    """
    Transformer for The Block market data.
    Maps The Block fields to normalized schema.
    """
    
    def __init__(self):
        super().__init__('theblock')
    
    def transform(self, df: pd.DataFrame) -> List[NormalizedDataPoint]:
        """
        Transform The Block DataFrame to normalized data points.
        
        Expected columns vary by chart, common patterns:
        - date
        - btc_volume_7dma
        - eth_volume_7dma
        - (other chart-specific metrics)
        
        Args:
            df: The Block DataFrame
            
        Returns:
            List of NormalizedDataPoint objects
        """
        if df.empty:
            return []
        
        normalized_points = []
        
        # Get timestamp/date
        timestamp = pd.Timestamp.now()
        if 'date' in df.columns:
            # Use latest date
            timestamp = pd.to_datetime(df['date'].iloc[-1])
        elif 'timestamp' in df.columns:
            timestamp = pd.to_datetime(df['timestamp'].iloc[-1])
        
        date = timestamp.normalize()
        
        # Get BTC price if available
        btc_price = self._get_btc_price(df)
        
        # Transform common The Block metrics
        
        # BTC Volume 7DMA
        if 'btc_volume_7dma' in df.columns and len(df) > 0:
            value = df['btc_volume_7dma'].iloc[-1]
            if pd.notna(value) and value > 0:
                value_float = float(value)
                value_usd = self._convert_to_usd(value_float, 'USD', btc_price) if btc_price else value_float
                normalized_points.append(
                    NormalizedDataPoint(
                        id=self._generate_id('BTC', 'volume_7dma', timestamp),
                        source=self.source_name,
                        asset='BTC',
                        metric='volume_7dma',
                        timestamp=timestamp,
                        date=date,
                        value=value_float,
                        value_usd=value_usd,
                        value_btc=self._convert_to_btc(value_float, 'USD', btc_price) if btc_price else None,
                        unit='USD',
                        confidence=94,
                        data_type='aggregate',
                        category='volume',
                        raw_source_field='btc_volume_7dma',
                    )
                )
        
        # ETH Volume 7DMA
        if 'eth_volume_7dma' in df.columns and len(df) > 0:
            value = df['eth_volume_7dma'].iloc[-1]
            if pd.notna(value) and value > 0:
                value_float = float(value)
                value_usd = self._convert_to_usd(value_float, 'USD', btc_price) if btc_price else value_float
                normalized_points.append(
                    NormalizedDataPoint(
                        id=self._generate_id('ETH', 'volume_7dma', timestamp),
                        source=self.source_name,
                        asset='ETH',
                        metric='volume_7dma',
                        timestamp=timestamp,
                        date=date,
                        value=value_float,
                        value_usd=value_usd,
                        value_btc=self._convert_to_btc(value_float, 'USD', btc_price) if btc_price else None,
                        unit='USD',
                        confidence=94,
                        data_type='aggregate',
                        category='volume',
                        raw_source_field='eth_volume_7dma',
                    )
                )
        
        # Handle other numeric columns (generic transformation)
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        metric_cols = [col for col in numeric_cols if col not in ['date', 'timestamp', 'btc_volume_7dma', 'eth_volume_7dma']]
        
        for col in metric_cols[:5]:  # Limit to avoid too many points
            if len(df) > 0:
                value = df[col].iloc[-1]
                if pd.notna(value):
                    # Infer asset from column name
                    col_lower = col.lower()
                    asset = 'ALL'
                    if 'btc' in col_lower:
                        asset = 'BTC'
                    elif 'eth' in col_lower:
                        asset = 'ETH'
                    
                    normalized_points.append(
                        NormalizedDataPoint(
                            id=self._generate_id(asset, col, timestamp),
                            source=self.source_name,
                            asset=asset,
                            metric=col,
                            timestamp=timestamp,
                            date=date,
                            value=float(value),
                            value_usd=self._convert_to_usd(float(value), 'USD', btc_price) if btc_price else None,
                            value_btc=self._convert_to_btc(float(value), 'USD', btc_price) if btc_price else None,
                            unit='USD',
                            confidence=90,
                            data_type='aggregate',
                            category='market',
                            raw_source_field=col,
                        )
                    )
        
        self.logger.info(f"Transformed {len(normalized_points)} data points from The Block")
        return normalized_points
