"""
Coinglass data transformer.
Transforms Coinglass scraped data into normalized data points.
"""

from typing import List
import pandas as pd

from .data_normalizer import BaseTransformer, NormalizedDataPoint


class CoinglassTransformer(BaseTransformer):
    """
    Transformer for Coinglass data.
    Maps Coinglass fields to normalized schema.
    """
    
    def __init__(self):
        super().__init__('coinglass')
    
    def transform(self, df: pd.DataFrame) -> List[NormalizedDataPoint]:
        """
        Transform Coinglass DataFrame to normalized data points.
        
        Expected columns:
        - timestamp
        - btc_price
        - spot_volume_24h
        - futures_volume_24h
        - open_interest
        - net_inflow_24h
        - (optional) futures_oi_all_exchanges
        - (optional) total_liquidations_24h
        - (optional) long_liquidations
        - (optional) short_liquidations
        - (optional) btc_liquidations_24h
        - (optional) eth_liquidations_24h
        
        Args:
            df: Coinglass DataFrame
            
        Returns:
            List of NormalizedDataPoint objects
        """
        if df.empty:
            return []
        
        normalized_points = []
        
        # Get timestamp (assume first row if multiple)
        timestamp = pd.to_datetime(df['timestamp'].iloc[0]) if 'timestamp' in df.columns else pd.Timestamp.now()
        date = timestamp.normalize()  # Date only
        
        # Get BTC price for conversions
        btc_price = self._get_btc_price(df)
        
        # Transform each metric
        # 1. BTC Price
        if 'btc_price' in df.columns and len(df) > 0:
            value = df['btc_price'].iloc[0]
            if pd.notna(value) and value > 0:
                normalized_points.append(
                    NormalizedDataPoint(
                        id=self._generate_id('BTC', 'btc_price', timestamp),
                        source=self.source_name,
                        asset='BTC',
                        metric='btc_price',
                        timestamp=timestamp,
                        date=date,
                        value=float(value),
                        value_usd=float(value),
                        value_btc=1.0,
                        unit='USD',
                        confidence=98,
                        data_type='snapshot',
                        category='market',
                        raw_source_field='btc_price',
                    )
                )
        
        # 2. Spot Volume 24h
        if 'spot_volume_24h' in df.columns and len(df) > 0:
            value = df['spot_volume_24h'].iloc[0]
            if pd.notna(value):
                value_float = float(value)
                value_usd = self._convert_to_usd(value_float, 'BTC', btc_price)
                normalized_points.append(
                    NormalizedDataPoint(
                        id=self._generate_id('ALL', 'spot_volume_24h', timestamp),
                        source=self.source_name,
                        asset='ALL',
                        metric='spot_volume_24h',
                        timestamp=timestamp,
                        date=date,
                        value=value_float,
                        value_usd=value_usd,
                        value_btc=value_float if value_float > 0 else None,
                        unit='BTC',
                        confidence=95,
                        data_type='aggregate',
                        category='volume',
                        raw_source_field='spot_volume_24h',
                    )
                )
        
        # 3. Futures Volume 24h
        if 'futures_volume_24h' in df.columns and len(df) > 0:
            value = df['futures_volume_24h'].iloc[0]
            if pd.notna(value):
                value_float = float(value)
                value_usd = self._convert_to_usd(value_float, 'BTC', btc_price)
                normalized_points.append(
                    NormalizedDataPoint(
                        id=self._generate_id('ALL', 'futures_volume_24h', timestamp),
                        source=self.source_name,
                        asset='ALL',
                        metric='futures_volume_24h',
                        timestamp=timestamp,
                        date=date,
                        value=value_float,
                        value_usd=value_usd,
                        value_btc=value_float if value_float > 0 else None,
                        unit='BTC',
                        confidence=93,
                        data_type='aggregate',
                        category='volume',
                        raw_source_field='futures_volume_24h',
                    )
                )
        
        # 4. Open Interest
        if 'open_interest' in df.columns and len(df) > 0:
            value = df['open_interest'].iloc[0]
            if pd.notna(value) and value > 0:
                value_float = float(value)
                value_usd = self._convert_to_usd(value_float, 'BTC', btc_price)
                normalized_points.append(
                    NormalizedDataPoint(
                        id=self._generate_id('ALL', 'open_interest', timestamp),
                        source=self.source_name,
                        asset='ALL',
                        metric='open_interest',
                        timestamp=timestamp,
                        date=date,
                        value=value_float,
                        value_usd=value_usd,
                        value_btc=value_float,
                        unit='BTC',
                        confidence=92,
                        data_type='snapshot',
                        category='derivatives',
                        raw_source_field='open_interest',
                    )
                )
        
        # 5. Net Inflow 24h
        if 'net_inflow_24h' in df.columns and len(df) > 0:
            value = df['net_inflow_24h'].iloc[0]
            if pd.notna(value):
                value_float = float(value)
                value_usd = self._convert_to_usd(value_float, 'BTC', btc_price)
                normalized_points.append(
                    NormalizedDataPoint(
                        id=self._generate_id('ALL', 'net_inflow_24h', timestamp),
                        source=self.source_name,
                        asset='ALL',
                        metric='net_inflow_24h',
                        timestamp=timestamp,
                        date=date,
                        value=value_float,
                        value_usd=value_usd,
                        value_btc=value_float if value_float != 0 else None,
                        unit='BTC',
                        confidence=90,
                        data_type='aggregate',
                        category='flows',
                        raw_source_field='net_inflow_24h',
                    )
                )
        
        # 6. Futures OI All Exchanges (if available)
        if 'futures_oi_all_exchanges' in df.columns and len(df) > 0:
            value = df['futures_oi_all_exchanges'].iloc[0]
            if pd.notna(value) and value > 0:
                value_float = float(value)
                # Assume already in USD (billions)
                normalized_points.append(
                    NormalizedDataPoint(
                        id=self._generate_id('ALL', 'futures_oi_all_exchanges', timestamp),
                        source=self.source_name,
                        asset='ALL',
                        metric='futures_oi_all_exchanges',
                        timestamp=timestamp,
                        date=date,
                        value=value_float,
                        value_usd=value_float,
                        value_btc=self._convert_to_btc(value_float, 'USD', btc_price),
                        unit='USD',
                        confidence=98,
                        data_type='snapshot',
                        category='derivatives',
                        raw_source_field='futures_oi_all_exchanges',
                    )
                )
        
        # 7. Total Liquidations 24h (if available)
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
                        confidence=96,
                        data_type='aggregate',
                        category='liquidations',
                        raw_source_field='total_liquidations_24h',
                    )
                )
        
        # 8. Long Liquidations (if available)
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
        
        # 9. Short Liquidations (if available)
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
        
        # 10. BTC Liquidations (if available)
        if 'btc_liquidations_24h' in df.columns and len(df) > 0:
            value = df['btc_liquidations_24h'].iloc[0]
            if pd.notna(value) and value > 0:
                value_float = float(value)
                normalized_points.append(
                    NormalizedDataPoint(
                        id=self._generate_id('BTC', 'liquidations_24h', timestamp),
                        source=self.source_name,
                        asset='BTC',
                        metric='liquidations_24h',
                        timestamp=timestamp,
                        date=date,
                        value=value_float,
                        value_usd=value_float,
                        value_btc=self._convert_to_btc(value_float, 'USD', btc_price),
                        unit='USD',
                        confidence=96,
                        data_type='per_asset',
                        category='liquidations',
                        raw_source_field='btc_liquidations_24h',
                    )
                )
        
        # 11. ETH Liquidations (if available)
        if 'eth_liquidations_24h' in df.columns and len(df) > 0:
            value = df['eth_liquidations_24h'].iloc[0]
            if pd.notna(value) and value > 0:
                value_float = float(value)
                normalized_points.append(
                    NormalizedDataPoint(
                        id=self._generate_id('ETH', 'liquidations_24h', timestamp),
                        source=self.source_name,
                        asset='ETH',
                        metric='liquidations_24h',
                        timestamp=timestamp,
                        date=date,
                        value=value_float,
                        value_usd=value_float,
                        value_btc=self._convert_to_btc(value_float, 'USD', btc_price),
                        unit='USD',
                        confidence=94,
                        data_type='per_asset',
                        category='liquidations',
                        raw_source_field='eth_liquidations_24h',
                    )
                )
        
        self.logger.info(f"Transformed {len(normalized_points)} data points from Coinglass")
        return normalized_points
