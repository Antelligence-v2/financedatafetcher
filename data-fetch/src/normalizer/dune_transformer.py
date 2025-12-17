"""
DUNE Analytics data transformer.
Transforms DUNE on-chain data into normalized data points.
"""

from typing import List
import pandas as pd

from .data_normalizer import BaseTransformer, NormalizedDataPoint


class DuneTransformer(BaseTransformer):
    """
    Transformer for DUNE Analytics on-chain data.
    Maps DUNE query results to normalized schema.
    """
    
    def __init__(self):
        super().__init__('dune')
    
    def transform(self, df: pd.DataFrame) -> List[NormalizedDataPoint]:
        """
        Transform DUNE DataFrame to normalized data points.
        
        DUNE data structure varies by query, so this is a flexible transformer
        that attempts to identify common patterns.
        
        Args:
            df: DUNE DataFrame (structure varies by query)
            
        Returns:
            List of NormalizedDataPoint objects
        """
        if df.empty:
            return []
        
        normalized_points = []
        
        # Get timestamp (try common column names)
        timestamp = pd.Timestamp.now()
        if 'timestamp' in df.columns:
            timestamp = pd.to_datetime(df['timestamp'].iloc[0])
        elif 'date' in df.columns:
            timestamp = pd.to_datetime(df['date'].iloc[0])
        elif 'time' in df.columns:
            timestamp = pd.to_datetime(df['time'].iloc[0])
        
        date = timestamp.normalize()
        
        # Get BTC price if available
        btc_price = self._get_btc_price(df)
        
        # DUNE queries have varying structures
        # For now, create a basic transformation that handles common patterns
        # This can be extended for specific DUNE queries
        
        # Look for common metric patterns
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        
        # Exclude timestamp/date columns
        metric_cols = [col for col in numeric_cols if col not in ['timestamp', 'date', 'time', 'block_number', 'block']]
        
        # For each numeric column, create a data point
        for col in metric_cols[:10]:  # Limit to first 10 to avoid too many points
            if len(df) > 0:
                # Use latest value or sum/avg depending on context
                value = df[col].iloc[-1] if len(df) > 0 else df[col].sum()
                
                if pd.notna(value):
                    # Try to infer asset and category from column name
                    col_lower = col.lower()
                    asset = 'ALL'
                    category = 'market'
                    
                    if 'btc' in col_lower or 'bitcoin' in col_lower:
                        asset = 'BTC'
                    elif 'eth' in col_lower or 'ethereum' in col_lower:
                        asset = 'ETH'
                    elif 'sol' in col_lower or 'solana' in col_lower:
                        asset = 'SOL'
                    
                    if 'volume' in col_lower:
                        category = 'volume'
                    elif 'liquidation' in col_lower:
                        category = 'liquidations'
                    elif 'flow' in col_lower:
                        category = 'flows'
                    elif 'staking' in col_lower or 'stake' in col_lower:
                        category = 'staking'
                    
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
                            confidence=85,  # DUNE data quality varies
                            data_type='aggregate',
                            category=category,
                            raw_source_field=col,
                        )
                    )
        
        self.logger.info(f"Transformed {len(normalized_points)} data points from DUNE")
        return normalized_points
