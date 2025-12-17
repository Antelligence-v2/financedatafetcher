"""
CoinGecko data transformer.
Transforms CoinGecko exchange volume data into normalized data points.
"""

from typing import List
import pandas as pd

from .data_normalizer import BaseTransformer, NormalizedDataPoint


class CoinGeckoTransformer(BaseTransformer):
    """
    Transformer for CoinGecko exchange volume data.
    Maps CoinGecko fields to normalized schema.
    """
    
    def __init__(self):
        super().__init__('coingecko')
    
    def transform(self, df: pd.DataFrame) -> List[NormalizedDataPoint]:
        """
        Transform CoinGecko DataFrame to normalized data points.
        
        Expected columns:
        - id (exchange_id)
        - name (exchange_name)
        - country
        - trust_score
        - trust_score_rank
        - trade_volume_24h_btc
        - (optional) timestamp
        
        Args:
            df: CoinGecko DataFrame
            
        Returns:
            List of NormalizedDataPoint objects
        """
        if df.empty:
            return []
        
        normalized_points = []
        
        # Get timestamp (use current time if not available)
        if 'timestamp' in df.columns:
            timestamp = pd.to_datetime(df['timestamp'].iloc[0])
        else:
            timestamp = pd.Timestamp.now()
        date = timestamp.normalize()
        
        # Get BTC price for conversions (if available)
        btc_price = self._get_btc_price(df)
        
        # CoinGecko data is per-exchange, so we'll create data points for each exchange
        # Also create aggregate metrics
        
        # Aggregate: Total exchange volume
        if 'trade_volume_24h_btc' in df.columns:
            total_volume_btc = df['trade_volume_24h_btc'].sum()
            if total_volume_btc > 0:
                total_volume_usd = self._convert_to_usd(total_volume_btc, 'BTC', btc_price)
                normalized_points.append(
                    NormalizedDataPoint(
                        id=self._generate_id('EXCHANGES', 'exchange_volume_24h_total', timestamp),
                        source=self.source_name,
                        asset='EXCHANGES',
                        metric='exchange_volume_24h_total',
                        timestamp=timestamp,
                        date=date,
                        value=total_volume_btc,
                        value_usd=total_volume_usd,
                        value_btc=total_volume_btc,
                        unit='BTC',
                        confidence=92,
                        data_type='aggregate',
                        category='volume',
                        raw_source_field='trade_volume_24h_btc',
                        metadata={
                            'exchange_count': len(df),
                            'top_exchanges': df.nlargest(5, 'trade_volume_24h_btc')[['id', 'name', 'trade_volume_24h_btc']].to_dict('records') if len(df) > 0 else []
                        }
                    )
                )
        
        # Average trust score
        if 'trust_score' in df.columns:
            avg_trust_score = df['trust_score'].mean()
            if pd.notna(avg_trust_score):
                normalized_points.append(
                    NormalizedDataPoint(
                        id=self._generate_id('EXCHANGES', 'avg_trust_score', timestamp),
                        source=self.source_name,
                        asset='EXCHANGES',
                        metric='avg_trust_score',
                        timestamp=timestamp,
                        date=date,
                        value=float(avg_trust_score),
                        value_usd=None,
                        value_btc=None,
                        unit='SCORE',
                        confidence=95,
                        data_type='aggregate',
                        category='trust',
                        raw_source_field='trust_score',
                        metadata={
                            'exchange_count': len(df),
                        }
                    )
                )
        
        self.logger.info(f"Transformed {len(normalized_points)} data points from CoinGecko ({len(df)} exchanges)")
        return normalized_points
