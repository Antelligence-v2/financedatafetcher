"""
Unified data normalization schema and base transformer.
Defines the common data model for all sources.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Union
import pandas as pd
from datetime import datetime

from ..utils.logger import get_logger


@dataclass
class NormalizedDataPoint:
    """
    Unified data point schema for all sources.
    All scraped data is transformed into this format.
    """
    id: str  # Unique: {source}_{asset}_{metric}_{timestamp}
    source: str  # 'coinglass', 'coingecko', 'dune', 'theblock', 'invezz', 'bitcoin_com'
    asset: str  # 'BTC', 'ETH', 'SOL', 'ALL', 'EXCHANGES'
    metric: str  # 'spot_volume_24h', 'liquidations_24h', 'open_interest', etc.
    timestamp: pd.Timestamp  # Exact timestamp when data was captured
    date: pd.Timestamp  # Date only (for daily grouping)
    value: float  # Raw value
    value_usd: Optional[float] = None  # USD-normalized value
    value_btc: Optional[float] = None  # BTC-normalized value
    unit: str = 'USD'  # 'USD', 'BTC', 'ETH', etc.
    confidence: int = 90  # 0-100 quality score
    data_type: str = 'aggregate'  # 'aggregate', 'per_asset', 'per_exchange', 'snapshot'
    category: str = 'market'  # 'volume', 'liquidations', 'derivatives', 'flows', 'market', 'trust'
    raw_source_field: str = ''  # Original field name in source
    metadata: Dict[str, Any] = field(default_factory=dict)  # Additional context
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for DataFrame creation."""
        return {
            'id': self.id,
            'source': self.source,
            'asset': self.asset,
            'metric': self.metric,
            'timestamp': self.timestamp,
            'date': self.date,
            'value': self.value,
            'value_usd': self.value_usd,
            'value_btc': self.value_btc,
            'unit': self.unit,
            'confidence': self.confidence,
            'data_type': self.data_type,
            'category': self.category,
            'raw_source_field': self.raw_source_field,
            'metadata': self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NormalizedDataPoint':
        """Create from dictionary."""
        # Handle timestamp conversion
        if isinstance(data.get('timestamp'), str):
            data['timestamp'] = pd.to_datetime(data['timestamp'])
        if isinstance(data.get('date'), str):
            data['date'] = pd.to_datetime(data['date']).date()
            data['date'] = pd.Timestamp(data['date'])
        
        return cls(**data)


class BaseTransformer:
    """
    Base class for source-specific transformers.
    Each source (Coinglass, CoinGecko, etc.) has its own transformer.
    """
    
    def __init__(self, source_name: str):
        """
        Initialize transformer.
        
        Args:
            source_name: Name of the data source (e.g., 'coinglass')
        """
        self.source_name = source_name
        self.logger = get_logger()
    
    def transform(self, df: pd.DataFrame) -> List[NormalizedDataPoint]:
        """
        Transform a DataFrame from the source into normalized data points.
        
        Args:
            df: Source DataFrame with raw data
            
        Returns:
            List of NormalizedDataPoint objects
        """
        raise NotImplementedError("Subclasses must implement transform()")
    
    def _generate_id(
        self,
        asset: str,
        metric: str,
        timestamp: pd.Timestamp
    ) -> str:
        """
        Generate unique ID for a data point.
        
        Args:
            asset: Asset identifier
            metric: Metric name
            timestamp: Timestamp
            
        Returns:
            Unique ID string
        """
        ts_str = timestamp.strftime('%Y%m%d_%H%M%S')
        return f"{self.source_name}_{asset}_{metric}_{ts_str}"
    
    def _get_btc_price(self, df: pd.DataFrame) -> Optional[float]:
        """
        Extract BTC price from DataFrame if available.
        
        Args:
            df: Source DataFrame
            
        Returns:
            BTC price in USD or None
        """
        # Try common column names
        btc_price_cols = ['btc_price', 'BTC_price', 'btcPrice', 'price_btc', 'BTC']
        for col in btc_price_cols:
            if col in df.columns and len(df) > 0:
                value = df[col].iloc[0]
                if pd.notna(value) and value > 0:
                    return float(value)
        return None
    
    def _convert_to_usd(
        self,
        value: float,
        unit: str,
        btc_price: Optional[float] = None
    ) -> Optional[float]:
        """
        Convert value to USD.
        
        Args:
            value: Value to convert
            unit: Current unit ('USD', 'BTC', 'ETH', etc.)
            btc_price: BTC price in USD (for BTC conversions)
            
        Returns:
            Value in USD or None if conversion not possible
        """
        if unit == 'USD':
            return value
        elif unit == 'BTC' and btc_price:
            return value * btc_price
        elif unit == 'ETH' and btc_price:
            # Approximate ETH price as fraction of BTC (rough estimate)
            # In production, would fetch actual ETH price
            eth_price = btc_price * 0.05  # Rough estimate
            return value * eth_price
        return None
    
    def _convert_to_btc(
        self,
        value: float,
        unit: str,
        btc_price: Optional[float] = None
    ) -> Optional[float]:
        """
        Convert value to BTC.
        
        Args:
            value: Value to convert
            unit: Current unit ('USD', 'BTC', 'ETH', etc.)
            btc_price: BTC price in USD (for USD conversions)
            
        Returns:
            Value in BTC or None if conversion not possible
        """
        if unit == 'BTC':
            return value
        elif unit == 'USD' and btc_price:
            return value / btc_price
        return None
