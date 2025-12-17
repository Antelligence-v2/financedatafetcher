"""
Metric category mapper.
Maps metric categories (volume, liquidations, etc.) to actual metric names.
"""

from typing import List, Optional, Dict, Set
from ..utils.logger import get_logger


class MetricCategoryMapper:
    """
    Maps metric categories to actual metric names.
    Supports filtering by asset for asset-specific metrics.
    """
    
    # Category to metrics mapping
    CATEGORY_METRICS = {
        'volume': {
            'metrics': [
                'spot_volume_24h',
                'futures_volume_24h',
                'volume_7dma',
                'exchange_volume_24h_total',
                # TheBlockTransformer emits 'volume_7dma' (asset-specific via asset='BTC'/'ETH'),
                # but some older dataframes may contain explicit btc_/eth_ columns.
                'btc_volume_7dma',
                'eth_volume_7dma',
                'sol_volume',
            ],
            'description': 'Trading volume metrics (spot, futures, exchange volumes)',
        },
        'liquidations': {
            'metrics': [
                'liquidations_24h_total',
                'liquidations_24h_long',
                'liquidations_24h_short',
                'liquidations_24h',  # Asset-specific (BTC, ETH, etc.)
            ],
            'description': 'Liquidation metrics (total, long, short, per-asset)',
        },
        'derivatives': {
            'metrics': [
                'open_interest',
                'futures_oi_all_exchanges',
                'futures_oi',
                'options_oi',
                'cme_btc_oi',
                'binance_btc_oi',
                'btc_options_calls_oi',
                'btc_options_puts_oi',
            ],
            'description': 'Derivatives metrics (open interest, futures, options)',
        },
        'flows': {
            'metrics': [
                'net_inflow_24h',
                'net_inflow_5min',
                'net_inflow_1h',
                'net_inflow_4h',
                'net_inflow_12h',
                'spot_inflow',
                'spot_outflow',
            ],
            'description': 'Flow metrics (inflows, outflows, net flows)',
        },
        'market': {
            'metrics': [
                'btc_price',
                'eth_price',
                'sol_price',
                'market_cap',
                'price',
            ],
            'description': 'Market data (prices, market caps)',
        },
        'trust': {
            'metrics': [
                'avg_trust_score',
                'trust_score',
                'trust_rank',
            ],
            'description': 'Trust and reputation metrics',
        },
        'staking': {
            'metrics': [
                'total_eth_deposited',
                'total_validators',
                'distinct_depositor_addresses',
                'staking_deposits',
                'validators',
            ],
            'description': 'Staking metrics (ETH staking, validators, deposits)',
        },
    }
    
    # Asset-specific metric filters
    ASSET_SPECIFIC_METRICS = {
        'BTC': {
            'include': ['btc_price', 'btc_volume_7dma', 'btc_liquidations_24h', 'cme_btc_oi', 'binance_btc_oi', 'btc_options_calls_oi', 'btc_options_puts_oi'],
            'exclude': ['eth_price', 'total_eth_deposited', 'total_validators'],
        },
        'ETH': {
            'include': ['eth_price', 'eth_volume_7dma', 'eth_liquidations_24h', 'total_eth_deposited', 'total_validators', 'distinct_depositor_addresses'],
            'exclude': ['btc_price', 'btc_volume_7dma', 'btc_liquidations_24h'],
        },
        'SOL': {
            'include': ['sol_price', 'sol_volume'],
            'exclude': ['btc_price', 'eth_price', 'total_eth_deposited'],
        },
        'ALL': {
            'include': [],  # Include all metrics
            'exclude': [],  # Exclude none
        },
    }
    
    def __init__(self):
        """Initialize metric category mapper."""
        self.logger = get_logger()
    
    def get_available_categories(self) -> List[str]:
        """
        Get list of all available metric categories.
        
        Returns:
            List of category names
        """
        return sorted(list(self.CATEGORY_METRICS.keys()))
    
    def get_category_description(self, category: str) -> str:
        """
        Get human-readable description for a category.
        
        Args:
            category: Category name
            
        Returns:
            Description string
        """
        category_info = self.CATEGORY_METRICS.get(category.lower())
        if category_info:
            return category_info['description']
        return f"Metrics in the {category} category"
    
    def get_metrics_for_categories(
        self,
        categories: List[str],
        asset: Optional[str] = None
    ) -> List[str]:
        """
        Get list of metrics that belong to the selected categories.
        
        Args:
            categories: List of category names (e.g., ['volume', 'liquidations'])
            asset: Optional asset name to filter asset-specific metrics
            
        Returns:
            List of metric names
        """
        if not categories:
            return []
        
        all_metrics: Set[str] = set()
        
        # Collect metrics from all selected categories
        for category in categories:
            category_lower = category.lower()
            if category_lower in self.CATEGORY_METRICS:
                category_metrics = self.CATEGORY_METRICS[category_lower]['metrics']
                all_metrics.update(category_metrics)
            else:
                self.logger.warning(f"Unknown category: {category}")
        
        # Apply asset-specific filtering if asset is provided
        if asset:
            asset_upper = asset.upper()
            asset_filter = self.ASSET_SPECIFIC_METRICS.get(asset_upper)
            
            if asset_filter:
                # Include asset-specific metrics
                if asset_filter['include']:
                    all_metrics.update(asset_filter['include'])
                
                # Exclude metrics not relevant to this asset
                if asset_filter['exclude']:
                    all_metrics = all_metrics - set(asset_filter['exclude'])
            else:
                # For unknown assets, try to filter based on asset name in metric
                asset_lower = asset_upper.lower()
                # Keep metrics that contain asset name or are general
                filtered_metrics = set()
                for metric in all_metrics:
                    if asset_lower in metric.lower() or metric.startswith(('spot_', 'futures_', 'total_', 'net_', 'open_', 'exchange_')):
                        filtered_metrics.add(metric)
                all_metrics = filtered_metrics
        
        return sorted(list(all_metrics))
    
    def get_category_for_metric(self, metric: str) -> Optional[str]:
        """
        Get the category for a specific metric.
        
        Args:
            metric: Metric name
            
        Returns:
            Category name or None if not found
        """
        metric_lower = metric.lower()
        
        for category, category_info in self.CATEGORY_METRICS.items():
            if metric_lower in [m.lower() for m in category_info['metrics']]:
                return category
        
        # Try partial matching
        if 'volume' in metric_lower:
            return 'volume'
        elif 'liquidation' in metric_lower:
            return 'liquidations'
        elif 'interest' in metric_lower or 'oi' in metric_lower or 'options' in metric_lower:
            return 'derivatives'
        elif 'flow' in metric_lower or 'inflow' in metric_lower or 'outflow' in metric_lower:
            return 'flows'
        elif 'price' in metric_lower or 'market_cap' in metric_lower:
            return 'market'
        elif 'trust' in metric_lower:
            return 'trust'
        elif 'staking' in metric_lower or 'stake' in metric_lower or 'validator' in metric_lower or 'deposit' in metric_lower:
            return 'staking'
        
        return None
    
    def get_metrics_info(
        self,
        categories: List[str],
        asset: Optional[str] = None
    ) -> Dict[str, any]:
        """
        Get detailed information about metrics for categories.
        
        Args:
            categories: List of category names
            asset: Optional asset name
            
        Returns:
            Dictionary with metrics info
        """
        metrics = self.get_metrics_for_categories(categories, asset)
        
        # Group metrics by category
        metrics_by_category: Dict[str, List[str]] = {}
        for metric in metrics:
            category = self.get_category_for_metric(metric)
            if category:
                metrics_by_category.setdefault(category, []).append(metric)
            else:
                metrics_by_category.setdefault('other', []).append(metric)
        
        return {
            'categories': categories,
            'asset': asset,
            'total_metrics': len(metrics),
            'metrics': metrics,
            'metrics_by_category': metrics_by_category,
        }

