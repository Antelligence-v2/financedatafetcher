"""
Asset to site mapping service.
Maps asset names (BTC, ETH, etc.) to configured site IDs from websites.yaml.
"""

from typing import List, Dict, Set
from pathlib import Path
import sys
import os
import json

# #region agent log
_log_path = Path(__file__).parent.parent.parent.parent / ".cursor" / "debug.log"
try:
    _parent_path = Path(__file__).parent.parent.parent.parent
    _config_path = _parent_path / "src" / "utils" / "config_manager.py"
    with open(_log_path, "a") as f:
        f.write(json.dumps({"location": "asset_mapper.py:12", "message": "Import path setup", "data": {"file": str(__file__), "parent_path": str(_parent_path), "config_exists": _config_path.exists(), "sys_path_before": sys.path[:5]}, "timestamp": int(os.path.getmtime(__file__) * 1000) if os.path.exists(__file__) else 0, "sessionId": "debug-session", "runId": "run1", "hypothesisId": "A"}) + "\n")
except: pass
# #endregion

# Add root directory to path to import config_manager
# This ensures root/src is found before data-fetch/src
_root_path = Path(__file__).parent.parent.parent.parent
_root_path_str = str(_root_path)
if _root_path_str not in sys.path:
    sys.path.insert(0, _root_path_str)
elif sys.path.index(_root_path_str) > 0:
    # Move to front if already in path but not first
    sys.path.remove(_root_path_str)
    sys.path.insert(0, _root_path_str)

# Ensure src.utils package points to root location
# If src.utils exists but points to data-fetch, we need to fix it
import types
_root_utils_path = _root_path / "src" / "utils"
_data_fetch_utils_path = _root_path / "data-fetch" / "src" / "utils"

# Check if src.utils exists and is from wrong location
if 'src.utils' in sys.modules:
    _src_utils = sys.modules['src.utils']
    _src_utils_paths = getattr(_src_utils, '__path__', [])
    # If it's pointing to data-fetch, recreate it to point to root
    if _src_utils_paths and any('data-fetch' in str(p) for p in _src_utils_paths):
        # Recreate src.utils to point to root
        _new_src_utils = types.ModuleType('src.utils')
        _new_src_utils.__path__ = [str(_root_utils_path)]
        _new_src_utils.__package__ = 'src.utils'
        sys.modules['src.utils'] = _new_src_utils
        # Clear config_manager if it was loaded from wrong location
        if 'src.utils.config_manager' in sys.modules:
            _cm_mod = sys.modules['src.utils.config_manager']
            _cm_file = getattr(_cm_mod, '__file__', '')
            if 'data-fetch' in str(_cm_file):
                del sys.modules['src.utils.config_manager']
elif _root_utils_path.exists():
    # Create src.utils package if it doesn't exist
    _new_src_utils = types.ModuleType('src.utils')
    _new_src_utils.__path__ = [str(_root_utils_path)]
    _new_src_utils.__package__ = 'src.utils'
    sys.modules['src.utils'] = _new_src_utils

# Ensure src package exists and points to root
if 'src' not in sys.modules:
    _new_src = types.ModuleType('src')
    _new_src.__path__ = [str(_root_path / "src")]
    _new_src.__package__ = 'src'
    sys.modules['src'] = _new_src
elif hasattr(sys.modules['src'], '__path__'):
    _src_paths = sys.modules['src'].__path__
    # If src is pointing to data-fetch, add root to path or recreate
    if all('data-fetch' in str(p) for p in _src_paths):
        # Add root src to path
        _src_paths.insert(0, str(_root_path / "src"))

# #region agent log
try:
    with open(_log_path, "a") as f:
        f.write(json.dumps({"location": "asset_mapper.py:45", "message": "After path insert", "data": {"sys_path_after": sys.path[:5], "root_in_path": _root_path_str in sys.path, "root_position": sys.path.index(_root_path_str) if _root_path_str in sys.path else -1, "cleared_modules": _modules_to_clear, "src_utils_exists": "src.utils" in sys.modules, "src_utils_config_exists": "src.utils.config_manager" in sys.modules}, "timestamp": int(os.path.getmtime(__file__) * 1000) if os.path.exists(__file__) else 0, "sessionId": "debug-session", "runId": "run4", "hypothesisId": "B"}) + "\n")
except: pass
# #endregion

# Import ConfigManager - root is now first in sys.path so it should find root/src/utils/config_manager
try:
    from src.utils.config_manager import ConfigManager
    # #region agent log
    try:
        with open(_log_path, "a") as f:
            f.write(json.dumps({"location": "asset_mapper.py:50", "message": "ConfigManager import success", "data": {"module": str(ConfigManager)}, "timestamp": int(os.path.getmtime(__file__) * 1000) if os.path.exists(__file__) else 0, "sessionId": "debug-session", "runId": "run3", "hypothesisId": "C"}) + "\n")
    except: pass
    # #endregion
except ImportError as e:
    # #region agent log
    try:
        with open(_log_path, "a") as f:
            f.write(json.dumps({"location": "asset_mapper.py:55", "message": "ConfigManager import failed", "data": {"error": str(e), "sys_path": sys.path[:5], "root_path": _root_path_str, "config_path": str(_root_path / "src" / "utils" / "config_manager.py"), "src_modules": [m for m in sys.modules.keys() if m.startswith("src.")][:5]}, "timestamp": int(os.path.getmtime(__file__) * 1000) if os.path.exists(__file__) else 0, "sessionId": "debug-session", "runId": "run3", "hypothesisId": "D"}) + "\n")
    except: pass
    # #endregion
    raise

from ..utils.logger import get_logger


class AssetMapper:
    """
    Maps asset names to site IDs that provide data for those assets.
    """
    
    # Asset name mappings (normalized names)
    ASSET_ALIASES = {
        'BTC': ['bitcoin', 'btc', 'BTC'],
        'ETH': ['ethereum', 'eth', 'ETH'],
        'SOL': ['solana', 'sol', 'SOL'],
        'ALL': ['all', 'ALL', 'aggregate', 'combined'],
        'EXCHANGES': ['exchanges', 'EXCHANGES', 'exchange'],
    }
    
    def __init__(self, config_manager: ConfigManager = None):
        """
        Initialize asset mapper.
        
        Args:
            config_manager: ConfigManager instance (creates new if not provided)
        """
        self.config_manager = config_manager or ConfigManager()
        self.logger = get_logger()
        self._asset_to_sites_cache: Dict[str, List[str]] = {}
        self._build_mapping()
    
    def _build_mapping(self):
        """Build asset to site ID mapping from configuration."""
        sites = self.config_manager.list_sites()
        
        # Initialize mapping
        asset_sites: Dict[str, Set[str]] = {}
        
        for site in sites:
            site_id = site.get('id', '')
            site_name = site.get('name', '').lower()
            site_id_lower = site_id.lower()
            
            # Check which assets this site provides data for
            # Based on site ID, name, and metadata
            
            # BTC-related sites
            if any(keyword in site_id_lower or keyword in site_name 
                   for keyword in ['btc', 'bitcoin', 'theblock_btc']):
                asset_sites.setdefault('BTC', set()).add(site_id)
            
            # ETH-related sites
            if any(keyword in site_id_lower or keyword in site_name 
                   for keyword in ['eth', 'ethereum', 'staking', 'dune_eth']):
                asset_sites.setdefault('ETH', set()).add(site_id)
            
            # SOL-related sites
            if any(keyword in site_id_lower or keyword in site_name 
                   for keyword in ['sol', 'solana']):
                asset_sites.setdefault('SOL', set()).add(site_id)
            
            # Aggregate/ALL sites (exchange volumes, combined metrics)
            if any(keyword in site_id_lower or keyword in site_name 
                   for keyword in ['exchange_volume', 'total', 'combined', 'all', 
                                  'coingecko_exchange', 'theblock_exchange']):
                asset_sites.setdefault('ALL', set()).add(site_id)
            
            # CoinGecko sites (usually support multiple assets)
            if 'coingecko' in site_id_lower:
                # CoinGecko BTC market chart
                if 'btc' in site_id_lower or 'bitcoin' in site_id_lower:
                    asset_sites.setdefault('BTC', set()).add(site_id)
                # CoinGecko exchange volume (aggregate)
                elif 'exchange' in site_id_lower:
                    asset_sites.setdefault('ALL', set()).add(site_id)
                    asset_sites.setdefault('BTC', set()).add(site_id)  # Also BTC-related
                else:
                    # Generic CoinGecko - assume supports multiple assets
                    asset_sites.setdefault('ALL', set()).add(site_id)
            
            # CoinGlass sites (usually BTC-focused but can have other assets)
            if 'coinglass' in site_id_lower:
                asset_sites.setdefault('BTC', set()).add(site_id)
                asset_sites.setdefault('ALL', set()).add(site_id)  # Also aggregate metrics
            
            # The Block sites
            if 'theblock' in site_id_lower:
                if 'btc' in site_id_lower or 'eth' in site_id_lower:
                    if 'btc' in site_id_lower:
                        asset_sites.setdefault('BTC', set()).add(site_id)
                    if 'eth' in site_id_lower:
                        asset_sites.setdefault('ETH', set()).add(site_id)
                    # Combined BTC/ETH sites
                    if 'btc' in site_id_lower and 'eth' in site_id_lower:
                        asset_sites.setdefault('ALL', set()).add(site_id)
                elif 'exchange' in site_id_lower or 'total' in site_id_lower:
                    asset_sites.setdefault('ALL', set()).add(site_id)
                    asset_sites.setdefault('BTC', set()).add(site_id)
                    asset_sites.setdefault('ETH', set()).add(site_id)
                    if 'sol' in site_id_lower:
                        asset_sites.setdefault('SOL', set()).add(site_id)
            
            # Dune sites (usually ETH-focused for staking)
            if 'dune' in site_id_lower:
                if 'eth' in site_id_lower or 'staking' in site_id_lower:
                    asset_sites.setdefault('ETH', set()).add(site_id)
                else:
                    asset_sites.setdefault('ALL', set()).add(site_id)
        
        # Convert sets to lists and cache
        for asset, site_set in asset_sites.items():
            self._asset_to_sites_cache[asset] = sorted(list(site_set))
        
        self.logger.debug(f"Built asset mapping: {len(self._asset_to_sites_cache)} assets mapped to sites")
    
    def normalize_asset_name(self, asset: str) -> str:
        """
        Normalize asset name to standard form (BTC, ETH, SOL, ALL, etc.).
        
        Args:
            asset: User-provided asset name (e.g., "bitcoin", "BTC", "btc")
            
        Returns:
            Normalized asset name (e.g., "BTC")
        """
        asset_lower = asset.strip().lower()
        
        # Check aliases
        for normalized, aliases in self.ASSET_ALIASES.items():
            if asset_lower in aliases:
                return normalized
        
        # If not found in aliases, try direct match (case-insensitive)
        if asset_lower.upper() in self._asset_to_sites_cache:
            return asset_lower.upper()
        
        # Default: return uppercase version
        return asset.strip().upper()
    
    def get_sites_for_asset(self, asset: str) -> List[str]:
        """
        Get list of site IDs that provide data for the given asset.
        
        Args:
            asset: Asset name (e.g., "BTC", "bitcoin", "ETH")
            
        Returns:
            List of site IDs
        """
        normalized_asset = self.normalize_asset_name(asset)
        
        # Get sites for this asset
        sites = self._asset_to_sites_cache.get(normalized_asset, [])
        
        # If asset is not found, try to find similar
        if not sites and normalized_asset not in self._asset_to_sites_cache:
            self.logger.warning(f"Asset '{asset}' (normalized: '{normalized_asset}') not found in mapping")
            # Try to find partial matches
            asset_lower = normalized_asset.lower()
            for cached_asset, cached_sites in self._asset_to_sites_cache.items():
                if asset_lower in cached_asset.lower() or cached_asset.lower() in asset_lower:
                    self.logger.info(f"Found partial match: '{asset}' -> '{cached_asset}'")
                    return cached_sites
        
        return sites
    
    def get_available_assets(self) -> List[str]:
        """
        Get list of all available assets.
        
        Returns:
            List of normalized asset names
        """
        return sorted(list(self._asset_to_sites_cache.keys()))
    
    def get_asset_info(self, asset: str) -> Dict[str, any]:
        """
        Get information about an asset including available sites.
        
        Args:
            asset: Asset name
            
        Returns:
            Dictionary with asset info
        """
        normalized = self.normalize_asset_name(asset)
        sites = self.get_sites_for_asset(asset)
        
        return {
            'asset': normalized,
            'original_input': asset,
            'site_count': len(sites),
            'sites': sites,
        }
