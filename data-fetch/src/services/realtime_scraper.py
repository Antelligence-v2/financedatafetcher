"""
Real-time scraping service.
Orchestrates scraping of multiple sites using PipelineRunner and FrontendAPI.
"""

from typing import List, Dict, Optional
import pandas as pd
import sys
from pathlib import Path
import os
import json

# #region agent log
_log_path = Path(__file__).parent.parent.parent.parent / ".cursor" / "debug.log"
try:
    _parent_path = Path(__file__).parent.parent.parent.parent
    _api_path = _parent_path / "src" / "api" / "frontend_api.py"
    with open(_log_path, "a") as f:
        f.write(json.dumps({"location": "realtime_scraper.py:12", "message": "Import path setup", "data": {"file": str(__file__), "parent_path": str(_parent_path), "api_exists": _api_path.exists(), "sys_path_before": sys.path[:5]}, "timestamp": int(os.path.getmtime(__file__) * 1000) if os.path.exists(__file__) else 0, "sessionId": "debug-session", "runId": "run1", "hypothesisId": "E"}) + "\n")
except: pass
# #endregion

# Add root directory to path to import FrontendAPI
# This ensures root/src is found before data-fetch/src
_root_path = Path(__file__).parent.parent.parent.parent
_root_path_str = str(_root_path)
if _root_path_str not in sys.path:
    sys.path.insert(0, _root_path_str)
elif sys.path.index(_root_path_str) > 0:
    # Move to front if already in path but not first
    sys.path.remove(_root_path_str)
    sys.path.insert(0, _root_path_str)

# #region agent log
try:
    with open(_log_path, "a") as f:
        f.write(json.dumps({"location": "realtime_scraper.py:25", "message": "After path insert", "data": {"sys_path_after": sys.path[:5], "root_in_path": _root_path_str in sys.path, "root_position": sys.path.index(_root_path_str) if _root_path_str in sys.path else -1}, "timestamp": int(os.path.getmtime(__file__) * 1000) if os.path.exists(__file__) else 0, "sessionId": "debug-session", "runId": "run1", "hypothesisId": "F"}) + "\n")
except: pass
# #endregion

try:
    from src.api.frontend_api import FrontendAPI
    # #region agent log
    try:
        with open(_log_path, "a") as f:
            f.write(json.dumps({"location": "realtime_scraper.py:33", "message": "FrontendAPI import success", "data": {"module": str(FrontendAPI)}, "timestamp": int(os.path.getmtime(__file__) * 1000) if os.path.exists(__file__) else 0, "sessionId": "debug-session", "runId": "run1", "hypothesisId": "G"}) + "\n")
    except: pass
    # #endregion
except ImportError as e:
    # #region agent log
    try:
        with open(_log_path, "a") as f:
            f.write(json.dumps({"location": "realtime_scraper.py:39", "message": "FrontendAPI import failed", "data": {"error": str(e), "sys_path": sys.path[:5], "root_path": _root_path_str}, "timestamp": int(os.path.getmtime(__file__) * 1000) if os.path.exists(__file__) else 0, "sessionId": "debug-session", "runId": "run1", "hypothesisId": "H"}) + "\n")
    except: pass
    # #endregion
    raise
from .asset_mapper import AssetMapper
from ..utils.logger import get_logger


class RealtimeScraper:
    """
    Service for real-time scraping of multiple sites.
    Handles parallel/sequential scraping with error handling.
    """
    
    def __init__(self, frontend_api: FrontendAPI = None, asset_mapper: AssetMapper = None):
        """
        Initialize real-time scraper.
        
        Args:
            frontend_api: FrontendAPI instance (creates new if not provided)
            asset_mapper: AssetMapper instance (creates new if not provided)
        """
        self.api = frontend_api or FrontendAPI()
        self.asset_mapper = asset_mapper or AssetMapper()
        self.logger = get_logger()
    
    def scrape_asset_data(
        self,
        asset: str,
        site_ids: Optional[List[str]] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Scrape data for an asset from all relevant sites.
        
        Args:
            asset: Asset name (e.g., 'BTC', 'ETH')
            site_ids: Optional list of specific site IDs to scrape.
                     If None, automatically determines sites from asset.
        
        Returns:
            Dictionary mapping site_id to DataFrame (only successful scrapes)
        """
        # Get site IDs if not provided
        if site_ids is None:
            site_ids = self.asset_mapper.get_sites_for_asset(asset)
        
        if not site_ids:
            self.logger.warning(f"No sites found for asset: {asset}")
            return {}
        
        self.logger.info(f"Scraping {len(site_ids)} sites for asset: {asset}")
        
        results: Dict[str, pd.DataFrame] = {}
        
        # Scrape each site sequentially (to respect rate limits)
        for site_id in site_ids:
            try:
                self.logger.info(f"Scraping site: {site_id}")
                df = self.scrape_single_site(site_id)
                
                if df is not None and not df.empty:
                    results[site_id] = df
                    self.logger.info(f"Successfully scraped {len(df)} rows from {site_id}")
                else:
                    self.logger.warning(f"No data extracted from {site_id}")
            
            except Exception as e:
                self.logger.error(f"Error scraping {site_id}: {str(e)}")
                # Continue with other sites
                continue
        
        self.logger.info(f"Successfully scraped {len(results)}/{len(site_ids)} sites")
        return results
    
    def scrape_single_site(self, site_id: str) -> Optional[pd.DataFrame]:
        """
        Scrape a single site.
        
        Args:
            site_id: Site ID from configuration
            
        Returns:
            DataFrame with scraped data, or None if failed
        """
        try:
            result = self.api.scrape_configured_site(
                site_id=site_id,
                use_stealth=True,
                override_robots=False,
            )
            
            if result.get("success") and result.get("data") is not None:
                df = result["data"]
                if not df.empty:
                    return df
            
            return None
        
        except Exception as e:
            self.logger.error(f"Exception scraping {site_id}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None
    
    def scrape_multiple_sites(
        self,
        site_ids: List[str],
        continue_on_error: bool = True
    ) -> Dict[str, pd.DataFrame]:
        """
        Scrape multiple sites.
        
        Args:
            site_ids: List of site IDs to scrape
            continue_on_error: If True, continue scraping other sites if one fails
        
        Returns:
            Dictionary mapping site_id to DataFrame (only successful scrapes)
        """
        results: Dict[str, pd.DataFrame] = {}
        
        for site_id in site_ids:
            try:
                df = self.scrape_single_site(site_id)
                if df is not None and not df.empty:
                    results[site_id] = df
            except Exception as e:
                self.logger.error(f"Error scraping {site_id}: {str(e)}")
                if not continue_on_error:
                    raise
                continue
        
        return results
