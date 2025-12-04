"""
Universal scraper that works on any URL without pre-configuration.
Uses discovery mode and LLM-powered analysis to extract data.
"""

import asyncio
import json
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

import pandas as pd

from .base_scraper import BaseScraper, ScraperResult
from ..utils.logger import get_logger
from ..utils.browser import BrowserManager, PageLoadResult, filter_data_requests
from ..utils.config_manager import SiteConfig, DataSource
from ..detector.network_inspector import NetworkInspector, CandidateEndpoint
from ..detector.data_detector import DataDetector, DetectionResult, ExtractionStrategy
from ..extractor.table_extractor import TableExtractor
from ..extractor.json_extractor import JsonExtractor


@dataclass
class DiscoveryResult:
    """Result of data source discovery."""
    url: str
    page_load_result: Optional[PageLoadResult] = None
    candidate_endpoints: List[CandidateEndpoint] = None
    detection_result: Optional[DetectionResult] = None
    recommended_strategy: Optional[ExtractionStrategy] = None
    
    def __post_init__(self):
        if self.candidate_endpoints is None:
            self.candidate_endpoints = []


class UniversalScraper(BaseScraper):
    """
    Universal scraper that can extract data from any URL.
    Uses browser automation, network inspection, and AI detection.
    """
    
    def __init__(
        self,
        config: Optional[SiteConfig] = None,
        use_llm: bool = True,
        headless: bool = True,
        **kwargs
    ):
        """
        Initialize the universal scraper.
        
        Args:
            config: Optional site configuration
            use_llm: Whether to use LLM for data detection
            headless: Run browser in headless mode
        """
        super().__init__(config=config, **kwargs)
        
        self.use_llm = use_llm
        self.headless = headless
        
        self.network_inspector = NetworkInspector()
        self.table_extractor = TableExtractor()
        self.json_extractor = JsonExtractor()
        self.data_detector = DataDetector() if use_llm else None
        
        self._discovery_result: Optional[DiscoveryResult] = None
        self._extraction_strategy: Optional[ExtractionStrategy] = None
    
    async def discover_data_sources(self, url: str) -> DiscoveryResult:
        """
        Discover data sources on a page.
        
        Args:
            url: URL to analyze
        
        Returns:
            DiscoveryResult with candidate endpoints and strategies
        """
        self.logger.info(f"Discovering data sources for {url}")
        
        result = DiscoveryResult(url=url)
        
        # Load page with browser and capture network requests
        async with BrowserManager(headless=self.headless, user_agent=self.user_agent) as browser:
            page_result = await browser.load_page(
                url=url,
                wait_for_timeout=5000,
                capture_network=True,
                capture_response_bodies=True,
            )
            result.page_load_result = page_result
        
        if page_result.error:
            self.logger.error(f"Page load error: {page_result.error}")
            return result
        
        # Analyze network requests
        data_requests = filter_data_requests(page_result.network_requests)
        result.candidate_endpoints = self.network_inspector.analyze_requests(
            data_requests, url
        )
        
        self.logger.info(f"Found {len(result.candidate_endpoints)} candidate endpoints")
        
        # Use LLM detection if available and we have data
        if self.data_detector and result.candidate_endpoints:
            best_endpoint = result.candidate_endpoints[0]
            if best_endpoint.response_body:
                try:
                    json_data = json.loads(best_endpoint.response_body)
                    result.detection_result = self.data_detector.analyze_json(
                        json_data, context=f"From URL: {url}"
                    )
                    if result.detection_result.recommended_strategy:
                        result.recommended_strategy = result.detection_result.recommended_strategy
                except json.JSONDecodeError:
                    pass
        
        # Fallback: analyze HTML if no good API endpoints
        if not result.candidate_endpoints and self.data_detector:
            result.detection_result = self.data_detector.analyze_html(
                page_result.html, context=f"From URL: {url}"
            )
            if result.detection_result.recommended_strategy:
                result.recommended_strategy = result.detection_result.recommended_strategy
        
        self._discovery_result = result
        return result
    
    def discover_data_sources_sync(self, url: str) -> DiscoveryResult:
        """Synchronous wrapper for discover_data_sources."""
        return asyncio.run(self.discover_data_sources(url))
    
    def fetch_raw(self, url: str) -> Dict[str, Any]:
        """
        Fetch raw data from the URL.
        Uses discovery result if available, otherwise runs discovery first.
        """
        # Run discovery if not already done
        if not self._discovery_result or self._discovery_result.url != url:
            self._discovery_result = self.discover_data_sources_sync(url)
        
        discovery = self._discovery_result
        
        # Determine extraction method based on discovery
        if discovery.candidate_endpoints:
            # Use best API endpoint
            best_endpoint = discovery.candidate_endpoints[0]
            self.logger.info(f"Using API endpoint: {best_endpoint.url}")
            
            # If response_body is None (Cloudflare blocking), fetch directly
            if best_endpoint.response_body is None:
                self.logger.warning("Response body is None, fetching endpoint directly...")
                import requests
                try:
                    response = requests.get(
                        best_endpoint.url,
                        headers={"User-Agent": self.user_agent, "Accept": "application/json"},
                        timeout=self.timeout,
                    )
                    response.raise_for_status()
                    content = response.text
                except Exception as e:
                    self.logger.error(f"Failed to fetch endpoint directly: {e}")
                    raise ValueError(f"Could not fetch data from {best_endpoint.url}: {e}")
            else:
                content = best_endpoint.response_body
                if isinstance(content, bytes):
                    content = content.decode("utf-8")
            
            return {
                "type": "api_json",
                "endpoint_url": best_endpoint.url,
                "content": content,
                "content_type": best_endpoint.content_type,
                "detected_structure": best_endpoint.detected_structure,
                "field_names": best_endpoint.field_names,
            }
        
        elif discovery.page_load_result and discovery.page_load_result.html:
            # Fallback to HTML extraction
            self.logger.info("No API endpoints found, using HTML extraction")
            
            return {
                "type": "dom_table",
                "content": discovery.page_load_result.html,
                "content_type": "text/html",
            }
        
        else:
            raise ValueError("No data sources found")
    
    def parse_raw(self, raw_data: Dict[str, Any]) -> pd.DataFrame:
        """Parse raw data into a DataFrame."""
        data_type = raw_data.get("type")
        content = raw_data.get("content")
        
        if data_type == "api_json":
            # Parse JSON response
            if isinstance(content, bytes):
                content = content.decode("utf-8")
            
            try:
                json_data = json.loads(content) if isinstance(content, str) else content
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse JSON: {e}")
                return pd.DataFrame()
            
            # Use detection result for data path if available
            data_path = None
            field_mappings = None
            
            if self._discovery_result and self._discovery_result.detection_result:
                detection = self._discovery_result.detection_result
                if detection.recommended_strategy:
                    data_path = detection.recommended_strategy.data_source.get("data_path")
                    field_mappings = detection.recommended_strategy.field_mappings
            
            return self.json_extractor.extract(
                json_data,
                data_path=data_path,
                field_mappings=field_mappings,
            )
        
        elif data_type == "dom_table":
            # Extract from HTML tables
            return self.table_extractor.extract_best_table(
                content,
                min_rows=3,
                require_numeric=True,
            ) or pd.DataFrame()
        
        else:
            self.logger.error(f"Unknown data type: {data_type}")
            return pd.DataFrame()
    
    def get_proposed_config(self, url: str) -> Optional[SiteConfig]:
        """
        Get a proposed site configuration based on discovery.
        
        Args:
            url: URL that was scraped
        
        Returns:
            Proposed SiteConfig or None
        """
        if not self._discovery_result:
            return None
        
        discovery = self._discovery_result
        from ..utils.io_utils import generate_site_id
        
        site_id = generate_site_id(url)
        
        # Determine extraction strategy
        if discovery.candidate_endpoints:
            endpoint = discovery.candidate_endpoints[0]
            strategy = "api_json"
            data_source = DataSource(
                type="api",
                endpoint=endpoint.url,
                method="GET",
            )
            field_mappings = {}
            if discovery.recommended_strategy:
                field_mappings = discovery.recommended_strategy.field_mappings
        else:
            strategy = "dom_table"
            data_source = DataSource(
                type="table",
                selector="table",
            )
            field_mappings = {}
        
        from urllib.parse import urlparse
        parsed = urlparse(url)
        
        return SiteConfig(
            id=site_id,
            name=f"Auto-detected: {parsed.netloc}",
            base_url=f"{parsed.scheme}://{parsed.netloc}",
            page_url=url,
            extraction_strategy=strategy,
            data_source=data_source,
            field_mappings=field_mappings,
        )
    
    def scrape_with_discovery(
        self,
        url: str,
        override_robots: bool = False,
        save_raw: bool = True,
    ) -> ScraperResult:
        """
        Scrape a URL with full discovery workflow.
        
        Args:
            url: URL to scrape
            override_robots: Override robots.txt for UNKNOWN status
            save_raw: Save raw responses
        
        Returns:
            ScraperResult with extracted data
        """
        # Run discovery first
        self.logger.info(f"Running discovery for {url}")
        discovery = self.discover_data_sources_sync(url)
        
        if not discovery.candidate_endpoints and not (
            discovery.page_load_result and discovery.page_load_result.html
        ):
            return ScraperResult(
                success=False,
                url=url,
                error="No data sources discovered",
            )
        
        # Now run the standard scrape
        return self.scrape(url, override_robots, save_raw)

