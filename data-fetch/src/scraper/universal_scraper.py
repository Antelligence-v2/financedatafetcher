"""
Universal scraper that works on any URL without pre-configuration.
Uses discovery mode and LLM-powered analysis to extract data.
"""

import asyncio
import json
import re
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

import pandas as pd

from .base_scraper import BaseScraper, ScraperResult
from ..utils.logger import get_logger
from ..utils.browser import BrowserManager, PageLoadResult, filter_data_requests
from ..utils.config_manager import SiteConfig, DataSource
from ..utils.stealth import StealthManager
from ..detector.network_inspector import NetworkInspector, CandidateEndpoint
from ..detector.data_detector import DataDetector, DetectionResult, ExtractionStrategy
from ..extractor.table_extractor import TableExtractor
from ..extractor.json_extractor import JsonExtractor
from ..extractor.csv_extractor import CsvExtractor
from ..extractor.xml_extractor import XmlExtractor
from ..extractor.js_data_extractor import JsDataExtractor


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
        use_stealth: bool = True,
        **kwargs
    ):
        """
        Initialize the universal scraper.
        
        Args:
            config: Optional site configuration
            use_llm: Whether to use LLM for data detection
            headless: Run browser in headless mode
            use_stealth: Enable stealth mode for browser automation
        """
        super().__init__(config=config, **kwargs)
        
        self.use_llm = use_llm
        self.headless = headless
        self.use_stealth = use_stealth
        self.stealth_manager = StealthManager(randomize=use_stealth) if use_stealth else None
        
        self.network_inspector = NetworkInspector()
        self.table_extractor = TableExtractor()
        self.json_extractor = JsonExtractor()
        self.csv_extractor = CsvExtractor()
        self.xml_extractor = XmlExtractor()
        self.js_extractor = JsDataExtractor()
        self.data_detector = DataDetector() if use_llm else None
        
        self._discovery_result: Optional[DiscoveryResult] = None
        self._extraction_strategy: Optional[ExtractionStrategy] = None
        self._used_strategy: Optional[str] = None  # Track which strategy succeeded
    
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
        
        # Configure browser with stealth if enabled
        user_agent = self.user_agent
        if self.use_stealth and self.stealth_manager:
            fingerprint = self.stealth_manager.get_fingerprint()
            user_agent = fingerprint.user_agent
        
        # Load page with browser and capture network requests
        async with BrowserManager(
            headless=self.headless,
            user_agent=user_agent,
        ) as browser:
            # Inject stealth scripts if enabled
            if self.use_stealth and self.stealth_manager:
                stealth_scripts = self.stealth_manager.inject_stealth_scripts()
                for script in stealth_scripts:
                    await browser._context.add_init_script(script)
            # Try to detect if this is a financial quote page
            is_financial_quote = any(keyword in url.lower() for keyword in [
                "quote", "market", "stock", "equity", "finance", "ticker", "symbol"
            ])
            
            # Use longer wait times for financial sites
            wait_timeout = 10000 if is_financial_quote else 5000
            
            # Common selectors for financial data
            financial_selectors = [
                "table",  # Generic table
                "[data-testid*='table']",  # React testing selectors
                "[data-testid*='quote']",  # Quote data
                ".quote-table", ".data-table", ".market-data",
                "table tbody tr",  # Table rows
                "[class*='table']", "[class*='data']", "[class*='quote']",
            ]
            
            # Site-specific waiting strategies
            wait_for_selector = None
            if "yahoo.com" in url or "finance.yahoo.com" in url:
                # Yahoo Finance - wait for table with multiple tbody rows (at least 2 data rows)
                wait_for_selector = "table tbody tr:nth-child(2), table[data-test='historical-prices'] tbody tr:nth-child(2)"
                # Use longer timeout for Yahoo Finance as tables load dynamically
                wait_timeout = max(wait_timeout, 20000)  # 20 seconds for Yahoo Finance
            elif "bloomberg.com" in url:
                wait_for_selector = "table, [data-table], .data-table"
            elif "reuters.com" in url:
                wait_for_selector = "table, [data-testid*='table']"
            elif "theblock.co" in url:
                wait_for_selector = "table, [data-chart]"
            elif is_financial_quote:
                wait_for_selector = "table"
            
            page_result = await browser.load_page(
                url=url,
                wait_for_timeout=wait_timeout,
                capture_network=True,
                capture_response_bodies=True,
                wait_for_selector=wait_for_selector,
            )
            
            # For Yahoo Finance, do an additional check to ensure table has data
            if ("yahoo.com" in url or "finance.yahoo.com" in url) and not page_result.error:
                # Wait a bit more and check if table has multiple rows
                import asyncio
                await asyncio.sleep(2)  # Additional wait for dynamic content
                
                # Try to get updated HTML if we have access to the page
                try:
                    # Create a new page to get fresh content with table data
                    new_page = await browser._context.new_page()
                    await new_page.goto(url, timeout=30000, wait_until="networkidle")
                    # Wait specifically for table rows
                    try:
                        await new_page.wait_for_selector("table tbody tr:nth-child(2)", timeout=10000)
                    except Exception:
                        pass  # Continue anyway
                    await asyncio.sleep(2)  # Wait for JS to render
                    page_result.html = await new_page.content()
                    await new_page.close()
                    self.logger.debug("Refreshed HTML content for Yahoo Finance table")
                except Exception as e:
                    self.logger.debug(f"Could not refresh page content: {e}")
            
            # If no table found, wait a bit more and try to use smart waiting
            if is_financial_quote and not page_result.error:
                # Check if we have tables in the HTML
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(page_result.html, "lxml")
                tables = soup.find_all("table")
                
                if not tables or len(tables) == 0:
                    # Wait a bit more for dynamic content
                    self.logger.info("No tables found initially, waiting for dynamic content...")
                    import asyncio
                    await asyncio.sleep(3)  # Additional wait
                    
                    # Try to get updated HTML by creating a new page
                    try:
                        # Create a new page to get fresh content
                        new_page = await browser._context.new_page()
                        await new_page.goto(url, timeout=30000, wait_until="networkidle")
                        await asyncio.sleep(2)  # Wait for JS to render
                        page_result.html = await new_page.content()
                        await new_page.close()
                    except Exception as e:
                        self.logger.debug(f"Could not reload page content: {e}")
            
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
        Fetch raw data from the URL using fallback chain.
        Tries strategies in order: API → JavaScript → Table → CSV → XML
        """
        # Check if config specifies browser-based extraction
        if self.config and self.config.extraction_strategy == "dom_js_extraction":
            # Use browser-based extraction (CoinGlass, etc.)
            from ..scraper.coinglass_scraper import CoinGlassScraper
            coinglass_scraper = CoinGlassScraper(config=self.config, use_stealth=self.use_stealth)
            return coinglass_scraper.fetch_raw(url)
        
        # Run discovery if not already done
        if not self._discovery_result or self._discovery_result.url != url:
            self._discovery_result = self.discover_data_sources_sync(url)
        
        discovery = self._discovery_result
        
        # Strategy 1: Try API endpoint (JSON)
        # Special handling for known sites with predictable API patterns
        site_specific_handled = False
        
        # The Block - try their API pattern first
        if "theblock.co" in url and "/data/" in url:
            try:
                # The Block uses /api/charts/chart/{path} pattern
                path = url.split("/data/")[1].rstrip("/")
                api_url = f"https://www.theblock.co/api/charts/chart/{path}"
                self.logger.info(f"Trying The Block API pattern: {api_url}")
                import requests
                headers = {
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                    "Accept": "application/json",
                    "Referer": url,
                }
                response = requests.get(api_url, headers=headers, timeout=self.timeout)
                if response.status_code == 200:
                    self._used_strategy = "api_json"
                    return {
                        "type": "api_json",
                        "endpoint_url": api_url,
                        "content": response.text,
                        "content_type": "application/json",
                    }
            except Exception as e:
                self.logger.debug(f"The Block API pattern failed: {e}, trying discovered endpoints")
        
        # Bloomberg - often uses GraphQL or REST APIs
        if "bloomberg.com" in url:
            # Bloomberg typically loads data via JavaScript, so we'll rely on JS extraction
            # But also check for API endpoints in network requests
            pass  # Handled by JS extraction strategy
        
        # Reuters - check for common API patterns
        if "reuters.com" in url:
            # Reuters often uses React/Next.js with data in __NEXT_DATA__ or API endpoints
            # This will be handled by JS extraction and endpoint discovery
            pass
        
        # Try discovered candidate endpoints
        if discovery.candidate_endpoints:
            for endpoint in discovery.candidate_endpoints:
                # Check content type - CandidateEndpoint doesn't have is_json, check content_type
                content_type = (endpoint.content_type or "").lower()
                # Also check URL patterns for JSON endpoints
                url_lower = endpoint.url.lower()
                
                # More lenient JSON detection - check multiple indicators
                is_json_endpoint = (
                    "json" in content_type or 
                    endpoint.detected_structure in ["array", "timeseries", "nested_timeseries", "object"] or
                    "/api/" in url_lower or
                    "/chart" in url_lower or
                    "/data" in url_lower or
                    "/indicesHistory" in url_lower or  # The Block pattern
                    (endpoint.confidence_score > 0.3 and not content_type)  # High confidence but no content type = likely JSON
                )
                
                if is_json_endpoint:
                    try:
                        self.logger.info(f"Trying API endpoint: {endpoint.url}")
                        best_endpoint = endpoint
                        
                        # If response_body is None (Cloudflare blocking), fetch directly
                        if best_endpoint.response_body is None:
                            self.logger.warning("Response body is None, fetching endpoint directly...")
                            import requests
                            try:
                                headers = {
                                    "User-Agent": self.user_agent,
                                    "Accept": "application/json",
                                    "Referer": url if url else "",
                                }
                                response = requests.get(
                                    best_endpoint.url,
                                    headers=headers,
                                    timeout=self.timeout,
                                )
                                response.raise_for_status()
                                content = response.text
                            except Exception as e:
                                self.logger.warning(f"Failed to fetch endpoint directly: {e}, trying next strategy")
                                continue
                        else:
                            content = best_endpoint.response_body
                            if isinstance(content, bytes):
                                content = content.decode("utf-8")
                        
                        self._used_strategy = "api_json"
                        return {
                            "type": "api_json",
                            "endpoint_url": best_endpoint.url,
                            "content": content,
                            "content_type": best_endpoint.content_type,
                            "detected_structure": best_endpoint.detected_structure,
                            "field_names": best_endpoint.field_names,
                        }
                    except Exception as e:
                        self.logger.warning(f"API endpoint extraction failed: {e}, trying next strategy")
                        continue
        
        # Strategy 2: Try JavaScript data extraction
        if discovery.page_load_result and discovery.page_load_result.html:
            try:
                self.logger.info("Trying JavaScript data extraction")
                html = discovery.page_load_result.html
                
                # Try to extract from JavaScript variables
                js_df = self.js_extractor.extract_from_html(html)
                if not js_df.empty and len(js_df) > 0:
                    self._used_strategy = "js_object"
                    return {
                        "type": "js_object",
                        "content": html,
                        "content_type": "text/html",
                        "extracted_df": js_df,  # Pre-extracted
                    }
            except Exception as e:
                self.logger.warning(f"JavaScript extraction failed: {e}, trying next strategy")
        
        # Strategy 3: Try HTML table extraction
        if discovery.page_load_result and discovery.page_load_result.html:
            try:
                self.logger.info("Trying HTML table extraction")
                html = discovery.page_load_result.html
                
                # For Yahoo Finance and similar sites, try to wait for table data if needed
                if "yahoo.com" in url or "finance.yahoo.com" in url:
                    # Yahoo Finance tables might need additional processing
                    # Check if table exists but has no data rows
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(html, "lxml")
                    tables = soup.find_all("table")
                    
                    # Look for Yahoo Finance specific table structures
                    for table in tables:
                        # Check for tbody with rows
                        tbody = table.find("tbody")
                        if tbody:
                            rows = tbody.find_all("tr")
                            if len(rows) > 0:
                                self.logger.info(f"Found Yahoo Finance table with {len(rows)} rows in tbody")
                                self._used_strategy = "dom_table"
                                return {
                                    "type": "dom_table",
                                    "content": html,
                                    "content_type": "text/html",
                                }
                        # Also check for rows directly in table
                        rows = table.find_all("tr")
                        if len(rows) > 1:  # More than just header
                            self.logger.info(f"Found table with {len(rows)} rows")
                            self._used_strategy = "dom_table"
                            return {
                                "type": "dom_table",
                                "content": html,
                                "content_type": "text/html",
                            }
                
                # Check if there are tables
                tables = self.table_extractor.find_tables(html)
                if tables:
                    # Verify at least one table has data
                    for table_info in tables:
                        if table_info.num_rows > 0:
                            self._used_strategy = "dom_table"
                            return {
                                "type": "dom_table",
                                "content": html,
                                "content_type": "text/html",
                            }
                else:
                    # If no tables found, try to extract from div-based data structures
                    # (some sites use divs styled as tables)
                    self.logger.info("No standard tables found, checking for div-based data structures...")
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(html, "lxml")
                    
                    # Look for common financial data patterns in divs
                    data_containers = soup.find_all(["div", "section"], class_=re.compile(
                        r"table|data|quote|market|price|stock|instrument", re.I
                    ))
                    
                    if data_containers:
                        # Try to extract structured data from divs
                        rows = []
                        for container in data_containers[:5]:  # Limit to first 5 containers
                            # Look for rows (divs with data attributes or specific classes)
                            row_elements = container.find_all(["div", "tr"], class_=re.compile(r"row|item|entry", re.I))
                            if row_elements:
                                for row in row_elements:
                                    cells = row.find_all(["div", "td", "span"], class_=re.compile(r"cell|col|value", re.I))
                                    if cells and len(cells) >= 2:  # At least 2 cells
                                        row_data = [self._clean_text(cell.get_text()) for cell in cells]
                                        if any(cell.strip() for cell in row_data):  # Skip empty rows
                                            rows.append(row_data)
                        
                        if rows and len(rows) > 0:
                            # Try to create DataFrame from div-based data
                            try:
                                import pandas as pd
                                # Use first row as header if it looks like headers
                                if any(keyword in str(rows[0]).lower() for keyword in ["date", "price", "value", "name"]):
                                    df = pd.DataFrame(rows[1:], columns=rows[0] if len(rows[0]) == len(rows[1]) else None)
                                else:
                                    df = pd.DataFrame(rows)
                                
                                if not df.empty and len(df) > 0:
                                    self._used_strategy = "dom_table"
                                    self.logger.info(f"Extracted {len(df)} rows from div-based structure")
                                    return {
                                        "type": "dom_table",
                                        "content": html,
                                        "content_type": "text/html",
                                        "extracted_df": df,  # Pre-extracted
                                    }
                            except Exception as e:
                                self.logger.debug(f"Failed to create DataFrame from divs: {e}")
            except Exception as e:
                self.logger.warning(f"Table extraction failed: {e}, trying next strategy")
        
        # Strategy 4: Try CSV endpoint
        if discovery.candidate_endpoints:
            for endpoint in discovery.candidate_endpoints:
                content_type = (endpoint.content_type or "").lower()
                if "csv" in content_type:
                    try:
                        self.logger.info(f"Trying CSV endpoint: {endpoint.url}")
                        import requests
                        response = requests.get(
                            endpoint.url,
                            headers={"User-Agent": self.user_agent},
                            timeout=self.timeout,
                        )
                        response.raise_for_status()
                        
                        self._used_strategy = "csv"
                        return {
                            "type": "csv",
                            "endpoint_url": endpoint.url,
                            "content": response.content,
                            "content_type": endpoint.content_type,
                        }
                    except Exception as e:
                        self.logger.warning(f"CSV extraction failed: {e}, trying next strategy")
                        continue
        
        # Strategy 5: Try XML endpoint
        if discovery.candidate_endpoints:
            for endpoint in discovery.candidate_endpoints:
                if "xml" in (endpoint.content_type or "").lower():
                    try:
                        self.logger.info(f"Trying XML endpoint: {endpoint.url}")
                        import requests
                        response = requests.get(
                            endpoint.url,
                            headers={"User-Agent": self.user_agent},
                            timeout=self.timeout,
                        )
                        response.raise_for_status()
                        
                        self._used_strategy = "xml"
                        return {
                            "type": "xml",
                            "endpoint_url": endpoint.url,
                            "content": response.content,
                            "content_type": endpoint.content_type,
                        }
                    except Exception as e:
                        self.logger.warning(f"XML extraction failed: {e}")
                        continue
        
        # All strategies failed
        raise ValueError("No data sources found with any extraction strategy")
    
    def parse_raw(self, raw_data: Dict[str, Any]) -> pd.DataFrame:
        """Parse raw data into a DataFrame using the appropriate extractor."""
        data_type = raw_data.get("type")
        content = raw_data.get("content")
        
        # Handle browser-based extraction (dom_js_extraction)
        if data_type == "dom_js_extraction":
            # Use CoinGlass scraper for parsing if config matches
            if self.config and self.config.extraction_strategy == "dom_js_extraction":
                from ..scraper.coinglass_scraper import CoinGlassScraper
                coinglass_scraper = CoinGlassScraper(config=self.config, use_stealth=self.use_stealth)
                return coinglass_scraper.parse_raw(raw_data)
            # Otherwise try to extract from HTML/JS
            html = content if isinstance(content, str) else content.decode("utf-8") if content else ""
            if html:
                # Try JavaScript extraction first
                js_df = self.js_extractor.extract_from_html(html)
                if not js_df.empty and len(js_df) > 0:
                    return js_df
                # Fallback to table extraction
                tables = self.table_extractor.find_tables(html)
                if tables:
                    return self.table_extractor.extract_best_table(html, min_rows=1, require_numeric=False)
            return pd.DataFrame()
        
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
        
        elif data_type == "js_object":
            # JavaScript data (may be pre-extracted)
            if "extracted_df" in raw_data:
                return raw_data["extracted_df"]
            
            # Extract from HTML
            html = content if isinstance(content, str) else content.decode("utf-8")
            return self.js_extractor.extract_from_html(html)
        
        elif data_type == "dom_table":
            # Extract from HTML tables
            html = content if isinstance(content, str) else content.decode("utf-8")
            
            # Check if we have pre-extracted DataFrame (from div-based structures)
            if "extracted_df" in raw_data:
                return raw_data["extracted_df"]
            
            # Get URL from discovery result for site-specific handling
            current_url = None
            if self._discovery_result:
                current_url = self._discovery_result.url
            
            # For Yahoo Finance, try extracting directly without min_rows requirement first
            if current_url and ("yahoo.com" in current_url or "finance.yahoo.com" in current_url):
                # Yahoo Finance tables might have complex structure - try direct extraction
                all_tables = self.table_extractor.extract_all_tables(html)
                if all_tables:
                    # Find the table with the most rows and columns (likely the historical data table)
                    best_table = None
                    best_score = 0
                    for table_df in all_tables:
                        # Score based on rows and columns
                        score = len(table_df) * len(table_df.columns)
                        # Bonus for having date column
                        if any("date" in str(col).lower() for col in table_df.columns):
                            score *= 2
                        # Bonus for having financial columns
                        financial_cols = ["open", "high", "low", "close", "volume"]
                        if any(col.lower() in str(table_df.columns).lower() for col in financial_cols):
                            score *= 2
                        if score > best_score:
                            best_score = score
                            best_table = table_df
                    if best_table is not None and len(best_table) > 0:
                        self.logger.info(f"Extracted Yahoo Finance table with {len(best_table)} rows and {len(best_table.columns)} columns")
                        return best_table
            
            # Try with require_numeric first, then without if no results
            df = self.table_extractor.extract_best_table(
                html,
                min_rows=1,  # Lower threshold for Yahoo Finance
                require_numeric=False,  # Don't require numeric for first attempt
            )
            if df is None or (isinstance(df, pd.DataFrame) and df.empty):
                # Try again with different parameters
                df = self.table_extractor.extract_best_table(
                    html,
                    min_rows=1,
                    require_numeric=True,
                )
            
            # If still empty, try extracting all tables and combine them
            if df is None or (isinstance(df, pd.DataFrame) and df.empty):
                self.logger.info("Best table extraction failed, trying to extract all tables...")
                all_tables = self.table_extractor.extract_all_tables(html)
                if all_tables:
                    # Use the largest table
                    largest_table = max(all_tables, key=len)
                    if len(largest_table) > 0:
                        df = largest_table
            
            # Fix: Use proper check instead of "df or pd.DataFrame()" which causes ambiguous truth value error
            if df is None:
                return pd.DataFrame()
            return df
        
        elif data_type == "csv":
            # Extract from CSV
            return self.csv_extractor.extract(content)
        
        elif data_type == "xml":
            # Extract from XML
            return self.xml_extractor.extract(content)
        
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
    
    def _clean_text(self, text: str) -> str:
        """Clean extracted text."""
        if not text:
            return ""
        # Remove extra whitespace
        text = " ".join(text.split())
        # Remove non-printable characters
        text = "".join(c for c in text if c.isprintable() or c.isspace())
        return text.strip()

