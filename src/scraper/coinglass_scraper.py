"""
CoinGlass scraper - browser-based extraction for crypto derivatives data.
Uses browser automation to extract data from CoinGlass pages.
"""

import json
import os
import re
import asyncio
from typing import Dict, Any, Optional
from dataclasses import dataclass

import pandas as pd
from bs4 import BeautifulSoup

from .base_scraper import BaseScraper, ScraperResult
from ..utils.logger import get_logger
from ..utils.browser import BrowserManager
from ..utils.config_manager import SiteConfig
from ..utils.stealth import StealthManager
from ..extractor.dom_extractor import DomExtractor, ExtractionSelector
from ..extractor.js_data_extractor import JsDataExtractor
from ..extractor.financial_normalizer import FinancialNormalizer


@dataclass
class CoinGlassMetrics:
    """Structure for CoinGlass metrics."""
    btc_price: Optional[float] = None
    futures_volume_24h: Optional[float] = None
    spot_volume_24h: Optional[float] = None
    open_interest: Optional[float] = None
    net_inflow_24h: Optional[float] = None
    net_inflow_5min: Optional[float] = None
    net_inflow_1h: Optional[float] = None
    net_inflow_4h: Optional[float] = None
    net_inflow_12h: Optional[float] = None
    btc_volatility_1d: Optional[float] = None
    eth_volatility_1d: Optional[float] = None
    sol_volatility_1d: Optional[float] = None
    xrp_volatility_1d: Optional[float] = None
    doge_volatility_1d: Optional[float] = None
    # Derivatives snapshot fields
    futures_oi_all_exchanges: Optional[float] = None
    cme_btc_oi: Optional[float] = None
    binance_btc_oi: Optional[float] = None
    btc_options_calls_oi: Optional[float] = None
    btc_options_puts_oi: Optional[float] = None
    # Liquidations fields
    total_liquidations_24h: Optional[float] = None
    long_liquidations: Optional[float] = None
    short_liquidations: Optional[float] = None
    btc_liquidations_24h: Optional[float] = None
    eth_liquidations_24h: Optional[float] = None


class CoinGlassScraper(BaseScraper):
    """
    Browser-based scraper for CoinGlass.
    Extracts data from rendered pages using DOM extraction and JavaScript evaluation.
    """
    
    def __init__(
        self,
        config: Optional[SiteConfig] = None,
        use_stealth: bool = True,
        **kwargs
    ):
        """
        Initialize CoinGlass scraper.
        
        Args:
            config: Site configuration
            use_stealth: Enable stealth mode for browser
        """
        super().__init__(config=config, **kwargs)
        self.use_stealth = use_stealth
        self.stealth_manager = StealthManager(randomize=use_stealth) if use_stealth else None
        self.dom_extractor = DomExtractor()
        self.js_extractor = JsDataExtractor()
        self.normalizer = FinancialNormalizer()
    
    def fetch_raw(self, url: str) -> Dict[str, Any]:
        """
        Fetch raw data from CoinGlass page using browser automation.
        
        Args:
            url: URL to scrape
        
        Returns:
            Dict with HTML content and metadata
        
        Raises:
            RuntimeError: If browser dependencies are missing
        """
        self.logger.info(f"Loading CoinGlass page: {url}")
        
        # Check for Playwright availability first
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise RuntimeError(
                "Playwright is not installed. CoinGlass scraper requires browser automation. "
                "Install with: pip install playwright && playwright install chromium"
            )
        
        # Use browser to load page
        async def _fetch():
            # Configure browser with stealth if enabled
            user_agent = self.user_agent
            if self.use_stealth and self.stealth_manager:
                fingerprint = self.stealth_manager.get_fingerprint()
                user_agent = fingerprint.user_agent
            
            async with BrowserManager(
                headless=True,
                user_agent=user_agent,
            ) as browser:
                # Inject stealth scripts if enabled
                if self.use_stealth and self.stealth_manager:
                    stealth_scripts = self.stealth_manager.inject_stealth_scripts()
                    for script in stealth_scripts:
                        await browser._context.add_init_script(script)
                
                page = await browser._context.new_page()
                
                # Inject stealth scripts into page as well
                if self.use_stealth and self.stealth_manager:
                    stealth_scripts = self.stealth_manager.inject_stealth_scripts()
                    for script in stealth_scripts:
                        await page.add_init_script(script)
                
                try:
                    # Load page with longer timeout for dynamic content
                    await page.goto(url, timeout=30000, wait_until="networkidle")
                    
                    # Wait for content to load (CoinGlass uses React)
                    await asyncio.sleep(3)
                    
                    # Try to wait for specific elements that indicate data is loaded
                    try:
                        # Wait for price or volume elements
                        await page.wait_for_selector(
                            "[class*='price'], [class*='volume'], [data-testid*='price']",
                            timeout=10000
                        )
                    except Exception:
                        # Continue anyway if selector not found
                        pass
                    
                    # Additional wait for JavaScript to render
                    await asyncio.sleep(2)
                    
                    # Get HTML content
                    html = await page.content()
                    
                    # Try to capture network requests for API data
                    network_data = None
                    api_responses = []
                    
                    # Capture network requests (store in list for later processing)
                    def handle_response(response):
                        try:
                            url = response.url.lower()
                            if "api" in url or "data" in url or "coinglass" in url:
                                # Store response for async processing
                                api_responses.append(response)
                        except:
                            pass
                    
                    page.on("response", handle_response)
                    
                    # Wait a bit for responses to come in
                    await asyncio.sleep(1)
                    
                    try:
                        # Evaluate JavaScript to get data from window objects and React state
                        js_data = await page.evaluate("""
                            () => {
                                const data = {};
                                // Try common data variable names
                                if (window.__INITIAL_STATE__) data.initialState = window.__INITIAL_STATE__;
                                if (window.chartData) data.chartData = window.chartData;
                                if (window.marketData) data.marketData = window.marketData;
                                if (window.__NEXT_DATA__) data.nextData = window.__NEXT_DATA__;
                                
                                // Try to extract from React component state
                                try {
                                    const reactRoot = document.querySelector('#root, [data-reactroot]');
                                    if (reactRoot && reactRoot._reactInternalInstance) {
                                        const fiber = reactRoot._reactInternalInstance;
                                        if (fiber.memoizedState) {
                                            data.reactState = JSON.stringify(fiber.memoizedState);
                                        }
                                    }
                                } catch(e) {}
                                
                                // Try to find data in script tags
                                const scripts = document.querySelectorAll('script[type="application/json"]');
                                scripts.forEach((script, idx) => {
                                    try {
                                        data[`script_${idx}`] = JSON.parse(script.textContent);
                                    } catch(e) {}
                                });
                                
                                return JSON.stringify(data);
                            }
                        """)
                        if js_data and js_data != "{}":
                            network_data = json.loads(js_data)
                    except Exception as e:
                        self.logger.debug(f"Could not extract JS data: {e}")
                    
                    # Process API responses - capture all network requests
                    if api_responses:
                        network_data = network_data or {}
                        processed_responses = []
                        for resp in api_responses:
                            try:
                                if resp.status == 200:
                                    # Try JSON first
                                    try:
                                        data = await resp.json()
                                        processed_responses.append({
                                            "url": resp.url,
                                            "data": data,
                                            "type": "json"
                                        })
                                    except:
                                        # Try text/CSV
                                        try:
                                            text_data = await resp.text()
                                            processed_responses.append({
                                                "url": resp.url,
                                                "data": text_data,
                                                "type": "text"
                                            })
                                        except:
                                            pass
                            except Exception as e:
                                self.logger.debug(f"Error processing API response: {e}")
                        if processed_responses:
                            network_data["api_responses"] = processed_responses
                            self.logger.info(f"Captured {len(processed_responses)} API responses")
                    
                    return {
                        "type": "dom_js_extraction",
                        "content": html,
                        "url": url,
                        "js_data": network_data,
                    }
                    
                finally:
                    await page.close()
            
            return {
                "type": "dom_js_extraction",
                "content": "",
                "url": url,
            }
        
        try:
            result = asyncio.run(_fetch())
            return result
        except RuntimeError as e:
            error_msg = str(e)
            if "missing" in error_msg.lower() or "libnspr4" in error_msg or "shared library" in error_msg.lower():
                # Detect deployment platform
                is_replit = os.environ.get("REPL_ID") is not None
                if is_replit:
                    raise RuntimeError(
                        "CoinGlass scraper requires browser automation but Playwright dependencies are not available. "
                        "On Replit, ensure replit.nix includes: pkgs.mesa, pkgs.nss, pkgs.nspr, pkgs.atk, "
                        "pkgs.at-spi2-atk, pkgs.cups, pkgs.libdrm, pkgs.xorg.libXcomposite, pkgs.xorg.libXdamage, "
                        "pkgs.pango, and other browser dependencies. Rebuild the environment after updating replit.nix."
                    )
                else:
                    raise RuntimeError(
                        "CoinGlass scraper requires browser automation but Playwright dependencies are not available. "
                        "On Streamlit Cloud, ensure packages.txt includes: libnss3, libnspr4, libatk1.0-0, "
                        "libatk-bridge2.0-0, libcups2, libdrm2, libxcomposite1, libxdamage1, libgbm1, and libpango-1.0-0."
                    )
            raise
    
    def parse_raw(self, raw_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Parse raw HTML/JS data into DataFrame.
        
        Args:
            raw_data: Raw data from fetch_raw
        
        Returns:
            Parsed DataFrame
        """
        html = raw_data.get("content", "")
        url = raw_data.get("url", "")
        js_data = raw_data.get("js_data")
        
        if not html:
            self.logger.error("No HTML content to parse")
            return pd.DataFrame()
        
        soup = BeautifulSoup(html, "lxml")
        metrics = CoinGlassMetrics()
        
        # Extract based on page type
        if "/currencies/BTC" in url or "/currencies/bitcoin" in url.lower():
            # BTC Overview page - check if it's derivatives snapshot or overview
            if "derivatives" in url.lower() or "snapshot" in url.lower():
                metrics = self._extract_derivatives_snapshot(soup, html, js_data)
            else:
                metrics = self._extract_btc_overview(soup, html, js_data)
        elif "liquidations" in url.lower() or "liquidationdata" in url.lower():
            # Liquidations page
            metrics = self._extract_liquidations(soup, html, js_data)
        elif "inflow" in url.lower() or "outflow" in url.lower():
            # Spot Inflow/Outflow page
            metrics = self._extract_spot_flows(soup, html, js_data)
        elif "volatility" in url.lower():
            # Volatility page
            metrics = self._extract_volatility(soup, html, js_data)
        else:
            # Try to extract all metrics from any page
            metrics = self._extract_all_metrics(soup, html, js_data)
        
        # Convert to DataFrame
        data_dict = {
            "timestamp": pd.Timestamp.now(),
        }
        
        # Add all non-None metrics
        for field_name, value in metrics.__dict__.items():
            if value is not None:
                data_dict[field_name] = value
        
        if not data_dict or len(data_dict) == 1:  # Only timestamp
            self.logger.warning("No metrics extracted from page")
            return pd.DataFrame()
        
        df = pd.DataFrame([data_dict])
        
        # Normalize financial values
        df = self.normalizer.normalize_dataframe(df)
        
        return df
    
    def _extract_btc_overview(
        self,
        soup: BeautifulSoup,
        html: str,
        js_data: Optional[Dict],
    ) -> CoinGlassMetrics:
        """Extract metrics from BTC Overview page."""
        metrics = CoinGlassMetrics()
        
        # Try multiple extraction methods
        # Method 1: Extract from text patterns
        patterns = {
            "btc_price": [
                r'\$?([\d,]+\.?\d*)\s*BTC',
                r'BTC[:\s]*\$?([\d,]+\.?\d*)',
                r'Price[:\s]*\$?([\d,]+\.?\d*)',
            ],
            "futures_volume_24h": [
                r'Futures\s+Volume[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
                r'24h\s+Futures[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
            ],
            "spot_volume_24h": [
                r'Spot\s+Volume[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
                r'24h\s+Spot[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
            ],
            "open_interest": [
                r'Open\s+Interest[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
                r'OI[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
                r'Open\s+Interest[:\s]*([\d,]+\.?\d*[BMK]?)\s*USD',
                r'Total\s+OI[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
                r'Open\s+Interest\s+\(24h\)[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
            ],
            "net_inflow_24h": [
                r'Net\s+Inflow[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
                r'24h\s+Net\s+Inflow[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
            ],
        }
        
        for field_name, pattern_list in patterns.items():
            for pattern in pattern_list:
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    # Net inflow can be negative, so allow it
                    value = self._parse_numeric_value(match.group(1), allow_negative=True)
                    if value is not None:
                        setattr(metrics, field_name, value)
                        break
        
        # Method 2: Extract from data attributes or specific selectors
        # Look for elements with data attributes containing values
        value_elements = soup.find_all(attrs={"data-value": True})
        for elem in value_elements:
            text = elem.get_text(strip=True)
            data_value = elem.get("data-value")
            
            # Try to match to field based on context
            parent_text = ""
            if elem.parent:
                parent_text = elem.parent.get_text()
            
            if "price" in parent_text.lower() and metrics.btc_price is None:
                metrics.btc_price = self._parse_numeric_value(data_value or text, allow_negative=False)
            elif "futures" in parent_text.lower() and "volume" in parent_text.lower():
                if metrics.futures_volume_24h is None:
                    metrics.futures_volume_24h = self._parse_numeric_value(data_value or text, allow_negative=False)
            elif "spot" in parent_text.lower() and "volume" in parent_text.lower():
                if metrics.spot_volume_24h is None:
                    metrics.spot_volume_24h = self._parse_numeric_value(data_value or text, allow_negative=False)
            elif "open" in parent_text.lower() and "interest" in parent_text.lower():
                if metrics.open_interest is None:
                    metrics.open_interest = self._parse_numeric_value(data_value or text, allow_negative=False)
            elif "inflow" in parent_text.lower() or "outflow" in parent_text.lower():
                if metrics.net_inflow_24h is None:
                    metrics.net_inflow_24h = self._parse_numeric_value(data_value or text, allow_negative=True)
        
        # Method 2b: Enhanced CSS selector search for Open Interest
        # Look for common CoinGlass card/stat patterns
        oi_selectors = [
            '[class*="open-interest"]',
            '[class*="OpenInterest"]',
            '[class*="oi"]',
            '[data-testid*="open-interest"]',
            '[data-testid*="oi"]',
            'div:contains("Open Interest")',
        ]
        
        for selector in oi_selectors:
            try:
                elements = soup.select(selector)
                for elem in elements:
                    text = elem.get_text()
                    # Look for numbers in the element or its children
                    numbers = re.findall(r'[\d,]+\.?\d*[BMK]?', text)
                    for num_str in numbers:
                        value = self._parse_numeric_value(num_str)
                        if value is not None and value > 0:
                            if metrics.open_interest is None:
                                metrics.open_interest = value
                                self.logger.debug(f"Found Open Interest via selector {selector}: {value}")
                                break
                    if metrics.open_interest is not None:
                        break
            except Exception as e:
                self.logger.debug(f"Selector {selector} failed: {e}")
            
            if metrics.open_interest is not None:
                break
        
        # Method 2c: Search in text content for OI patterns near numbers
        text_content = soup.get_text()
        oi_patterns = [
            r'Open\s+Interest[:\s]*\$?\s*([\d,]+\.?\d*[BMK]?)',
            r'OI[:\s]*\$?\s*([\d,]+\.?\d*[BMK]?)',
            r'Total\s+Open\s+Interest[:\s]*\$?\s*([\d,]+\.?\d*[BMK]?)',
        ]
        for pattern in oi_patterns:
            matches = re.finditer(pattern, text_content, re.IGNORECASE)
            for match in matches:
                value = self._parse_numeric_value(match.group(1))
                if value is not None and value > 0:
                    if metrics.open_interest is None:
                        metrics.open_interest = value
                        self.logger.debug(f"Found Open Interest via text pattern: {value}")
                        break
            if metrics.open_interest is not None:
                break
        
        # Method 3: Extract from JavaScript data
        if js_data:
            metrics = self._extract_from_js_data(js_data, metrics)
        
        # Method 4: Extract from API responses captured during page load
        if js_data and "api_responses" in js_data:
            for api_resp in js_data["api_responses"]:
                api_data = api_resp.get("data", {})
                metrics = self._extract_from_api_response(api_data, metrics)
        
        return metrics
    
    def _extract_from_api_response(
        self,
        api_data: Dict,
        metrics: CoinGlassMetrics,
    ) -> CoinGlassMetrics:
        """Extract metrics from API response data."""
        def find_value(data, keys):
            """Recursively search for value by key names."""
            if isinstance(data, dict):
                for key, value in data.items():
                    key_lower = key.lower()
                    # Check if key matches any search term
                    if any(k.lower() in key_lower for k in keys):
                        if isinstance(value, (int, float)):
                            return value
                        elif isinstance(value, str):
                            parsed = self._parse_numeric_value(value)
                            if parsed is not None:
                                return parsed
                    elif isinstance(value, (dict, list)):
                        result = find_value(value, keys)
                        if result is not None:
                            return result
            elif isinstance(data, list):
                for item in data:
                    result = find_value(item, keys)
                    if result is not None:
                        return result
            return None
        
        # Map field names to search keys
        field_mappings = {
            "open_interest": ["open", "interest", "oi", "openinterest", "total_oi"],
            "btc_price": ["price", "btc", "bitcoin"],
            "futures_volume_24h": ["futures", "volume", "24h"],
            "spot_volume_24h": ["spot", "volume", "24h"],
            "net_inflow_24h": ["inflow", "net", "24h"],
            # Derivatives snapshot fields
            "futures_oi_all_exchanges": ["futures", "oi", "all", "exchanges"],
            "cme_btc_oi": ["cme", "btc", "oi"],
            "binance_btc_oi": ["binance", "btc", "oi"],
            "btc_options_calls_oi": ["btc", "options", "calls", "oi"],
            "btc_options_puts_oi": ["btc", "options", "puts", "oi"],
            # Liquidations fields
            "total_liquidations_24h": ["total", "liquidations", "24h"],
            "long_liquidations": ["long", "liquidations"],
            "short_liquidations": ["short", "liquidations"],
            "btc_liquidations_24h": ["btc", "liquidations", "24h"],
            "eth_liquidations_24h": ["eth", "liquidations", "24h"],
        }
        
        for field_name, search_keys in field_mappings.items():
            if getattr(metrics, field_name) is None:
                value = find_value(api_data, search_keys)
                if value is not None:
                    setattr(metrics, field_name, value)
                    self.logger.debug(f"Extracted {field_name} from API response: {value}")
        
        return metrics
    
    def _extract_derivatives_snapshot(
        self,
        soup: BeautifulSoup,
        html: str,
        js_data: Optional[Dict],
    ) -> CoinGlassMetrics:
        """Extract derivatives snapshot metrics (Futures OI, Options OI, etc.)."""
        metrics = CoinGlassMetrics()
        
        # Patterns for derivatives metrics
        patterns = {
            "futures_oi_all_exchanges": [
                r'Futures\s+OI[:\s]*\(All\s+Exchanges\)[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
                r'Total\s+Futures\s+OI[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
                r'Futures\s+Open\s+Interest[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
            ],
            "cme_btc_oi": [
                r'CME\s+BTC\s+OI[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
                r'CME[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
            ],
            "binance_btc_oi": [
                r'Binance\s+BTC\s+OI[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
                r'Binance[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
            ],
            "btc_options_calls_oi": [
                r'BTC\s+Options\s+Calls\s+OI[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
                r'Calls\s+OI[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
            ],
            "btc_options_puts_oi": [
                r'BTC\s+Options\s+Puts\s+OI[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
                r'Puts\s+OI[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
            ],
        }
        
        # Extract using patterns (don't allow negative for OI)
        for field_name, pattern_list in patterns.items():
            for pattern in pattern_list:
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    value = self._parse_numeric_value(match.group(1), allow_negative=False)
                    if value is not None and value > 0:
                        setattr(metrics, field_name, value)
                        self.logger.debug(f"Extracted {field_name}: {value}")
                        break
        
        # Extract from DOM elements
        text_content = soup.get_text()
        for field_name in ["futures_oi_all_exchanges", "cme_btc_oi", "binance_btc_oi", 
                          "btc_options_calls_oi", "btc_options_puts_oi"]:
            if getattr(metrics, field_name) is None:
                # Try to find in text with context
                field_label = field_name.replace("_", " ").title()
                pattern = rf'{field_label}[:\s]*\$?([\d,]+\.?\d*[BMK]?)'
                match = re.search(pattern, text_content, re.IGNORECASE)
                if match:
                    value = self._parse_numeric_value(match.group(1), allow_negative=False)
                    if value is not None and value > 0:
                        setattr(metrics, field_name, value)
        
        # Extract from JS data
        if js_data:
            metrics = self._extract_from_js_data(js_data, metrics)
            # Also try API responses
            if "api_responses" in js_data:
                for api_resp in js_data["api_responses"]:
                    api_data = api_resp.get("data", {})
                    metrics = self._extract_from_api_response(api_data, metrics)
        
        # Post-extraction validation: ensure all OI values are positive
        for field_name in ["futures_oi_all_exchanges", "cme_btc_oi", "binance_btc_oi",
                          "btc_options_calls_oi", "btc_options_puts_oi"]:
            value = getattr(metrics, field_name)
            if value is not None and value < 0:
                self.logger.warning(
                    f"Rejecting negative {field_name} value: {value}. "
                    "This indicates incorrect extraction."
                )
                setattr(metrics, field_name, None)
        
        return metrics
    
    def _extract_liquidations(
        self,
        soup: BeautifulSoup,
        html: str,
        js_data: Optional[Dict],
    ) -> CoinGlassMetrics:
        """Extract liquidations metrics from CoinGlass liquidations page."""
        metrics = CoinGlassMetrics()
        
        # First, try to extract from API responses (most reliable)
        if js_data and "api_responses" in js_data:
            for api_resp in js_data["api_responses"]:
                api_data = api_resp.get("data", {})
                resp_url = api_resp.get("url", "")
                
                # Look for liquidation-related API endpoints
                if "liquidation" in resp_url.lower() or "liquid" in resp_url.lower():
                    self.logger.info(f"Found liquidation API response: {resp_url}")
                    
                    # Try to extract from JSON response
                    if isinstance(api_data, dict):
                        metrics = self._extract_liquidations_from_api(api_data, metrics)
                    # Try to extract from text/CSV response
                    elif isinstance(api_data, str):
                        # Try to parse as JSON if it's JSON string
                        try:
                            json_data = json.loads(api_data)
                            metrics = self._extract_liquidations_from_api(json_data, metrics)
                        except:
                            # Try to extract from text patterns
                            metrics = self._extract_liquidations_from_text(api_data, metrics)
        
        # Patterns for liquidations metrics (fallback to HTML extraction)
        patterns = {
            "total_liquidations_24h": [
                r'total\s+liquidations[:\s]*comes\s+in\s+at\s+\$?([\d,]+\.?\d*)\s*million',  # Match "million" text FIRST (most specific)
                r'24h\s+Rekt[^>]*Total[^>]*Rekt[:\s]*\$?([\d,]+\.?\d*[BMK]?)',  # Match from 24h Rekt card
                r'Total\s+Liquidations[:\s]*\(24h\)[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
                r'Total\s+Liquidations[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
                r'24h\s+Liquidations[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
            ],
            "long_liquidations": [
                r'24h\s+Rekt[^>]*Long[:\s]*\$?([\d,]+\.?\d*[BMK]?)',  # Match Long from 24h Rekt card
                r'Long[:\s]*\$?([\d,]+\.?\d*[BMK]?)[^<]*24h',  # Match Long with 24h context
                r'Long\s+Liquidations[:\s]*\(24h\)[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
                r'Long\s+Liquidations[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
                # Removed generic "Long[:\s]*\$?" pattern - it matches "Long1" incorrectly
            ],
            "short_liquidations": [
                r'24h\s+Rekt[^>]*Short[:\s]*\$?([\d,]+\.?\d*[BMK]?)',  # Match Short from 24h Rekt card
                r'Short[:\s]*\$?([\d,]+\.?\d*[BMK]?)[^<]*24h',  # Match Short with 24h context
                r'Short\s+Liquidations[:\s]*\(24h\)[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
                r'Short\s+Liquidations[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
                # Removed generic "Short[:\s]*\$?" pattern - it matches "Short1" incorrectly
            ],
            "btc_liquidations_24h": [
                r'BTC\s+Liquidations[:\s]*\(24h\)[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
                r'BTC[:\s]*Liquidations[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
                r'BTC[:\s]*24h\s+Long[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
            ],
            "eth_liquidations_24h": [
                r'ETH\s+Liquidations[:\s]*\(24h\)[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
                r'ETH[:\s]*Liquidations[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
                r'ETH[:\s]*24h\s+Long[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
            ],
        }
        
        # Extract from DOM elements FIRST - look for 24h Rekt card specifically
        # This should be done before HTML patterns to avoid matching wrong values
        text_content = soup.get_text()
        html_lower = html.lower()
        
        # First, try to find the 24h Rekt card element in the HTML structure
        rekt_24h_section = None
        rekt_24h_text = None
        
        # Look for elements containing "24h" and "Rekt" or "24h Rekt"
        for elem in soup.find_all(['div', 'section', 'article', 'card'], class_=re.compile(r'24h|rekt', re.I)):
            elem_text = elem.get_text()
            if '24h' in elem_text.lower() and 'rekt' in elem_text.lower():
                rekt_24h_section = elem
                rekt_24h_text = elem_text
                break
        
        # If not found by class, search by text content
        if not rekt_24h_section:
            for elem in soup.find_all(['div', 'section', 'article']):
                elem_text = elem.get_text()
                # Check if this element contains "24h Rekt" and has numeric values
                if '24h' in elem_text.lower() and 'rekt' in elem_text.lower() and '$' in elem_text:
                    # Make sure it's the 24h one, not 1h, 4h, or 12h
                    if re.search(r'\b24h\b', elem_text, re.I) and not re.search(r'\b(1h|4h|12h)\b', elem_text, re.I):
                        rekt_24h_section = elem
                        rekt_24h_text = elem_text
                        break
        
        
        # Use the 24h section text if found, otherwise use full text
        search_text = rekt_24h_text if rekt_24h_text else text_content
        
        # Try to find 24h Rekt card specifically - look for the card structure
        # Pattern: "24h Rekt" followed by Total Rekt value, then Long, then Short
        rekt_24h_patterns = [
            r'24h\s+rekt[^0-9]*total[^0-9]*rekt[:\s]*\$?([\d,]+\.?\d*[bmk]?)[^0-9]*long[:\s]*\$?([\d,]+\.?\d*[bmk]?)[^0-9]*short[:\s]*\$?([\d,]+\.?\d*[bmk]?)',
            r'24h\s+rekt[^$]*\$?([\d,]+\.?\d*[bmk]?)[^$]*long[:\s]*\$?([\d,]+\.?\d*[bmk]?)[^$]*short[:\s]*\$?([\d,]+\.?\d*[bmk]?)',
        ]
        rekt_match = None
        for pattern in rekt_24h_patterns:
            rekt_match = re.search(pattern, search_text, re.IGNORECASE | re.DOTALL)
            if rekt_match:
                break
        if rekt_match:
            # Always extract from rekt_match - it's the most reliable source
            # Overwrite any existing values (they might be wrong from earlier patterns)
            total_val = self._parse_numeric_value(rekt_match.group(1), allow_negative=False)
            long_val = self._parse_numeric_value(rekt_match.group(2), allow_negative=False)
            short_val = self._parse_numeric_value(rekt_match.group(3), allow_negative=False)
            
            if total_val is not None:
                metrics.total_liquidations_24h = total_val
            if long_val is not None and long_val > 0:
                metrics.long_liquidations = long_val
            if short_val is not None and short_val > 0:
                metrics.short_liquidations = short_val
        
        # Extract using patterns from HTML (if not already extracted from API or rekt_match)
        
        for field_name, pattern_list in patterns.items():
            if getattr(metrics, field_name) is None:
                for pattern_idx, pattern in enumerate(pattern_list):
                    match = re.search(pattern, html, re.IGNORECASE)
                    if match:
                        value = self._parse_numeric_value(match.group(1), allow_negative=False)
                        if value is not None:
                            # For long/short, reject values that are too small (likely wrong matches)
                            if field_name in ["long_liquidations", "short_liquidations"]:
                                if value < 10:  # Reject values less than 10 (likely wrong matches like "Long1")
                                    continue
                            # For total_liquidations_24h, check if pattern matched "million" and multiply
                            if field_name == "total_liquidations_24h" and "million" in pattern.lower():
                                value = value * 1e6
                            setattr(metrics, field_name, value)
                            self.logger.debug(f"Extracted {field_name} from HTML: {value}")
                            break
        
        # Also try individual patterns for each field - prioritize 24h context
        for field_name in ["total_liquidations_24h", "long_liquidations", "short_liquidations",
                          "btc_liquidations_24h", "eth_liquidations_24h"]:
            if getattr(metrics, field_name) is None:
                # Try specific patterns for 24h liquidations
                if field_name == "long_liquidations":
                    # Look for "Long: $X.XXM" specifically in 24h Rekt card context
                    long_patterns = [
                        r'24h\s+rekt[^$]*long[:\s]*\$?([\d,]+\.?\d*[bmk]?)',  # From 24h Rekt card
                        r'long[:\s]*\$?([\d,]+\.?\d*[bmk]?)[^<]*24h\s+rekt',  # Long with 24h Rekt after
                        r'24h[^$]*long[:\s]*\$?([\d,]+\.?\d*[bmk]?)',  # Any 24h context with Long
                    ]
                    for pattern in long_patterns:
                        match = re.search(pattern, search_text, re.IGNORECASE | re.DOTALL)
                        if match:
                            value = self._parse_numeric_value(match.group(1), allow_negative=False)
                            if value is not None and value > 0:
                                setattr(metrics, field_name, value)
                                break
                elif field_name == "short_liquidations":
                    # Look for "Short: $X.XXM" specifically in 24h Rekt card context
                    short_patterns = [
                        r'24h\s+rekt[^$]*short[:\s]*\$?([\d,]+\.?\d*[bmk]?)',  # From 24h Rekt card
                        r'short[:\s]*\$?([\d,]+\.?\d*[bmk]?)[^<]*24h\s+rekt',  # Short with 24h Rekt after
                        r'24h[^$]*short[:\s]*\$?([\d,]+\.?\d*[bmk]?)',  # Any 24h context with Short
                    ]
                    for pattern in short_patterns:
                        match = re.search(pattern, search_text, re.IGNORECASE | re.DOTALL)
                        if match:
                            value = self._parse_numeric_value(match.group(1), allow_negative=False)
                            if value is not None and value > 0:
                                setattr(metrics, field_name, value)
                                break
                elif field_name == "total_liquidations_24h":
                    # Look for "comes in at $X.XX million" or from 24h Rekt card
                    # Try full text first for "comes in at" pattern, then 24h section
                    total_patterns = [
                        (r'comes\s+in\s+at\s+\$?([\d,]+\.?\d*)\s*million', text_content),  # "comes in at $X.XX million" - check full text
                        (r'total\s+liquidations[:\s]*comes\s+in\s+at\s+\$?([\d,]+\.?\d*[bmk]?)', text_content),  # Alternative format
                        (r'24h\s+rekt[^$]*total[^$]*rekt[:\s]*\$?([\d,]+\.?\d*[bmk]?)', search_text),  # From 24h Rekt card
                        (r'24h\s+rekt[^$]*\$?([\d,]+\.?\d*[bmk]?)[^$]*total', search_text),  # Total value in 24h Rekt
                    ]
                    for pattern, search_in in total_patterns:
                        match = re.search(pattern, search_in, re.IGNORECASE | re.DOTALL)
                        if match:
                            value = self._parse_numeric_value(match.group(1), allow_negative=False)
                            if value is not None:
                                # If pattern matched "million" text, multiply by 1e6
                                if "million" in pattern.lower():
                                    value = value * 1e6
                                setattr(metrics, field_name, value)
                                break
                else:
                    # Fallback to generic pattern
                    field_label = field_name.replace("_", " ").title()
                    pattern = rf'{field_label}[:\s]*\$?([\d,]+\.?\d*[BMK]?)'
                    match = re.search(pattern, text_content, re.IGNORECASE)
                    if match:
                        value = self._parse_numeric_value(match.group(1), allow_negative=False)
                        if value is not None:
                            setattr(metrics, field_name, value)
        
        # Extract from JS data
        if js_data:
            metrics = self._extract_from_js_data(js_data, metrics)
        
        return metrics
    
    def _extract_liquidations_from_api(
        self,
        api_data: Dict,
        metrics: CoinGlassMetrics,
    ) -> CoinGlassMetrics:
        """Extract liquidations from API response data."""
        def find_value(data, keys):
            """Recursively search for value by key names."""
            if isinstance(data, dict):
                for key, value in data.items():
                    key_lower = key.lower()
                    if any(k.lower() in key_lower for k in keys):
                        if isinstance(value, (int, float)):
                            return value
                        elif isinstance(value, str):
                            parsed = self._parse_numeric_value(value, allow_negative=False)
                            if parsed is not None:
                                return parsed
                    elif isinstance(value, (dict, list)):
                        result = find_value(value, keys)
                        if result is not None:
                            return result
            elif isinstance(data, list):
                for item in data:
                    result = find_value(item, keys)
                    if result is not None:
                        return result
            return None
        
        # Map field names to search keys
        field_mappings = {
            "total_liquidations_24h": ["total", "liquidations", "24h", "24", "totalLiquidations"],
            "long_liquidations": ["long", "liquidations", "longLiquidations"],
            "short_liquidations": ["short", "liquidations", "shortLiquidations"],
            "btc_liquidations_24h": ["btc", "liquidations", "24h", "bitcoin"],
            "eth_liquidations_24h": ["eth", "liquidations", "24h", "ethereum"],
        }
        
        for field_name, search_keys in field_mappings.items():
            if getattr(metrics, field_name) is None:
                value = find_value(api_data, search_keys)
                if value is not None:
                    setattr(metrics, field_name, value)
                    self.logger.debug(f"Extracted {field_name} from API: {value}")
        
        return metrics
    
    def _extract_liquidations_from_text(
        self,
        text: str,
        metrics: CoinGlassMetrics,
    ) -> CoinGlassMetrics:
        """Extract liquidations from text/CSV data."""
        # Try to find liquidation values in text
        patterns = {
            "total_liquidations_24h": r'total[:\s]*liquidations[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
            "long_liquidations": r'long[:\s]*liquidations[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
            "short_liquidations": r'short[:\s]*liquidations[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
        }
        
        for field_name, pattern in patterns.items():
            if getattr(metrics, field_name) is None:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    value = self._parse_numeric_value(match.group(1), allow_negative=False)
                    if value is not None:
                        setattr(metrics, field_name, value)
        
        return metrics
    
    def _extract_spot_flows(
        self,
        soup: BeautifulSoup,
        html: str,
        js_data: Optional[Dict],
    ) -> CoinGlassMetrics:
        """Extract spot inflow/outflow metrics."""
        metrics = CoinGlassMetrics()
        
        # Extract different timeframes
        timeframes = {
            "net_inflow_5min": r'5\s*min[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
            "net_inflow_1h": r'1\s*h[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
            "net_inflow_4h": r'4\s*h[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
            "net_inflow_12h": r'12\s*h[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
            "net_inflow_24h": r'24\s*h[:\s]*\$?([\d,]+\.?\d*[BMK]?)',
        }
        
        for field_name, pattern in timeframes.items():
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                # Net inflow can be negative
                value = self._parse_numeric_value(match.group(1), allow_negative=True)
                if value is not None:
                    setattr(metrics, field_name, value)
        
        # Also try to extract from JS data
        if js_data:
            metrics = self._extract_from_js_data(js_data, metrics)
        
        return metrics
    
    def _extract_volatility(
        self,
        soup: BeautifulSoup,
        html: str,
        js_data: Optional[Dict],
    ) -> CoinGlassMetrics:
        """Extract volatility metrics for different coins."""
        metrics = CoinGlassMetrics()
        
        # Extract volatility for each coin
        coins = {
            "btc_volatility_1d": ["BTC", "Bitcoin"],
            "eth_volatility_1d": ["ETH", "Ethereum"],
            "sol_volatility_1d": ["SOL", "Solana"],
            "xrp_volatility_1d": ["XRP", "Ripple"],
            "doge_volatility_1d": ["DOGE", "Dogecoin"],
        }
        
        for field_name, coin_names in coins.items():
            for coin_name in coin_names:
                pattern = rf'{coin_name}[:\s]*([\d,]+\.?\d*)\s*%?'
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    value = self._parse_numeric_value(match.group(1))
                    if value is not None:
                        setattr(metrics, field_name, value)
                        break
        
        # Also try to extract from JS data
        if js_data:
            metrics = self._extract_from_js_data(js_data, metrics)
        
        return metrics
    
    def _extract_all_metrics(
        self,
        soup: BeautifulSoup,
        html: str,
        js_data: Optional[Dict],
    ) -> CoinGlassMetrics:
        """Try to extract all metrics from any page."""
        # Combine all extraction methods
        metrics = self._extract_btc_overview(soup, html, js_data)
        flow_metrics = self._extract_spot_flows(soup, html, js_data)
        vol_metrics = self._extract_volatility(soup, html, js_data)
        
        # Merge metrics (prefer non-None values)
        for field_name in CoinGlassMetrics.__dataclass_fields__:
            value = getattr(flow_metrics, field_name) or getattr(vol_metrics, field_name)
            if value is not None and getattr(metrics, field_name) is None:
                setattr(metrics, field_name, value)
        
        return metrics
    
    def _extract_from_js_data(
        self,
        js_data: Dict,
        metrics: CoinGlassMetrics,
    ) -> CoinGlassMetrics:
        """Extract metrics from JavaScript data objects."""
        # Try to find metrics in nested JS data structures
        def find_value(data, keys):
            """Recursively search for value by key names."""
            if isinstance(data, dict):
                for key, value in data.items():
                    if any(k.lower() in key.lower() for k in keys):
                        if isinstance(value, (int, float)):
                            return value
                        elif isinstance(value, str):
                            return self._parse_numeric_value(value)
                    elif isinstance(value, (dict, list)):
                        result = find_value(value, keys)
                        if result is not None:
                            return result
            elif isinstance(data, list):
                for item in data:
                    result = find_value(item, keys)
                    if result is not None:
                        return result
            return None
        
        # Map field names to search keys
        field_mappings = {
            "btc_price": ["price", "btc", "bitcoin"],
            "futures_volume_24h": ["futures", "volume", "24h"],
            "spot_volume_24h": ["spot", "volume", "24h"],
            "open_interest": ["open", "interest", "oi"],
            "net_inflow_24h": ["inflow", "net", "24h"],
        }
        
        for field_name, search_keys in field_mappings.items():
            if getattr(metrics, field_name) is None:
                value = find_value(js_data, search_keys)
                if value is not None:
                    setattr(metrics, field_name, value)
        
        return metrics
    
    def _parse_numeric_value(
        self, 
        value: str, 
        allow_negative: bool = False
    ) -> Optional[float]:
        """
        Parse numeric value from string, handling currency, suffixes, etc.
        
        Args:
            value: String value to parse
            allow_negative: Whether negative values are allowed (default: False)
        
        Returns:
            Parsed float value, or None if parsing fails
        """
        if not value:
            return None
        
        # Remove whitespace and currency symbols
        value = str(value).strip().replace("$", "").replace(",", "").replace(" ", "")
        
        # Handle negative values - check for various minus signs
        is_negative = False
        negative_signs = ["-", "–", "−", "—"]  # Regular, en-dash, minus, em-dash
        for sign in negative_signs:
            if value.startswith(sign):
                is_negative = True
                value = value[1:].strip()
                break
        
        # If negative not allowed, log warning and reject
        if is_negative and not allow_negative:
            self.logger.warning(
                f"Negative value detected but not allowed: {value}. "
                "This may indicate incorrect extraction."
            )
            return None
        
        # Handle suffixes (B, M, K)
        multipliers = {
            "B": 1e9,
            "b": 1e9,
            "M": 1e6,
            "m": 1e6,
            "K": 1e3,
            "k": 1e3,
        }
        
        multiplier = 1
        for suffix, mult in multipliers.items():
            if value.endswith(suffix):
                multiplier = mult
                value = value[:-1]
                break
        
        try:
            num = float(value) * multiplier
            result = -num if is_negative else num
            
            # Validate: reject negative values if not allowed (double-check)
            if result < 0 and not allow_negative:
                self.logger.warning(
                    f"Rejecting negative value: {result} (allow_negative={allow_negative})"
                )
                return None
            
            # Validate: reject suspiciously small or large values
            if result < 0.01 and result > 0:  # Very small positive values might be errors
                self.logger.debug(f"Very small value detected: {result}")
            
            return result
        except ValueError:
            self.logger.debug(f"Failed to parse numeric value: {value}")
            return None

