"""
DOM extractor for financial metrics from web pages.
Extracts data from CSS selectors, data attributes, and text patterns.
"""

import re
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

import pandas as pd
from bs4 import BeautifulSoup

from ..utils.logger import get_logger
from ..utils.browser import BrowserManager


@dataclass
class ExtractionSelector:
    """Selector configuration for extracting a field."""
    field_name: str
    css_selector: Optional[str] = None
    data_attribute: Optional[str] = None
    text_pattern: Optional[str] = None
    js_variable: Optional[str] = None
    attribute_name: Optional[str] = None  # For extracting from element attributes


class DomExtractor:
    """
    Extractor for financial metrics from DOM elements.
    Handles CSS selectors, data attributes, text patterns, and JavaScript variables.
    """
    
    def __init__(self):
        self.logger = get_logger()
    
    def extract_by_selectors(
        self,
        html: str,
        selectors: Dict[str, ExtractionSelector],
    ) -> pd.DataFrame:
        """
        Extract multiple fields by CSS selectors or other methods.
        
        Args:
            html: HTML content
            selectors: Dictionary mapping field names to ExtractionSelector configs
        
        Returns:
            DataFrame with extracted fields
        """
        soup = BeautifulSoup(html, "lxml")
        extracted_data = {}
        
        for field_name, selector_config in selectors.items():
            value = None
            
            # Method 1: CSS selector
            if selector_config.css_selector:
                value = self._extract_by_css_selector(soup, selector_config)
            
            # Method 2: Data attribute
            elif selector_config.data_attribute:
                value = self._extract_by_data_attribute(soup, selector_config)
            
            # Method 3: Text pattern (regex)
            elif selector_config.text_pattern:
                value = self._extract_by_text_pattern(html, selector_config)
            
            # Method 4: JavaScript variable (requires browser evaluation)
            elif selector_config.js_variable:
                # This would need browser context - handled separately
                value = None
            
            if value is not None:
                extracted_data[field_name] = value
        
        if extracted_data:
            return pd.DataFrame([extracted_data])
        return pd.DataFrame()
    
    def extract_from_browser(
        self,
        browser_manager: BrowserManager,
        url: str,
        selectors: Dict[str, ExtractionSelector],
        wait_for_selector: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Extract data from a page using browser automation.
        
        Args:
            browser_manager: BrowserManager instance
            url: URL to load
            selectors: Dictionary mapping field names to ExtractionSelector configs
            wait_for_selector: CSS selector to wait for before extraction
        
        Returns:
            DataFrame with extracted fields
        """
        import asyncio
        
        async def _extract():
            async with browser_manager:
                page = await browser_manager._context.new_page()
                try:
                    await page.goto(url, timeout=30000, wait_until="networkidle")
                    
                    if wait_for_selector:
                        await page.wait_for_selector(wait_for_selector, timeout=10000)
                    
                    # Wait a bit for dynamic content
                    await asyncio.sleep(2)
                    
                    # Get HTML
                    html = await page.content()
                    soup = BeautifulSoup(html, "lxml")
                    
                    extracted_data = {}
                    
                    for field_name, selector_config in selectors.items():
                        value = None
                        
                        # Try CSS selector first
                        if selector_config.css_selector:
                            value = self._extract_by_css_selector(soup, selector_config)
                        
                        # Try data attribute
                        if value is None and selector_config.data_attribute:
                            value = self._extract_by_data_attribute(soup, selector_config)
                        
                        # Try JavaScript variable evaluation
                        if value is None and selector_config.js_variable:
                            try:
                                script = f"JSON.stringify({selector_config.js_variable})"
                                result = await page.evaluate(script)
                                if result:
                                    import json
                                    value = json.loads(result)
                            except Exception as e:
                                self.logger.debug(f"JS variable extraction failed: {e}")
                        
                        # Try text pattern
                        if value is None and selector_config.text_pattern:
                            value = self._extract_by_text_pattern(html, selector_config)
                        
                        if value is not None:
                            extracted_data[field_name] = value
                    
                    return pd.DataFrame([extracted_data]) if extracted_data else pd.DataFrame()
                    
                finally:
                    await page.close()
            
            return pd.DataFrame()
        
        return asyncio.run(_extract())
    
    def _extract_by_css_selector(
        self,
        soup: BeautifulSoup,
        selector_config: ExtractionSelector,
    ) -> Optional[Any]:
        """Extract value using CSS selector."""
        try:
            elements = soup.select(selector_config.css_selector)
            if not elements:
                return None
            
            # Get text from first element
            element = elements[0]
            
            # If attribute_name is specified, get from attribute
            if selector_config.attribute_name:
                return element.get(selector_config.attribute_name)
            
            # Otherwise get text
            text = element.get_text(strip=True)
            return self._parse_value(text)
        except Exception as e:
            self.logger.debug(f"CSS selector extraction failed: {e}")
            return None
    
    def _extract_by_data_attribute(
        self,
        soup: BeautifulSoup,
        selector_config: ExtractionSelector,
    ) -> Optional[Any]:
        """Extract value from data attribute."""
        try:
            # Find element with the data attribute
            attr_name = f"data-{selector_config.data_attribute}"
            element = soup.find(attrs={attr_name: True})
            
            if element:
                value = element.get(attr_name)
                return self._parse_value(value)
        except Exception as e:
            self.logger.debug(f"Data attribute extraction failed: {e}")
        return None
    
    def _extract_by_text_pattern(
        self,
        html: str,
        selector_config: ExtractionSelector,
    ) -> Optional[Any]:
        """Extract value using regex pattern."""
        try:
            pattern = selector_config.text_pattern
            match = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
            if match:
                # Try to extract the value from the match
                value = match.group(1) if match.groups() else match.group(0)
                return self._parse_value(value)
        except Exception as e:
            self.logger.debug(f"Text pattern extraction failed: {e}")
        return None
    
    def _parse_value(self, value: str) -> Any:
        """
        Parse extracted text value into appropriate type.
        Handles currency symbols, large numbers (K/M/B), percentages, etc.
        """
        if not value:
            return None
        
        # Remove whitespace
        value = value.strip()
        
        # Handle currency symbols
        if value.startswith("$"):
            value = value[1:].strip()
        
        # Handle percentages
        if value.endswith("%"):
            try:
                return float(value[:-1].replace(",", ""))
            except ValueError:
                return value
        
        # Handle large number suffixes (K, M, B)
        multipliers = {
            "B": 1e9,
            "b": 1e9,
            "M": 1e6,
            "m": 1e6,
            "K": 1e3,
            "k": 1e3,
        }
        
        for suffix, mult in multipliers.items():
            if value.endswith(suffix):
                try:
                    num = float(value[:-1].replace(",", ""))
                    return num * mult
                except ValueError:
                    pass
        
        # Handle negative values
        is_negative = value.startswith("-") or value.startswith("–")
        if is_negative:
            value = value[1:].strip()
        
        # Try to parse as number
        try:
            # Remove commas
            cleaned = value.replace(",", "")
            num = float(cleaned)
            return -num if is_negative else num
        except ValueError:
            # Return as string if not a number
            return f"-{value}" if is_negative else value



