"""
XML data extractor for the data-fetch framework.
Supports XPath queries and handles RSS feeds and XML-RPC responses.
"""

import re
from typing import Union, Optional, List, Dict, Any
from pathlib import Path

import pandas as pd
from lxml import etree

from ..utils.logger import get_logger


class XmlExtractor:
    """
    Extractor for XML data.
    Supports XPath queries for data extraction.
    """
    
    def __init__(self):
        self.logger = get_logger()
    
    def extract(
        self,
        data: Union[str, bytes, Path],
        xpath: Optional[str] = None,
        root_tag: Optional[str] = None,
        encoding: str = "utf-8",
    ) -> pd.DataFrame:
        """
        Extract data from XML and return as DataFrame.
        
        Args:
            data: XML data (string, bytes, or file path)
            xpath: XPath expression to extract data (auto-detect if None)
            root_tag: Root tag to extract from (auto-detect if None)
            encoding: Text encoding (default: utf-8)
        
        Returns:
            Extracted DataFrame
        """
        # Parse XML
        if isinstance(data, Path):
            tree = etree.parse(str(data))
            root = tree.getroot()
        elif isinstance(data, bytes):
            root = etree.fromstring(data)
        else:
            root = etree.fromstring(data.encode(encoding))
        
        # Extract data
        if xpath:
            rows = self._extract_with_xpath(root, xpath)
        elif root_tag:
            rows = self._extract_by_tag(root, root_tag)
        else:
            rows = self._auto_extract(root)
        
        if not rows:
            self.logger.warning("No data extracted from XML")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(rows)
        
        self.logger.info(f"Extracted XML data with {len(df)} rows, {len(df.columns)} columns")
        return df
    
    def _extract_with_xpath(self, root: etree.Element, xpath: str) -> List[Dict[str, Any]]:
        """
        Extract data using XPath expression.
        
        Args:
            root: XML root element
            xpath: XPath expression
        
        Returns:
            List of dictionaries (rows)
        """
        rows = []
        elements = root.xpath(xpath)
        
        for elem in elements:
            row = {}
            
            # Extract text content
            if elem.text and elem.text.strip():
                row["value"] = elem.text.strip()
            
            # Extract attributes
            row.update(elem.attrib)
            
            # Extract child elements
            for child in elem:
                tag = self._clean_tag_name(child.tag)
                value = child.text.strip() if child.text else ""
                if value:
                    row[tag] = value
                    # Also include attributes
                    for attr, attr_value in child.attrib.items():
                        row[f"{tag}_{attr}"] = attr_value
            
            if row:
                rows.append(row)
        
        return rows
    
    def _extract_by_tag(self, root: etree.Element, tag: str) -> List[Dict[str, Any]]:
        """
        Extract data by tag name.
        
        Args:
            root: XML root element
            tag: Tag name to extract
        
        Returns:
            List of dictionaries (rows)
        """
        rows = []
        elements = root.findall(f".//{tag}")
        
        for elem in elements:
            row = {}
            
            # Extract text
            if elem.text and elem.text.strip():
                row["value"] = elem.text.strip()
            
            # Extract attributes
            row.update(elem.attrib)
            
            # Extract child elements
            for child in elem:
                child_tag = self._clean_tag_name(child.tag)
                value = child.text.strip() if child.text else ""
                if value:
                    row[child_tag] = value
            
            if row:
                rows.append(row)
        
        return rows
    
    def _auto_extract(self, root: etree.Element) -> List[Dict[str, Any]]:
        """
        Automatically extract data from XML structure.
        
        Args:
            root: XML root element
        
        Returns:
            List of dictionaries (rows)
        """
        # Look for common patterns
        # Pattern 1: Array of similar elements
        children = list(root)
        if children and len(children) > 1:
            # Check if all children have the same tag
            first_tag = children[0].tag
            if all(child.tag == first_tag for child in children):
                return self._extract_by_tag(root, first_tag)
        
        # Pattern 2: RSS feed
        if root.tag == "rss" or root.tag.endswith("}rss"):
            return self._extract_rss(root)
        
        # Pattern 3: Single object with nested structure
        return self._extract_nested(root)
    
    def _extract_rss(self, root: etree.Element) -> List[Dict[str, Any]]:
        """Extract data from RSS feed."""
        rows = []
        
        # Find all items
        items = root.xpath(".//item")
        if not items:
            items = root.xpath(".//*[local-name()='item']")
        
        for item in items:
            row = {}
            
            # Extract common RSS fields
            for child in item:
                tag = self._clean_tag_name(child.tag)
                value = child.text.strip() if child.text else ""
                if value:
                    row[tag] = value
            
            if row:
                rows.append(row)
        
        return rows
    
    def _extract_nested(self, root: etree.Element) -> List[Dict[str, Any]]:
        """Extract nested XML structure."""
        rows = []
        row = {}
        
        def extract_element(elem: etree.Element, prefix: str = ""):
            """Recursively extract element data."""
            tag = self._clean_tag_name(elem.tag)
            full_tag = f"{prefix}_{tag}" if prefix else tag
            
            # Extract text
            if elem.text and elem.text.strip():
                row[full_tag] = elem.text.strip()
            
            # Extract attributes
            for attr, value in elem.attrib.items():
                row[f"{full_tag}_{attr}"] = value
            
            # Extract children
            for child in elem:
                extract_element(child, full_tag)
        
        extract_element(root)
        
        if row:
            rows.append(row)
        
        return rows
    
    def _clean_tag_name(self, tag: str) -> str:
        """Clean XML tag name (remove namespace, special chars)."""
        # Remove namespace prefix
        if "}" in tag:
            tag = tag.split("}")[-1]
        
        # Remove special characters
        tag = re.sub(r"[^\w]", "_", tag)
        
        # Lowercase
        return tag.lower()
    
    def extract_from_url(self, url: str, **kwargs) -> pd.DataFrame:
        """
        Extract XML data from a URL.
        
        Args:
            url: URL to fetch XML from
            **kwargs: Additional arguments for extract()
        
        Returns:
            Extracted DataFrame
        """
        import requests
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            return self.extract(response.content, **kwargs)
        except Exception as e:
            self.logger.error(f"Error fetching XML from URL {url}: {e}")
            return pd.DataFrame()




