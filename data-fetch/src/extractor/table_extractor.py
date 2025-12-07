"""
HTML table extractor for the data-fetch framework.
Extracts data from HTML tables and converts to DataFrames.
"""

import re
from typing import List, Optional, Dict, Any
from dataclasses import dataclass

import pandas as pd
from bs4 import BeautifulSoup

from ..utils.logger import get_logger


@dataclass
class TableInfo:
    """Information about a detected HTML table."""
    index: int
    selector: str
    num_rows: int
    num_cols: int
    headers: List[str]
    sample_row: List[str]
    has_numeric_data: bool


class TableExtractor:
    """
    Extractor for HTML tables.
    Converts HTML tables to pandas DataFrames.
    """
    
    def __init__(self):
        self.logger = get_logger()
    
    def find_tables(self, html: str) -> List[TableInfo]:
        """
        Find all tables in HTML and return information about them.
        
        Args:
            html: HTML content
        
        Returns:
            List of TableInfo objects describing each table
        """
        soup = BeautifulSoup(html, "lxml")
        tables = soup.find_all("table")
        
        table_infos = []
        
        for i, table in enumerate(tables):
            # Get headers
            headers = []
            header_row = table.find("thead")
            if header_row:
                headers = [th.get_text(strip=True) for th in header_row.find_all(["th", "td"])]
            else:
                # Try first row
                first_row = table.find("tr")
                if first_row:
                    headers = [cell.get_text(strip=True) for cell in first_row.find_all(["th", "td"])]
            
            # Get row count - check tbody separately for accurate count
            tbody = table.find("tbody")
            if tbody:
                rows = tbody.find_all("tr")
                num_rows = len(rows)  # Data rows in tbody
            else:
                rows = table.find_all("tr")
                # Exclude header rows
                header_count = len(table.find_all("thead")) + len([r for r in rows if r.find("th")])
                num_rows = len(rows) - header_count
            num_cols = len(headers) if headers else 0
            
            # Get sample row
            sample_row = []
            if len(rows) > 1:
                sample_cells = rows[1].find_all(["th", "td"])
                sample_row = [cell.get_text(strip=True) for cell in sample_cells]
            
            # Check for numeric data
            has_numeric = any(
                self._looks_numeric(cell)
                for cell in sample_row
            )
            
            # Create selector
            table_id = table.get("id")
            table_class = table.get("class")
            if table_id:
                selector = f"#{table_id}"
            elif table_class:
                selector = f"table.{'.'.join(table_class)}"
            else:
                selector = f"table:nth-of-type({i + 1})"
            
            table_infos.append(TableInfo(
                index=i,
                selector=selector,
                num_rows=num_rows,
                num_cols=num_cols,
                headers=headers,
                sample_row=sample_row,
                has_numeric_data=has_numeric,
            ))
        
        self.logger.info(f"Found {len(table_infos)} tables in HTML")
        return table_infos
    
    def extract_table(
        self,
        html: str,
        selector: Optional[str] = None,
        table_index: int = 0,
        header_row: int = 0,
        skip_rows: int = 0,
        handle_merged_cells: bool = True,
        extract_nested_tables: bool = False,
    ) -> pd.DataFrame:
        """
        Extract a table from HTML as a DataFrame.
        
        Args:
            html: HTML content
            selector: CSS selector for the table (optional)
            table_index: Index of table to extract if no selector
            header_row: Row index to use as headers
            skip_rows: Number of rows to skip after headers
            handle_merged_cells: Whether to handle colspan/rowspan attributes
            extract_nested_tables: Whether to extract nested tables (returns list if True)
        
        Returns:
            Extracted DataFrame (or list of DataFrames if extract_nested_tables=True)
        """
        soup = BeautifulSoup(html, "lxml")
        
        # Find the table
        if selector:
            table = soup.select_one(selector)
        else:
            tables = soup.find_all("table")
            if table_index < len(tables):
                table = tables[table_index]
            else:
                raise ValueError(f"Table index {table_index} out of range")
        
        if not table:
            raise ValueError(f"Table not found with selector: {selector}")
        
        # Check for nested tables
        nested_tables = table.find_all("table", recursive=True)
        if len(nested_tables) > 1 and extract_nested_tables:
            # Extract nested tables separately
            nested_dfs = []
            for nested_table in nested_tables[1:]:  # Skip the parent table
                try:
                    nested_html = str(nested_table)
                    nested_df = self._extract_table_internal(nested_table, handle_merged_cells)
                    if not nested_df.empty:
                        nested_dfs.append(nested_df)
                except Exception as e:
                    self.logger.warning(f"Failed to extract nested table: {e}")
            return nested_dfs
        
        return self._extract_table_internal(table, handle_merged_cells, header_row, skip_rows)
    
    def _extract_table_internal(
        self,
        table,
        handle_merged_cells: bool = True,
        header_row: int = 0,
        skip_rows: int = 0,
    ) -> pd.DataFrame:
        """Internal method to extract table with merged cell handling."""
        # Check for thead and tbody separately (Yahoo Finance and many sites use this structure)
        thead = table.find("thead")
        tbody = table.find("tbody")
        
        # Extract header rows
        header_rows_list = []
        if thead:
            header_rows_list = thead.find_all("tr")
            self.logger.debug(f"Found {len(header_rows_list)} header rows in thead")
        else:
            # Look for header rows in the table directly
            all_rows = table.find_all("tr")
            header_rows_list = [r for r in all_rows if r.find("th")]
            if not header_rows_list:
                # Use first row as header if it has th elements
                if all_rows:
                    first_row = all_rows[0]
                    if first_row.find("th"):
                        header_rows_list = [first_row]
        
        # Extract data rows
        data_rows = []
        if tbody:
            data_rows = tbody.find_all("tr")
            self.logger.debug(f"Found {len(data_rows)} data rows in tbody")
        else:
            # Get all rows and exclude header rows
            all_rows = table.find_all("tr")
            header_indices = []
            for i, row in enumerate(all_rows):
                if row in header_rows_list or row.find("th"):
                    header_indices.append(i)
            data_rows = [row for i, row in enumerate(all_rows) if i not in header_indices]
        
        # If still no data rows, try getting all tr elements
        if not data_rows:
            all_rows = table.find_all("tr")
            # Filter out header rows
            data_rows = [r for r in all_rows if not r.find("th") and r not in header_rows_list]
        
        if not data_rows:
            self.logger.warning("No data rows found in table")
            return pd.DataFrame()
        
        # Extract headers
        if header_rows_list:
            header_row_elem = header_rows_list[0]
            if handle_merged_cells:
                headers = self._extract_headers_with_merged([header_row_elem], 0)
            else:
                header_cells = header_row_elem.find_all(["th", "td"])
                headers = [self._clean_text(cell.get_text()) for cell in header_cells]
        else:
            # No headers found, generate default headers
            if data_rows:
                first_row_cells = data_rows[0].find_all(["th", "td"])
                num_cols = len(first_row_cells)
                headers = [f"Column_{i+1}" for i in range(num_cols)]
            else:
                return pd.DataFrame()
        
        # Make headers unique
        headers = self._make_unique_headers(headers)
        
        # Extract data rows with merged cell handling
        data = []
        start_row = skip_rows
        
        for row in data_rows[start_row:]:
            if handle_merged_cells:
                row_data = self._extract_row_with_merged(row, len(headers))
            else:
                cells = row.find_all(["th", "td"])
                row_data = [self._clean_text(cell.get_text()) for cell in cells]
            
            # Skip completely empty rows
            if not any(cell and str(cell).strip() for cell in row_data):
                continue
            
            # Pad or trim to match header count
            if len(row_data) < len(headers):
                row_data.extend([""] * (len(headers) - len(row_data)))
            elif len(row_data) > len(headers):
                row_data = row_data[:len(headers)]
            
            data.append(row_data)
        
        # Create DataFrame
        df = pd.DataFrame(data, columns=headers)
        
        # Try to convert numeric columns
        for col in df.columns:
            df[col] = self._convert_to_numeric(df[col])
        
        self.logger.info(f"Extracted table with {len(df)} rows and {len(df.columns)} columns")
        return df
    
    def _detect_header_rows(self, rows) -> List[int]:
        """Detect multiple header rows (common in financial tables with merged headers)."""
        header_candidates = []
        
        for i, row in enumerate(rows[:5]):  # Check first 5 rows
            cells = row.find_all(["th", "td"])
            th_count = len(row.find_all("th"))
            
            # If row has mostly <th> tags, it's likely a header
            if th_count > len(cells) * 0.5:
                header_candidates.append(i)
            # If row has date/price-like headers, it's likely a header
            elif th_count > 0:
                cell_texts = " ".join([self._clean_text(c.get_text()) for c in cells]).lower()
                financial_keywords = ["date", "time", "price", "open", "high", "low", "close", "volume"]
                if any(kw in cell_texts for kw in financial_keywords):
                    header_candidates.append(i)
        
        return header_candidates if header_candidates else [0]
    
    def _extract_headers_with_merged(self, rows, header_row: int) -> List[str]:
        """Extract headers handling colspan attributes."""
        header_row_elem = rows[header_row]
        headers = []
        
        for cell in header_row_elem.find_all(["th", "td"]):
            text = self._clean_text(cell.get_text())
            colspan = int(cell.get("colspan", 1))
            
            # Add the header text, then repeat for colspan
            headers.append(text)
            for _ in range(colspan - 1):
                headers.append(f"{text}_continued")
        
        return headers
    
    def _extract_row_with_merged(self, row, expected_cols: int) -> List[str]:
        """Extract row data handling colspan and rowspan attributes."""
        row_data = []
        cells = row.find_all(["th", "td"])
        
        for cell in cells:
            text = self._clean_text(cell.get_text())
            colspan = int(cell.get("colspan", 1))
            rowspan = int(cell.get("rowspan", 1))
            
            # Add the cell value
            row_data.append(text)
            
            # Handle colspan: add empty strings for merged columns
            for _ in range(colspan - 1):
                row_data.append("")
        
        return row_data
    
    def extract_all_tables(self, html: str) -> List[pd.DataFrame]:
        """
        Extract all tables from HTML.
        
        Args:
            html: HTML content
        
        Returns:
            List of DataFrames, one per table
        """
        table_infos = self.find_tables(html)
        
        dfs = []
        for info in table_infos:
            try:
                df = self.extract_table(html, table_index=info.index)
                if not df.empty:
                    dfs.append(df)
            except Exception as e:
                self.logger.warning(f"Failed to extract table {info.index}: {e}")
        
        return dfs
    
    def extract_best_table(
        self,
        html: str,
        min_rows: int = 5,
        require_numeric: bool = True,
    ) -> Optional[pd.DataFrame]:
        """
        Extract the best table from HTML based on heuristics.
        
        Args:
            html: HTML content
            min_rows: Minimum number of rows required
            require_numeric: Whether table must have numeric data
        
        Returns:
            Best matching DataFrame or None
        """
        table_infos = self.find_tables(html)
        
        # Filter and score tables
        scored_tables = []
        for info in table_infos:
            score = 0
            
            # Skip tables that don't meet requirements
            if info.num_rows < min_rows:
                continue
            if require_numeric and not info.has_numeric_data:
                continue
            
            # Score by row count
            score += min(info.num_rows / 100, 1.0)
            
            # Score by column count (prefer more columns up to a point)
            score += min(info.num_cols / 10, 0.5)
            
            # Bonus for having date-like headers
            date_keywords = ["date", "time", "day", "month", "year"]
            if any(kw in " ".join(info.headers).lower() for kw in date_keywords):
                score += 0.5
            
            # Bonus for financial data indicators
            financial_keywords = ["price", "open", "high", "low", "close", "volume", "market", "cap"]
            if any(kw in " ".join(info.headers).lower() for kw in financial_keywords):
                score += 0.5
            
            # Bonus for OHLC structure (Open, High, Low, Close)
            header_text = " ".join(info.headers).lower()
            ohlc_count = sum(1 for kw in ["open", "high", "low", "close"] if kw in header_text)
            if ohlc_count >= 3:
                score += 1.0  # Strong indicator of financial data
            
            scored_tables.append((info, score))
        
        if not scored_tables:
            return None
        
        # Get best table
        scored_tables.sort(key=lambda x: x[1], reverse=True)
        best_info = scored_tables[0][0]
        
        return self.extract_table(html, table_index=best_info.index)
    
    def _clean_text(self, text: str) -> str:
        """Clean extracted text."""
        # Remove extra whitespace
        text = " ".join(text.split())
        # Remove non-printable characters
        text = "".join(c for c in text if c.isprintable())
        return text.strip()
    
    def _make_unique_headers(self, headers: List[str]) -> List[str]:
        """Make header names unique by adding suffixes."""
        seen = {}
        unique = []
        
        for h in headers:
            if not h:
                h = "column"
            
            if h in seen:
                seen[h] += 1
                unique.append(f"{h}_{seen[h]}")
            else:
                seen[h] = 0
                unique.append(h)
        
        return unique
    
    def _looks_numeric(self, text: str) -> bool:
        """Check if text looks like a number."""
        # Remove common number formatting
        cleaned = text.replace(",", "").replace("$", "").replace("%", "")
        cleaned = cleaned.replace("B", "e9").replace("M", "e6").replace("K", "e3")
        
        try:
            float(cleaned)
            return True
        except (ValueError, TypeError):
            return False
    
    def _convert_to_numeric(self, series: pd.Series) -> pd.Series:
        """Try to convert a series to numeric values."""
        def parse_value(val):
            if pd.isna(val) or val == "":
                return val
            
            val_str = str(val).strip()
            
            # Handle common suffixes
            multipliers = {
                "B": 1e9, "b": 1e9,
                "M": 1e6, "m": 1e6,
                "K": 1e3, "k": 1e3,
            }
            
            for suffix, mult in multipliers.items():
                if val_str.endswith(suffix):
                    try:
                        num = float(val_str[:-1].replace(",", "").replace("$", ""))
                        return num * mult
                    except ValueError:
                        pass
            
            # Try direct conversion
            try:
                cleaned = val_str.replace(",", "").replace("$", "").replace("%", "")
                return float(cleaned)
            except ValueError:
                return val
        
        return series.apply(parse_value)
    
    def detect_pagination(self, html: str) -> Dict[str, Any]:
        """
        Detect pagination controls in HTML.
        
        Args:
            html: HTML content
        
        Returns:
            Dictionary with pagination info (has_pagination, next_url, page_count, etc.)
        """
        soup = BeautifulSoup(html, "lxml")
        
        pagination_info = {
            "has_pagination": False,
            "next_url": None,
            "prev_url": None,
            "page_count": None,
            "current_page": None,
            "pagination_type": None,  # "buttons", "infinite_scroll", "none"
        }
        
        # Look for pagination controls
        pagination_patterns = [
            # Next/Previous buttons
            soup.find_all("a", string=re.compile(r"(next|more|load more)", re.I)),
            soup.find_all("button", string=re.compile(r"(next|more|load more)", re.I)),
            soup.find_all("a", class_=re.compile(r"next|pagination", re.I)),
            soup.find_all("button", class_=re.compile(r"next|pagination", re.I)),
            # Page numbers
            soup.find_all("a", href=re.compile(r"[?&]page=\d+")),
            soup.find_all(class_=re.compile(r"pagination|pager", re.I)),
        ]
        
        for pattern_results in pagination_patterns:
            if pattern_results:
                pagination_info["has_pagination"] = True
                pagination_info["pagination_type"] = "buttons"
                
                # Try to find next link
                for elem in pattern_results:
                    if elem.name == "a" and elem.get("href"):
                        href = elem.get("href")
                        text = elem.get_text().lower()
                        if "next" in text or "more" in text:
                            pagination_info["next_url"] = href
                        elif "prev" in text or "previous" in text:
                            pagination_info["prev_url"] = href
                break
        
        # Check for infinite scroll indicators
        scroll_indicators = soup.find_all(class_=re.compile(r"infinite|scroll|load-more", re.I))
        if scroll_indicators:
            pagination_info["has_pagination"] = True
            pagination_info["pagination_type"] = "infinite_scroll"
        
        # Try to detect page count from page numbers
        page_links = soup.find_all("a", href=re.compile(r"[?&]page=\d+"))
        if page_links:
            page_numbers = []
            for link in page_links:
                match = re.search(r"page=(\d+)", link.get("href", ""))
                if match:
                    page_numbers.append(int(match.group(1)))
            if page_numbers:
                pagination_info["page_count"] = max(page_numbers)
                pagination_info["current_page"] = min(page_numbers)
        
        return pagination_info
    
    def is_financial_table(self, table_info: TableInfo) -> bool:
        """
        Check if a table appears to contain financial data.
        
        Args:
            table_info: TableInfo object
        
        Returns:
            True if table appears to be financial data
        """
        header_text = " ".join(table_info.headers).lower()
        
        # Financial keywords
        financial_keywords = [
            "price", "open", "high", "low", "close", "volume", "market", "cap",
            "ticker", "symbol", "stock", "share", "dividend", "yield", "return",
            "ohlc", "candle", "quote", "bid", "ask", "spread",
        ]
        
        keyword_matches = sum(1 for kw in financial_keywords if kw in header_text)
        
        # OHLC structure detection
        ohlc_count = sum(1 for kw in ["open", "high", "low", "close"] if kw in header_text)
        
        # Date column presence
        has_date = any(kw in header_text for kw in ["date", "time", "timestamp"])
        
        # Numeric data requirement
        has_numeric = table_info.has_numeric_data
        
        # Scoring
        score = 0
        if keyword_matches >= 2:
            score += 2
        if ohlc_count >= 3:
            score += 3  # Strong OHLC indicator
        if has_date:
            score += 1
        if has_numeric:
            score += 1
        
        return score >= 3

