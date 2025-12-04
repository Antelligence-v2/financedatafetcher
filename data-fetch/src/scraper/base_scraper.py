"""
Base scraper class for the data-fetch framework.
Provides lifecycle methods and common error handling.
"""

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
import pandas as pd

from ..utils.logger import get_logger
from ..utils.robots import check_robots_permission, RobotsDecision, RobotsStatus
from ..utils.io_utils import save_raw_response, generate_run_id
from ..utils.config_manager import SiteConfig


@dataclass
class ScraperResult:
    """Result of a scraping operation."""
    success: bool
    data: Optional[pd.DataFrame] = None
    source: str = ""
    url: str = ""
    run_id: str = ""
    rows_extracted: int = 0
    date_range: tuple = (None, None)
    raw_response_path: Optional[str] = None
    robots_decision: Optional[RobotsDecision] = None
    validation_warnings: List[str] = field(default_factory=list)
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "success": self.success,
            "source": self.source,
            "url": self.url,
            "run_id": self.run_id,
            "rows_extracted": self.rows_extracted,
            "date_range": self.date_range,
            "raw_response_path": str(self.raw_response_path) if self.raw_response_path else None,
            "robots_status": self.robots_decision.status.value if self.robots_decision else None,
            "validation_warnings": self.validation_warnings,
            "error": self.error,
            "metadata": self.metadata,
        }


class BaseScraper(ABC):
    """
    Abstract base class for all scrapers.
    Provides lifecycle methods and common functionality.
    """
    
    def __init__(
        self,
        config: Optional[SiteConfig] = None,
        user_agent: str = "DataFetchBot/1.0",
        timeout: int = 30,
        max_retries: int = 3,
        retry_delay: float = 2.0,
    ):
        """
        Initialize the scraper.
        
        Args:
            config: Site configuration (optional for universal scraper)
            user_agent: User agent string for requests
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts
            retry_delay: Base delay between retries (exponential backoff)
        """
        self.config = config
        self.user_agent = user_agent
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.logger = get_logger()
        
        self._run_id: Optional[str] = None
        self._robots_decision: Optional[RobotsDecision] = None
    
    @property
    def site_id(self) -> str:
        """Get the site ID."""
        return self.config.id if self.config else "unknown"
    
    @property
    def run_id(self) -> str:
        """Get or generate a run ID."""
        if not self._run_id:
            self._run_id = generate_run_id(self.site_id)
        return self._run_id
    
    def check_compliance(self, url: str, override: bool = False) -> RobotsDecision:
        """
        Check robots.txt compliance for a URL.
        
        Args:
            url: URL to check
            override: If True, allow scraping even if UNKNOWN
        
        Returns:
            RobotsDecision with status and reason
        
        Raises:
            PermissionError: If scraping is DISALLOWED
        """
        self.logger.info(f"Checking robots.txt compliance for {url}")
        
        decision = check_robots_permission(url, self.user_agent, self.timeout)
        self._robots_decision = decision
        
        if decision.is_disallowed:
            self.logger.error(f"Scraping DISALLOWED: {decision.reason}")
            raise PermissionError(f"Robots.txt disallows scraping: {decision.reason}")
        
        if decision.is_unknown and not override:
            self.logger.warning(f"Robots.txt status UNKNOWN: {decision.reason}")
            self.logger.warning("Use --override-robots flag to proceed at your own risk")
        
        return decision
    
    @abstractmethod
    def fetch_raw(self, url: str) -> Dict[str, Any]:
        """
        Fetch raw data from the URL.
        Must be implemented by subclasses.
        
        Args:
            url: URL to fetch
        
        Returns:
            Dictionary containing raw data and metadata
        """
        pass
    
    @abstractmethod
    def parse_raw(self, raw_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Parse raw data into a DataFrame.
        Must be implemented by subclasses.
        
        Args:
            raw_data: Raw data from fetch_raw
        
        Returns:
            Parsed DataFrame
        """
        pass
    
    def validate(self, df: pd.DataFrame) -> List[str]:
        """
        Validate the extracted data.
        Override in subclasses for custom validation.
        
        Args:
            df: DataFrame to validate
        
        Returns:
            List of validation warnings (empty if valid)
        """
        warnings = []
        
        if df is None or df.empty:
            warnings.append("DataFrame is empty or None")
            return warnings
        
        # Check for date column
        date_cols = [col for col in df.columns if "date" in col.lower() or "time" in col.lower()]
        if not date_cols:
            warnings.append("No date/time column found")
        
        # Check for duplicate rows (only if DataFrame is hashable)
        try:
            if df.duplicated().any():
                dup_count = df.duplicated().sum()
                warnings.append(f"Found {dup_count} duplicate rows")
        except TypeError:
            # DataFrame contains unhashable types (dicts, lists), skip duplicate check
            warnings.append("Cannot check for duplicates (contains unhashable types)")
        
        # Check for NaN values
        nan_counts = df.isna().sum()
        cols_with_nan = nan_counts[nan_counts > 0]
        if not cols_with_nan.empty:
            for col, count in cols_with_nan.items():
                warnings.append(f"Column '{col}' has {count} NaN values")
        
        return warnings
    
    def _retry_operation(self, operation, *args, **kwargs):
        """
        Execute an operation with retry logic.
        
        Args:
            operation: Callable to execute
            *args, **kwargs: Arguments for the operation
        
        Returns:
            Result of the operation
        
        Raises:
            Last exception if all retries fail
        """
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)  # Exponential backoff
                    self.logger.warning(
                        f"Attempt {attempt + 1}/{self.max_retries} failed: {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    time.sleep(delay)
                else:
                    self.logger.error(f"All {self.max_retries} attempts failed")
        
        raise last_error
    
    def scrape(
        self,
        url: Optional[str] = None,
        override_robots: bool = False,
        save_raw: bool = True,
    ) -> ScraperResult:
        """
        Execute the full scraping workflow.
        
        Args:
            url: URL to scrape (uses config.page_url if not provided)
            override_robots: If True, proceed even if robots.txt is UNKNOWN
            save_raw: If True, save raw response to disk
        
        Returns:
            ScraperResult with extracted data and metadata
        """
        # Get URL
        if url is None:
            if self.config:
                url = self.config.page_url
            else:
                raise ValueError("URL must be provided if no config is set")
        
        self._run_id = generate_run_id(self.site_id)
        self.logger.info(f"Starting scrape: {url} (run_id: {self.run_id})")
        
        result = ScraperResult(
            success=False,
            source=self.site_id,
            url=url,
            run_id=self.run_id,
        )
        
        try:
            # Step 1: Check robots.txt compliance
            try:
                robots_decision = self.check_compliance(url, override_robots)
                result.robots_decision = robots_decision
            except PermissionError as e:
                result.error = str(e)
                return result
            
            # Step 2: Fetch raw data
            self.logger.info("Fetching raw data...")
            raw_data = self._retry_operation(self.fetch_raw, url)
            
            # Save raw response
            if save_raw and raw_data:
                raw_path = save_raw_response(
                    raw_data.get("content", raw_data),
                    "response",
                    self.site_id,
                    self.run_id,
                )
                result.raw_response_path = str(raw_path)
                self.logger.info(f"Saved raw response to {raw_path}")
            
            # Step 3: Parse data
            self.logger.info("Parsing raw data...")
            df = self.parse_raw(raw_data)
            
            if df is None or df.empty:
                result.error = "No data extracted"
                return result
            
            # Step 4: Validate
            self.logger.info("Validating extracted data...")
            warnings = self.validate(df)
            result.validation_warnings = warnings
            
            if warnings:
                self.logger.warning(f"Validation warnings: {warnings}")
            
            # Step 5: Set result
            result.success = True
            result.data = df
            result.rows_extracted = len(df)
            
            # Try to get date range
            date_cols = [col for col in df.columns if "date" in col.lower()]
            if date_cols:
                date_col = date_cols[0]
                try:
                    result.date_range = (
                        df[date_col].min(),
                        df[date_col].max(),
                    )
                except Exception:
                    pass
            
            self.logger.info(
                f"Scrape successful: {result.rows_extracted} rows extracted"
            )
            
        except Exception as e:
            self.logger.error(f"Scrape failed: {e}")
            result.error = str(e)
        
        return result

