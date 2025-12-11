"""
Dental ETF Scraper - Helper class for scraping dental-themed ETF and stock data.
Handles dynamic URL construction and multi-ETF/ticker scraping operations.
"""

import asyncio
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

import pandas as pd

from .universal_scraper import UniversalScraper
from .base_scraper import ScraperResult
from ..utils.logger import get_logger
from ..utils.config_manager import ConfigManager, SiteConfig


@dataclass
class DentalScrapeResult:
    """Result of a dental ETF scraping operation."""
    success: bool
    data: Optional[pd.DataFrame]
    rows: int
    error: Optional[str]
    source_id: str
    symbol: Optional[str] = None  # ETF or ticker symbol


class DentalETFScraper:
    """
    Scraper helper for dental ETF data sources.
    Handles dynamic URL construction and multi-symbol scraping.
    """
    
    def __init__(
        self,
        config_manager: Optional[ConfigManager] = None,
        use_stealth: bool = True,
        headless: bool = True,
    ):
        """
        Initialize the dental ETF scraper.
        
        Args:
            config_manager: ConfigManager instance for site configs
            use_stealth: Enable stealth mode for browser automation
            headless: Run browser in headless mode
        """
        self.config_manager = config_manager or ConfigManager()
        self.use_stealth = use_stealth
        self.headless = headless
        self.logger = get_logger()
    
    def scrape_yahoo_etf_holdings(
        self,
        etf_symbol: str,
    ) -> DentalScrapeResult:
        """
        Scrape ETF holdings from Yahoo Finance.
        
        Args:
            etf_symbol: ETF ticker symbol (e.g., "IHI", "XHE")
        
        Returns:
            DentalScrapeResult with holdings data
        """
        url = f"https://finance.yahoo.com/quote/{etf_symbol}/holdings/"
        self.logger.info(f"Scraping Yahoo Finance holdings for {etf_symbol}: {url}")
        
        try:
            scraper = UniversalScraper(
                use_stealth=self.use_stealth,
                headless=self.headless,
            )
            
            result = scraper.scrape_with_discovery(
                url=url,
                override_robots=False,
                save_raw=False,
            )
            
            if result.success and result.data is not None and not result.data.empty:
                # Add ETF symbol column
                df = result.data.copy()
                df["etf_symbol"] = etf_symbol
                
                return DentalScrapeResult(
                    success=True,
                    data=df,
                    rows=len(df),
                    error=None,
                    source_id="dental_yahoo_etf_holdings",
                    symbol=etf_symbol,
                )
            else:
                return DentalScrapeResult(
                    success=False,
                    data=None,
                    rows=0,
                    error=result.error or "No data extracted",
                    source_id="dental_yahoo_etf_holdings",
                    symbol=etf_symbol,
                )
        
        except Exception as e:
            self.logger.error(f"Error scraping Yahoo Finance: {e}")
            return DentalScrapeResult(
                success=False,
                data=None,
                rows=0,
                error=str(e),
                source_id="dental_yahoo_etf_holdings",
                symbol=etf_symbol,
            )
    
    def scrape_etfchannel_ownership(
        self,
        ticker: str,
    ) -> DentalScrapeResult:
        """
        Scrape ETF ownership data from ETFChannel for a specific stock.
        
        Args:
            ticker: Stock ticker symbol (e.g., "XRAY", "ALGN")
        
        Returns:
            DentalScrapeResult with ETF ownership data
        """
        url = f"https://www.etfchannel.com/symbol/{ticker.lower()}/"
        self.logger.info(f"Scraping ETFChannel ownership for {ticker}: {url}")
        
        try:
            scraper = UniversalScraper(
                use_stealth=self.use_stealth,
                headless=self.headless,
            )
            
            result = scraper.scrape_with_discovery(
                url=url,
                override_robots=False,
                save_raw=False,
            )
            
            if result.success and result.data is not None and not result.data.empty:
                # Add stock ticker column
                df = result.data.copy()
                df["stock_ticker"] = ticker
                
                return DentalScrapeResult(
                    success=True,
                    data=df,
                    rows=len(df),
                    error=None,
                    source_id="dental_etfchannel_ownership",
                    symbol=ticker,
                )
            else:
                return DentalScrapeResult(
                    success=False,
                    data=None,
                    rows=0,
                    error=result.error or "No data extracted",
                    source_id="dental_etfchannel_ownership",
                    symbol=ticker,
                )
        
        except Exception as e:
            self.logger.error(f"Error scraping ETFChannel: {e}")
            return DentalScrapeResult(
                success=False,
                data=None,
                rows=0,
                error=str(e),
                source_id="dental_etfchannel_ownership",
                symbol=ticker,
            )
    
    def scrape_multiple_etf_holdings(
        self,
        etf_symbols: List[str],
    ) -> DentalScrapeResult:
        """
        Scrape holdings for multiple ETFs and combine results.
        
        Args:
            etf_symbols: List of ETF ticker symbols
        
        Returns:
            DentalScrapeResult with combined holdings data
        """
        all_data = []
        errors = []
        
        for symbol in etf_symbols:
            result = self.scrape_yahoo_etf_holdings(symbol)
            if result.success and result.data is not None:
                all_data.append(result.data)
            else:
                errors.append(f"{symbol}: {result.error}")
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            return DentalScrapeResult(
                success=True,
                data=combined_df,
                rows=len(combined_df),
                error="; ".join(errors) if errors else None,
                source_id="dental_yahoo_etf_holdings",
                symbol=",".join(etf_symbols),
            )
        else:
            return DentalScrapeResult(
                success=False,
                data=None,
                rows=0,
                error="; ".join(errors) or "No data extracted from any ETF",
                source_id="dental_yahoo_etf_holdings",
                symbol=",".join(etf_symbols),
            )
    
    def scrape_multiple_ticker_etfs(
        self,
        tickers: List[str],
    ) -> DentalScrapeResult:
        """
        Scrape ETF ownership for multiple stock tickers and combine results.
        
        Args:
            tickers: List of stock ticker symbols
        
        Returns:
            DentalScrapeResult with combined ETF ownership data
        """
        all_data = []
        errors = []
        
        for ticker in tickers:
            result = self.scrape_etfchannel_ownership(ticker)
            if result.success and result.data is not None:
                all_data.append(result.data)
            else:
                errors.append(f"{ticker}: {result.error}")
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            return DentalScrapeResult(
                success=True,
                data=combined_df,
                rows=len(combined_df),
                error="; ".join(errors) if errors else None,
                source_id="dental_etfchannel_ownership",
                symbol=",".join(tickers),
            )
        else:
            return DentalScrapeResult(
                success=False,
                data=None,
                rows=0,
                error="; ".join(errors) or "No data extracted from any ticker",
                source_id="dental_etfchannel_ownership",
                symbol=",".join(tickers),
            )


def scrape_dental_source(
    source_id: str,
    etf_symbol: Optional[str] = None,
    ticker: Optional[str] = None,
    config_manager: Optional[ConfigManager] = None,
) -> Dict[str, Any]:
    """
    Convenience function to scrape a dental ETF data source.
    
    Args:
        source_id: Site ID from configuration
        etf_symbol: ETF symbol for Yahoo Finance (if applicable)
        ticker: Stock ticker for ETFChannel (if applicable)
        config_manager: ConfigManager instance
    
    Returns:
        Dictionary with result data
    """
    scraper = DentalETFScraper(config_manager=config_manager)
    
    if source_id == "dental_yahoo_etf_holdings" and etf_symbol:
        result = scraper.scrape_yahoo_etf_holdings(etf_symbol)
    elif source_id == "dental_etfchannel_ownership" and ticker:
        result = scraper.scrape_etfchannel_ownership(ticker)
    else:
        # Use standard scraping for other sources
        return None  # Let the caller handle standard scraping
    
    return {
        "success": result.success,
        "data": result.data,
        "rows": result.rows,
        "error": result.error,
        "warnings": [],
        "metadata": {
            "source_id": result.source_id,
            "symbol": result.symbol,
        },
    }
