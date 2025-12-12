"""
RSS client for fetching and parsing news feeds.
Lightweight, Railway-friendly (no browser automation).
"""

import feedparser
import requests
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from datetime import datetime
import time

from ..utils.logger import get_logger

logger = get_logger()


@dataclass
class Headline:
    """Represents a single news headline."""
    title: str
    link: str
    published_at: Optional[datetime]
    source_name: str
    description: Optional[str] = None
    
    def __lt__(self, other):
        """Sort by published_at (newest first), fallback to title."""
        if self.published_at and other.published_at:
            return self.published_at > other.published_at
        if self.published_at:
            return False
        if other.published_at:
            return True
        return self.title < other.title


class RSSClient:
    """
    Client for fetching and parsing RSS/Atom feeds.
    Handles timeouts, errors, and normalization.
    """
    
    def __init__(self, timeout: int = 10):
        """
        Initialize RSS client.
        
        Args:
            timeout: Request timeout in seconds
        """
        self.timeout = timeout
    
    def fetch_feed(self, rss_url: str, source_name: str) -> List[Headline]:
        """
        Fetch and parse an RSS feed.
        
        Args:
            rss_url: URL of the RSS feed
            source_name: Name of the news source (for attribution)
        
        Returns:
            List of Headline objects, sorted by published_at (newest first)
        """
        try:
            # Fetch feed with timeout
            response = requests.get(rss_url, timeout=self.timeout)
            response.raise_for_status()
            
            # Parse feed
            feed = feedparser.parse(response.content)
            
            if feed.bozo and feed.bozo_exception:
                logger.warning(f"Feed parsing warning for {source_name}: {feed.bozo_exception}")
            
            headlines = []
            for entry in feed.entries[:20]:  # Limit to 20 entries per feed
                # Extract published date
                published_at = None
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    try:
                        published_at = datetime(*entry.published_parsed[:6])
                    except (ValueError, TypeError):
                        pass
                elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                    try:
                        published_at = datetime(*entry.updated_parsed[:6])
                    except (ValueError, TypeError):
                        pass
                
                # Extract description
                description = None
                if hasattr(entry, 'summary'):
                    description = entry.summary
                elif hasattr(entry, 'description'):
                    description = entry.description
                
                headline = Headline(
                    title=entry.get('title', 'Untitled'),
                    link=entry.get('link', ''),
                    published_at=published_at,
                    source_name=source_name,
                    description=description,
                )
                headlines.append(headline)
            
            # Sort by published_at (newest first)
            headlines.sort()
            
            logger.info(f"Fetched {len(headlines)} headlines from {source_name}")
            return headlines
        
        except requests.exceptions.Timeout:
            logger.error(f"Timeout fetching feed from {source_name}: {rss_url}")
            return []
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching feed from {source_name}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error parsing feed from {source_name}: {e}")
            return []
    
    def fetch_multiple_feeds(
        self,
        feeds: List[Dict[str, str]],
        max_headlines: int = 5
    ) -> List[Headline]:
        """
        Fetch multiple feeds and return top N headlines (aggregated, sorted by date).
        
        Args:
            feeds: List of dicts with 'rss_url' and 'source_name' keys
            max_headlines: Maximum number of headlines to return
        
        Returns:
            List of Headline objects, sorted by published_at (newest first), limited to max_headlines
        """
        all_headlines = []
        
        for feed_info in feeds:
            rss_url = feed_info.get('rss_url')
            source_name = feed_info.get('source_name', 'Unknown')
            
            if not rss_url:
                continue
            
            headlines = self.fetch_feed(rss_url, source_name)
            all_headlines.extend(headlines)
        
        # Sort all headlines by published_at (newest first)
        all_headlines.sort()
        
        # Return top N
        return all_headlines[:max_headlines]
