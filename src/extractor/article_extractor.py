"""
Article extractor for extracting main content from article-like pages.
Used when table extraction yields no meaningful structured data.
"""

import re
from typing import Dict, Optional, Any
from datetime import datetime
from bs4 import BeautifulSoup, Tag
import requests

from ..utils.logger import get_logger

logger = get_logger()


class ArticleExtractor:
    """
    Extracts article content (title, text, metadata) from HTML pages.
    Uses heuristics to find main content, similar to readability algorithms.
    """
    
    def __init__(self):
        """Initialize the article extractor."""
        # Common selectors for article content
        self.article_selectors = [
            'article',
            '[role="article"]',
            '.article',
            '.post',
            '.entry-content',
            '.content',
            'main',
            '#main-content',
            '.main-content',
        ]
        
        # Selectors to remove (ads, navigation, etc.)
        self.remove_selectors = [
            'nav',
            'header',
            'footer',
            '.advertisement',
            '.ad',
            '.sidebar',
            '.comments',
            '.social-share',
            'script',
            'style',
        ]
    
    def extract(self, html: str, url: Optional[str] = None) -> Dict[str, Any]:
        """
        Extract article content from HTML.
        
        Args:
            html: HTML content as string
            url: Optional URL for metadata
        
        Returns:
            Dictionary with:
            - title: Article title
            - text: Main article text (cleaned)
            - author: Author name (if found)
            - published_at: Publication date (if found)
            - url: Source URL
            - extracted_at: Timestamp of extraction
        """
        soup = BeautifulSoup(html, 'lxml')
        
        # Extract title
        title = self._extract_title(soup)
        
        # Extract main content
        text = self._extract_text(soup)
        
        # Extract metadata
        author = self._extract_author(soup)
        published_at = self._extract_published_date(soup)
        
        return {
            'title': title,
            'text': text,
            'author': author,
            'published_at': published_at,
            'url': url or '',
            'extracted_at': datetime.now().isoformat(),
        }
    
    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract article title."""
        # Try h1 first
        h1 = soup.find('h1')
        if h1:
            title = h1.get_text(strip=True)
            if title and len(title) > 10:  # Reasonable title length
                return title
        
        # Try title tag
        title_tag = soup.find('title')
        if title_tag:
            title = title_tag.get_text(strip=True)
            # Remove site name if present (common pattern: "Title | Site Name")
            if '|' in title:
                title = title.split('|')[0].strip()
            if title and len(title) > 10:
                return title
        
        # Try meta og:title
        og_title = soup.find('meta', property='og:title')
        if og_title and og_title.get('content'):
            return og_title['content'].strip()
        
        return "Untitled Article"
    
    def _extract_text(self, soup: BeautifulSoup) -> str:
        """Extract main article text."""
        # Remove unwanted elements
        for selector in self.remove_selectors:
            for elem in soup.select(selector):
                elem.decompose()
        
        # Try to find article container
        article_content = None
        for selector in self.article_selectors:
            article_content = soup.select_one(selector)
            if article_content:
                break
        
        if not article_content:
            # Fallback: find the largest div with text
            divs = soup.find_all('div')
            if divs:
                # Score divs by text length (excluding scripts, styles)
                scored_divs = []
                for div in divs:
                    text = div.get_text(separator=' ', strip=True)
                    # Skip if too short or too long (likely not main content)
                    if 200 < len(text) < 50000:
                        scored_divs.append((len(text), div))
                
                if scored_divs:
                    scored_divs.sort(reverse=True)
                    article_content = scored_divs[0][1]
        
        if article_content:
            # Extract paragraphs
            paragraphs = []
            for p in article_content.find_all(['p', 'div']):
                text = p.get_text(separator=' ', strip=True)
                if text and len(text) > 20:  # Skip very short paragraphs
                    paragraphs.append(text)
            
            if paragraphs:
                return '\n\n'.join(paragraphs)
        
        # Last resort: extract all paragraph text
        paragraphs = []
        for p in soup.find_all('p'):
            text = p.get_text(strip=True)
            if text and len(text) > 20:
                paragraphs.append(text)
        
        return '\n\n'.join(paragraphs) if paragraphs else ""
    
    def _extract_author(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract author name."""
        # Try meta tags
        author_selectors = [
            ('meta', {'name': 'author'}),
            ('meta', {'property': 'article:author'}),
            ('meta', {'name': 'twitter:creator'}),
        ]
        
        for tag_name, attrs in author_selectors:
            meta = soup.find(tag_name, attrs)
            if meta and meta.get('content'):
                return meta['content'].strip()
        
        # Try common class names
        author_classes = ['.author', '.byline', '.article-author', '[rel="author"]']
        for selector in author_classes:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                if text:
                    return text
        
        return None
    
    def _extract_published_date(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract publication date."""
        # Try meta tags
        date_selectors = [
            ('meta', {'property': 'article:published_time'}),
            ('meta', {'name': 'pubdate'}),
            ('meta', {'property': 'og:published_time'}),
            ('time', {'datetime': True}),
        ]
        
        for tag_name, attrs in date_selectors:
            if tag_name == 'time':
                time_elem = soup.find('time', attrs)
                if time_elem and time_elem.get('datetime'):
                    return time_elem['datetime']
            else:
                meta = soup.find(tag_name, attrs)
                if meta and meta.get('content'):
                    return meta['content'].strip()
        
        return None
