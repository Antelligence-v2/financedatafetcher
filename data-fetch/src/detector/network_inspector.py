"""
Network request inspector for detecting data endpoints.
Analyzes captured network requests to find API endpoints containing data.
"""

import json
import re
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse, parse_qs

from ..utils.logger import get_logger
from ..utils.browser import NetworkRequest


@dataclass
class CandidateEndpoint:
    """A candidate data endpoint detected from network requests."""
    url: str
    method: str
    content_type: str
    confidence_score: float
    data_preview: Optional[str] = None
    detected_structure: Optional[str] = None  # "timeseries", "table", "object", etc.
    field_names: List[str] = field(default_factory=list)
    row_count_estimate: Optional[int] = None
    response_body: Optional[bytes] = None
    
    def __repr__(self):
        return f"CandidateEndpoint(url={self.url[:60]}..., score={self.confidence_score:.2f})"


class NetworkInspector:
    """
    Inspector for analyzing network requests and detecting data endpoints.
    Uses pattern matching and heuristics to identify potential data sources.
    """
    
    # Keywords that suggest data endpoints
    DATA_KEYWORDS = [
        "api", "chart", "data", "series", "history", "export",
        "json", "csv", "stats", "metrics", "prices", "volume",
        "market", "ticker", "quote", "ohlc", "candle",
    ]
    
    # Keywords that suggest tracking/analytics (to filter out)
    TRACKING_KEYWORDS = [
        "analytics", "tracking", "pixel", "beacon", "collect",
        "facebook", "google-analytics", "clarity", "hotjar",
        "segment", "amplitude", "mixpanel", "gtm", "recaptcha",
        "cookie", "consent", "ads", "advertising",
    ]
    
    def __init__(self):
        self.logger = get_logger()
    
    def analyze_requests(
        self,
        requests: List[NetworkRequest],
        target_url: str = None,
    ) -> List[CandidateEndpoint]:
        """
        Analyze network requests and return candidate data endpoints.
        
        Args:
            requests: List of captured network requests
            target_url: The page URL being scraped (for context)
        
        Returns:
            List of candidate endpoints sorted by confidence score
        """
        candidates = []
        target_domain = urlparse(target_url).netloc if target_url else None
        
        for request in requests:
            # Skip non-successful responses
            if request.status != 200:
                continue
            
            # Skip non-data content types
            if not request.is_json and not request.is_csv:
                continue
            
            # Calculate confidence score
            score = self._calculate_score(request, target_domain)
            
            # Skip low-confidence results
            if score < 0.2:
                continue
            
            # Create candidate
            candidate = CandidateEndpoint(
                url=request.url,
                method=request.method,
                content_type=request.content_type or "",
                confidence_score=score,
                response_body=request.response_body,
            )
            
            # Analyze response body if available
            if request.response_body:
                self._analyze_response(candidate, request.response_body)
            
            candidates.append(candidate)
        
        # Sort by confidence score
        candidates.sort(key=lambda x: x.confidence_score, reverse=True)
        
        self.logger.info(f"Found {len(candidates)} candidate endpoints")
        return candidates
    
    def _calculate_score(
        self,
        request: NetworkRequest,
        target_domain: Optional[str],
    ) -> float:
        """
        Calculate confidence score for a network request.
        
        Scoring algorithm:
        - +2.0 for each data keyword in URL
        - +3.0 if response contains timeseries data
        - +1.0 for JSON content type
        - +0.5 for same domain as target
        - -2.0 for each tracking keyword
        - Normalize to 0-1 range
        """
        score = 0.0
        url_lower = request.url.lower()
        
        # Check for data keywords
        for keyword in self.DATA_KEYWORDS:
            if keyword in url_lower:
                score += 2.0
        
        # Check for tracking keywords (negative)
        for keyword in self.TRACKING_KEYWORDS:
            if keyword in url_lower:
                score -= 2.0
        
        # Content type bonus
        if request.is_json:
            score += 1.0
        elif request.is_csv:
            score += 1.5  # CSV is often cleaner data
        
        # Same domain bonus
        if target_domain:
            request_domain = urlparse(request.url).netloc
            if request_domain == target_domain:
                score += 0.5
        
        # Response size indicator (larger responses often contain more data)
        if request.content_length:
            if request.content_length > 1000:
                score += 0.5
            if request.content_length > 10000:
                score += 0.5
        
        # Analyze response body for timeseries patterns
        if request.response_body:
            try:
                body_str = request.response_body.decode("utf-8", errors="ignore")
                if self._looks_like_timeseries(body_str):
                    score += 3.0
            except Exception:
                pass
        
        # Normalize to 0-1 range (cap at 10 points max)
        return min(max(score / 10.0, 0.0), 1.0)
    
    def _looks_like_timeseries(self, body: str) -> bool:
        """Check if response body looks like timeseries data."""
        # Look for date patterns
        date_patterns = [
            r"\d{4}-\d{2}-\d{2}",  # YYYY-MM-DD
            r"\d{10,13}",  # Unix timestamp
            r'"date"', r'"time"', r'"timestamp"',
        ]
        
        date_matches = sum(1 for p in date_patterns if re.search(p, body))
        
        # Look for array of objects pattern
        array_pattern = r'\[\s*\{[^}]+\}\s*,\s*\{[^}]+\}'
        has_object_array = bool(re.search(array_pattern, body))
        
        return date_matches >= 1 and has_object_array
    
    def _analyze_response(
        self,
        candidate: CandidateEndpoint,
        body: bytes,
    ):
        """Analyze response body and update candidate metadata."""
        try:
            body_str = body.decode("utf-8", errors="ignore")
            
            # Try to parse as JSON
            try:
                data = json.loads(body_str)
                self._analyze_json(candidate, data)
            except json.JSONDecodeError:
                pass
            
            # Create preview
            preview_length = 500
            candidate.data_preview = body_str[:preview_length]
            if len(body_str) > preview_length:
                candidate.data_preview += "..."
            
        except Exception as e:
            self.logger.debug(f"Error analyzing response: {e}")
    
    def _analyze_json(self, candidate: CandidateEndpoint, data: Any):
        """Analyze JSON data structure."""
        # Handle different JSON structures
        if isinstance(data, list):
            candidate.detected_structure = "array"
            candidate.row_count_estimate = len(data)
            if data and isinstance(data[0], dict):
                candidate.field_names = list(data[0].keys())
                candidate.detected_structure = "timeseries"
        
        elif isinstance(data, dict):
            # Look for nested arrays
            for key, value in data.items():
                if isinstance(value, list) and len(value) > 0:
                    if isinstance(value[0], dict):
                        candidate.detected_structure = "nested_timeseries"
                        candidate.row_count_estimate = len(value)
                        candidate.field_names = list(value[0].keys())
                        break
                    elif isinstance(value[0], (int, float)):
                        candidate.detected_structure = "value_arrays"
                        candidate.row_count_estimate = len(value)
                        candidate.field_names.append(key)
            
            if not candidate.detected_structure:
                candidate.detected_structure = "object"
                candidate.field_names = list(data.keys())
    
    def get_best_endpoint(
        self,
        requests: List[NetworkRequest],
        target_url: str = None,
        min_confidence: float = 0.3,
    ) -> Optional[CandidateEndpoint]:
        """
        Get the best candidate endpoint from a list of requests.
        
        Args:
            requests: List of captured network requests
            target_url: The page URL being scraped
            min_confidence: Minimum confidence threshold
        
        Returns:
            Best candidate or None if no suitable endpoint found
        """
        candidates = self.analyze_requests(requests, target_url)
        
        for candidate in candidates:
            if candidate.confidence_score >= min_confidence:
                return candidate
        
        return None
    
    def filter_by_pattern(
        self,
        candidates: List[CandidateEndpoint],
        url_pattern: str,
    ) -> List[CandidateEndpoint]:
        """
        Filter candidates by URL pattern.
        
        Args:
            candidates: List of candidate endpoints
            url_pattern: Regex pattern to match URLs
        
        Returns:
            Filtered list of candidates
        """
        pattern = re.compile(url_pattern, re.IGNORECASE)
        return [c for c in candidates if pattern.search(c.url)]

