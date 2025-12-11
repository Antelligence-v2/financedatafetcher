"""
Rate limiter for respectful scraping.
Implements per-domain rate limiting with token bucket algorithm.
"""

import time
from typing import Dict, Optional
from dataclasses import dataclass, field
from collections import defaultdict

from .logger import get_logger


@dataclass
class RateLimit:
    """Rate limit configuration for a domain."""
    requests_per_second: float = 1.0
    requests_per_minute: Optional[float] = None
    requests_per_hour: Optional[float] = None
    retry_after: Optional[float] = None  # Seconds to wait if rate limited


@dataclass
class TokenBucket:
    """Token bucket for rate limiting."""
    capacity: float
    tokens: float
    refill_rate: float  # tokens per second
    last_refill: float
    
    def consume(self, tokens: float = 1.0) -> bool:
        """
        Try to consume tokens from the bucket.
        
        Args:
            tokens: Number of tokens to consume
        
        Returns:
            True if tokens were consumed, False if insufficient
        """
        self._refill()
        
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False
    
    def _refill(self):
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill
        
        # Add tokens based on refill rate
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now
    
    def wait_time(self, tokens: float = 1.0) -> float:
        """
        Calculate time to wait before tokens are available.
        
        Args:
            tokens: Number of tokens needed
        
        Returns:
            Seconds to wait
        """
        self._refill()
        
        if self.tokens >= tokens:
            return 0.0
        
        needed = tokens - self.tokens
        return needed / self.refill_rate


class RateLimiter:
    """
    Rate limiter with per-domain limits.
    Uses token bucket algorithm for smooth rate limiting.
    """
    
    def __init__(self):
        self.logger = get_logger()
        self._buckets: Dict[str, TokenBucket] = {}
        self._rate_limits: Dict[str, RateLimit] = {}
        self._last_request: Dict[str, float] = defaultdict(float)
        self._retry_after: Dict[str, float] = {}
    
    def set_rate_limit(self, domain: str, rate_limit: RateLimit):
        """
        Set rate limit for a domain.
        
        Args:
            domain: Domain name
            rate_limit: Rate limit configuration
        """
        self._rate_limits[domain] = rate_limit
        
        # Create token bucket
        # Use the most restrictive limit
        if rate_limit.requests_per_second:
            rps = rate_limit.requests_per_second
        elif rate_limit.requests_per_minute:
            rps = rate_limit.requests_per_minute / 60.0
        elif rate_limit.requests_per_hour:
            rps = rate_limit.requests_per_hour / 3600.0
        else:
            rps = 1.0  # Default
        
        self._buckets[domain] = TokenBucket(
            capacity=rps * 10,  # Allow burst of 10x rate
            tokens=rps * 10,
            refill_rate=rps,
            last_refill=time.time(),
        )
        
        self.logger.info(f"Set rate limit for {domain}: {rps:.2f} requests/second")
    
    def wait_if_needed(self, domain: str) -> float:
        """
        Wait if necessary to respect rate limit.
        
        Args:
            domain: Domain name
        
        Returns:
            Time waited in seconds
        """
        # Check retry-after header
        if domain in self._retry_after:
            retry_after = self._retry_after[domain]
            if retry_after > time.time():
                wait_time = retry_after - time.time()
                self.logger.warning(f"Rate limited for {domain}, waiting {wait_time:.1f}s")
                time.sleep(wait_time)
                return wait_time
            else:
                del self._retry_after[domain]
        
        # Check token bucket
        if domain in self._buckets:
            bucket = self._buckets[domain]
            
            if not bucket.consume():
                wait_time = bucket.wait_time()
                if wait_time > 0:
                    self.logger.debug(f"Rate limiting {domain}, waiting {wait_time:.2f}s")
                    time.sleep(wait_time)
                    bucket.consume()  # Consume after waiting
                    return wait_time
        
        # Update last request time
        self._last_request[domain] = time.time()
        
        return 0.0
    
    def record_rate_limit(self, domain: str, retry_after: Optional[float] = None):
        """
        Record that a rate limit was hit.
        
        Args:
            domain: Domain name
            retry_after: Seconds to wait (from Retry-After header)
        """
        if retry_after:
            self._retry_after[domain] = time.time() + retry_after
            self.logger.warning(f"Rate limited by {domain}, retry after {retry_after}s")
        else:
            # Default retry after 60 seconds
            self._retry_after[domain] = time.time() + 60
            self.logger.warning(f"Rate limited by {domain}, defaulting to 60s wait")
    
    def get_wait_time(self, domain: str) -> float:
        """
        Get time to wait before next request (without waiting).
        
        Args:
            domain: Domain name
        
        Returns:
            Seconds to wait
        """
        # Check retry-after
        if domain in self._retry_after:
            retry_after = self._retry_after[domain]
            if retry_after > time.time():
                return retry_after - time.time()
        
        # Check token bucket
        if domain in self._buckets:
            return self._buckets[domain].wait_time()
        
        return 0.0




