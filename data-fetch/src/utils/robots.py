"""
Robots.txt compliance module for the data-fetch framework.
Handles fetching, parsing, and checking robots.txt permissions.
"""

import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple
from urllib.parse import urlparse, urljoin
import requests

from .logger import get_logger


class RobotsStatus(Enum):
    """Status of robots.txt permission check."""
    ALLOWED = "ALLOWED"
    DISALLOWED = "DISALLOWED"
    UNKNOWN = "UNKNOWN"


@dataclass
class RobotsDecision:
    """Result of a robots.txt permission check."""
    status: RobotsStatus
    reason: str
    robots_url: Optional[str] = None
    raw_robots: Optional[str] = None
    
    @property
    def is_allowed(self) -> bool:
        return self.status == RobotsStatus.ALLOWED
    
    @property
    def is_disallowed(self) -> bool:
        return self.status == RobotsStatus.DISALLOWED
    
    @property
    def is_unknown(self) -> bool:
        return self.status == RobotsStatus.UNKNOWN


class RobotsParser:
    """
    Parser for robots.txt files.
    Simplified implementation that handles common directives.
    """
    
    def __init__(self, robots_txt: str, user_agent: str = "*"):
        self.robots_txt = robots_txt
        self.user_agent = user_agent
        self.rules = self._parse()
    
    def _parse(self) -> dict:
        """Parse robots.txt content into rules."""
        rules = {
            "*": {"allow": [], "disallow": []},
        }
        
        current_agents = ["*"]
        
        for line in self.robots_txt.split("\n"):
            line = line.strip()
            
            # Skip comments and empty lines
            if not line or line.startswith("#"):
                continue
            
            # Parse directive
            if ":" not in line:
                continue
            
            directive, value = line.split(":", 1)
            directive = directive.strip().lower()
            value = value.strip()
            
            if directive == "user-agent":
                if value not in rules:
                    rules[value] = {"allow": [], "disallow": []}
                current_agents = [value]
                # Also track wildcard rules
                if value == "*":
                    current_agents = ["*"]
            elif directive == "disallow":
                for agent in current_agents:
                    if agent in rules:
                        rules[agent]["disallow"].append(value)
            elif directive == "allow":
                for agent in current_agents:
                    if agent in rules:
                        rules[agent]["allow"].append(value)
        
        return rules
    
    def is_allowed(self, path: str) -> bool:
        """
        Check if a path is allowed for the configured user agent.
        
        Args:
            path: URL path to check (e.g., "/data/charts")
        
        Returns:
            True if allowed, False if disallowed
        """
        # Get applicable rules
        agent_rules = self.rules.get(self.user_agent, self.rules.get("*", {}))
        
        # Check allow rules first (they take precedence)
        for allow_path in agent_rules.get("allow", []):
            if self._path_matches(path, allow_path):
                return True
        
        # Check disallow rules
        for disallow_path in agent_rules.get("disallow", []):
            if self._path_matches(path, disallow_path):
                return False
        
        # Default to allowed
        return True
    
    def _path_matches(self, path: str, pattern: str) -> bool:
        """Check if a path matches a robots.txt pattern."""
        if not pattern:
            return False
        
        # Handle wildcards
        if "*" in pattern:
            # Convert to regex
            regex_pattern = re.escape(pattern).replace(r"\*", ".*")
            if pattern.endswith("$"):
                regex_pattern = regex_pattern[:-2] + "$"
            return bool(re.match(regex_pattern, path))
        
        # Simple prefix matching
        return path.startswith(pattern)


def fetch_robots_txt(
    base_url: str,
    timeout: int = 10,
    user_agent: str = "DataFetchBot/1.0",
) -> Tuple[Optional[str], Optional[str]]:
    """
    Fetch robots.txt from a website.
    
    Args:
        base_url: Base URL of the website
        timeout: Request timeout in seconds
        user_agent: User agent string for the request
    
    Returns:
        Tuple of (robots_txt_content, error_message)
    """
    logger = get_logger()
    
    # Construct robots.txt URL
    parsed = urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    
    try:
        response = requests.get(
            robots_url,
            timeout=timeout,
            headers={"User-Agent": user_agent},
            allow_redirects=True,
        )
        
        if response.status_code == 200:
            return response.text, None
        elif response.status_code == 404:
            # No robots.txt means everything is allowed
            return "", None
        else:
            return None, f"HTTP {response.status_code}: {response.reason}"
    
    except requests.exceptions.Timeout:
        return None, "Request timed out"
    except requests.exceptions.ConnectionError as e:
        return None, f"Connection error: {str(e)}"
    except requests.exceptions.RequestException as e:
        return None, f"Request error: {str(e)}"


def check_robots_permission(
    url: str,
    user_agent: str = "DataFetchBot/1.0",
    timeout: int = 10,
) -> RobotsDecision:
    """
    Check if scraping a URL is allowed according to robots.txt.
    
    Args:
        url: The URL to check
        user_agent: User agent string
        timeout: Request timeout in seconds
    
    Returns:
        RobotsDecision with status and reason
    """
    logger = get_logger()
    parsed = urlparse(url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    robots_url = f"{base_url}/robots.txt"
    path = parsed.path or "/"
    
    # Skip robots.txt check for API endpoints (they have their own auth/rate limits)
    if "/api/" in path or path.startswith("/api") or "api." in parsed.netloc:
        logger.info(f"Skipping robots.txt check for API endpoint: {url}")
        return RobotsDecision(
            status=RobotsStatus.ALLOWED,
            reason="API endpoint - robots.txt check skipped (APIs have their own access controls)",
            robots_url=robots_url,
        )
    
    logger.info(f"Checking robots.txt for {base_url}")
    
    # Fetch robots.txt
    robots_txt, error = fetch_robots_txt(base_url, timeout, user_agent)
    
    if error:
        logger.warning(f"Could not fetch robots.txt: {error}")
        return RobotsDecision(
            status=RobotsStatus.UNKNOWN,
            reason=f"Could not fetch robots.txt: {error}",
            robots_url=robots_url,
        )
    
    if robots_txt == "":
        # No robots.txt found - everything is allowed
        logger.info("No robots.txt found - scraping allowed by default")
        return RobotsDecision(
            status=RobotsStatus.ALLOWED,
            reason="No robots.txt found - allowed by default",
            robots_url=robots_url,
        )
    
    # Parse and check
    try:
        parser = RobotsParser(robots_txt, user_agent)
        is_allowed = parser.is_allowed(path)
        
        if is_allowed:
            logger.info(f"Scraping allowed for {path}")
            return RobotsDecision(
                status=RobotsStatus.ALLOWED,
                reason=f"Path {path} is allowed in robots.txt",
                robots_url=robots_url,
                raw_robots=robots_txt,
            )
        else:
            logger.warning(f"Scraping disallowed for {path}")
            return RobotsDecision(
                status=RobotsStatus.DISALLOWED,
                reason=f"Path {path} is disallowed in robots.txt",
                robots_url=robots_url,
                raw_robots=robots_txt,
            )
    
    except Exception as e:
        logger.error(f"Error parsing robots.txt: {e}")
        return RobotsDecision(
            status=RobotsStatus.UNKNOWN,
            reason=f"Error parsing robots.txt: {str(e)}",
            robots_url=robots_url,
            raw_robots=robots_txt,
        )


def check_multiple_urls(
    urls: list[str],
    user_agent: str = "DataFetchBot/1.0",
) -> dict[str, RobotsDecision]:
    """
    Check robots.txt permissions for multiple URLs.
    Groups URLs by domain to minimize requests.
    
    Args:
        urls: List of URLs to check
        user_agent: User agent string
    
    Returns:
        Dictionary mapping URLs to their RobotsDecision
    """
    results = {}
    domain_cache = {}  # Cache robots.txt by domain
    
    for url in urls:
        parsed = urlparse(url)
        domain = f"{parsed.scheme}://{parsed.netloc}"
        
        # Use cached robots.txt if available
        if domain not in domain_cache:
            robots_txt, error = fetch_robots_txt(domain, user_agent=user_agent)
            domain_cache[domain] = (robots_txt, error)
        
        robots_txt, error = domain_cache[domain]
        
        if error:
            results[url] = RobotsDecision(
                status=RobotsStatus.UNKNOWN,
                reason=f"Could not fetch robots.txt: {error}",
                robots_url=f"{domain}/robots.txt",
            )
        elif robots_txt == "":
            results[url] = RobotsDecision(
                status=RobotsStatus.ALLOWED,
                reason="No robots.txt found - allowed by default",
                robots_url=f"{domain}/robots.txt",
            )
        else:
            try:
                parser = RobotsParser(robots_txt, user_agent)
                path = parsed.path or "/"
                is_allowed = parser.is_allowed(path)
                
                results[url] = RobotsDecision(
                    status=RobotsStatus.ALLOWED if is_allowed else RobotsStatus.DISALLOWED,
                    reason=f"Path {path} is {'allowed' if is_allowed else 'disallowed'} in robots.txt",
                    robots_url=f"{domain}/robots.txt",
                    raw_robots=robots_txt,
                )
            except Exception as e:
                results[url] = RobotsDecision(
                    status=RobotsStatus.UNKNOWN,
                    reason=f"Error parsing robots.txt: {str(e)}",
                    robots_url=f"{domain}/robots.txt",
                    raw_robots=robots_txt,
                )
    
    return results

