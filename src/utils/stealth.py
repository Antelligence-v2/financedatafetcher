"""
Stealth utilities for bypassing anti-bot detection.
Implements fingerprint randomization and browser automation stealth techniques.
"""

import random
import string
from typing import Dict, Optional, List
from dataclasses import dataclass

from .logger import get_logger


@dataclass
class BrowserFingerprint:
    """Browser fingerprint configuration."""
    user_agent: str
    viewport_width: int
    viewport_height: int
    platform: str
    languages: List[str]
    timezone: str
    webgl_vendor: str
    webgl_renderer: str


class StealthManager:
    """
    Manager for stealth techniques to bypass anti-bot detection.
    Implements fingerprint randomization and common evasion patterns.
    """
    
    # Common user agents (rotated to avoid patterns)
    USER_AGENTS = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]
    
    # WebGL vendor/renderer combinations
    WEBGL_VENDORS = [
        "Intel Inc.",
        "Google Inc. (NVIDIA)",
        "Google Inc. (Intel)",
        "Apple Inc.",
    ]
    
    WEBGL_RENDERERS = [
        "Intel Iris OpenGL Engine",
        "ANGLE (NVIDIA, NVIDIA GeForce GTX 1060 Direct3D11 vs_5_0 ps_5_0, D3D11)",
        "ANGLE (Intel, Intel(R) UHD Graphics 630 Direct3D11 vs_5_0 ps_5_0, D3D11)",
        "Apple GPU",
    ]
    
    # Timezones
    TIMEZONES = [
        "America/New_York",
        "America/Los_Angeles",
        "America/Chicago",
        "Europe/London",
        "Europe/Paris",
        "Asia/Tokyo",
        "Asia/Shanghai",
    ]
    
    # Languages
    LANGUAGE_COMBOS = [
        ["en-US", "en"],
        ["en-GB", "en"],
        ["en-US", "en", "fr"],
        ["en-US", "en", "es"],
    ]
    
    def __init__(self, randomize: bool = True):
        """
        Initialize the stealth manager.
        
        Args:
            randomize: If True, randomize fingerprint on each use
        """
        self.randomize = randomize
        self.logger = get_logger()
        self._fingerprint: Optional[BrowserFingerprint] = None
    
    def get_fingerprint(self) -> BrowserFingerprint:
        """
        Get or generate a browser fingerprint.
        
        Returns:
            BrowserFingerprint configuration
        """
        if self._fingerprint is None or self.randomize:
            self._fingerprint = self._generate_fingerprint()
        return self._fingerprint
    
    def _generate_fingerprint(self) -> BrowserFingerprint:
        """Generate a randomized browser fingerprint."""
        user_agent = random.choice(self.USER_AGENTS)
        
        # Extract platform from user agent
        if "Macintosh" in user_agent:
            platform = "MacIntel"
            viewport_width = random.choice([1920, 1440, 2560])
            viewport_height = random.choice([1080, 900, 1440])
        elif "Windows" in user_agent:
            platform = "Win32"
            viewport_width = random.choice([1920, 1366, 2560])
            viewport_height = random.choice([1080, 768, 1440])
        else:
            platform = "Linux x86_64"
            viewport_width = random.choice([1920, 1366])
            viewport_height = random.choice([1080, 768])
        
        # Randomize WebGL
        webgl_vendor = random.choice(self.WEBGL_VENDORS)
        webgl_renderer = random.choice(self.WEBGL_RENDERERS)
        
        # Randomize timezone and languages
        timezone = random.choice(self.TIMEZONES)
        languages = random.choice(self.LANGUAGE_COMBOS)
        
        return BrowserFingerprint(
            user_agent=user_agent,
            viewport_width=viewport_width,
            viewport_height=viewport_height,
            platform=platform,
            languages=languages,
            timezone=timezone,
            webgl_vendor=webgl_vendor,
            webgl_renderer=webgl_renderer,
        )
    
    def get_stealth_headers(self) -> Dict[str, str]:
        """
        Get headers that help bypass detection.
        
        Returns:
            Dictionary of stealth headers
        """
        fingerprint = self.get_fingerprint()
        
        headers = {
            "User-Agent": fingerprint.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": ", ".join(fingerprint.languages) + ";q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }
        
        return headers
    
    def get_playwright_context_options(self) -> Dict:
        """
        Get Playwright context options for stealth mode.
        
        Returns:
            Dictionary of Playwright context options
        """
        fingerprint = self.get_fingerprint()
        
        return {
            "user_agent": fingerprint.user_agent,
            "viewport": {
                "width": fingerprint.viewport_width,
                "height": fingerprint.viewport_height,
            },
            "locale": fingerprint.languages[0],
            "timezone_id": fingerprint.timezone,
            "permissions": ["geolocation"],
            "geolocation": {
                "latitude": random.uniform(25.0, 50.0),
                "longitude": random.uniform(-125.0, -70.0),
            },
            "extra_http_headers": self.get_stealth_headers(),
        }
    
    def get_random_delay(self, base_ms: int = 1000, jitter_ms: int = 500) -> float:
        """
        Get a randomized delay to avoid detection patterns.
        
        Args:
            base_ms: Base delay in milliseconds
            jitter_ms: Random jitter range in milliseconds
        
        Returns:
            Delay in seconds
        """
        delay_ms = base_ms + random.randint(0, jitter_ms)
        return delay_ms / 1000.0
    
    def inject_stealth_scripts(self) -> List[str]:
        """
        Get JavaScript snippets to inject for stealth.
        
        Returns:
            List of JavaScript code strings to inject
        """
        fingerprint = self.get_fingerprint()
        
        scripts = [
            # Override navigator properties
            f"""
            Object.defineProperty(navigator, 'platform', {{
                get: () => '{fingerprint.platform}'
            }});
            """,
            # Override WebGL
            f"""
            const getParameter = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(parameter) {{
                if (parameter === 37445) {{
                    return '{fingerprint.webgl_vendor}';
                }}
                if (parameter === 37446) {{
                    return '{fingerprint.webgl_renderer}';
                }}
                return getParameter.call(this, parameter);
            }};
            """,
            # Override plugins
            """
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            """,
            # Override languages
            f"""
            Object.defineProperty(navigator, 'languages', {{
                get: () => {fingerprint.languages}
            }});
            """,
            # Remove automation flags
            """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            """,
            # Override permissions
            """
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
            """,
        ]
        
        return scripts




