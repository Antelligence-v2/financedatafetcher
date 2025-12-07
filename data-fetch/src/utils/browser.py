"""
Browser automation utilities using Playwright.
Handles dynamic page loading and network request interception.
"""

import asyncio
import json
import random
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Callable
from pathlib import Path

from .logger import get_logger
from .io_utils import ensure_dir, save_raw_response
from .stealth import StealthManager


@dataclass
class NetworkRequest:
    """Captured network request."""
    url: str
    method: str
    resource_type: str
    status: Optional[int] = None
    content_type: Optional[str] = None
    content_length: Optional[int] = None
    response_body: Optional[bytes] = None
    headers: Dict[str, str] = field(default_factory=dict)
    
    @property
    def is_json(self) -> bool:
        return self.content_type and "json" in self.content_type.lower()
    
    @property
    def is_html(self) -> bool:
        return self.content_type and "html" in self.content_type.lower()
    
    @property
    def is_csv(self) -> bool:
        return self.content_type and "csv" in self.content_type.lower()
    
    @property
    def is_data_response(self) -> bool:
        """Check if this might be a data response (not scripts, styles, etc.)"""
        return self.is_json or self.is_csv or self.is_html


@dataclass
class PageLoadResult:
    """Result of loading a page."""
    url: str
    html: str
    title: str
    network_requests: List[NetworkRequest]
    screenshot_path: Optional[Path] = None
    error: Optional[str] = None
    load_time_ms: int = 0


class BrowserManager:
    """
    Manager for Playwright browser automation.
    Handles page loading and network request capture.
    """
    
    def __init__(
        self,
        headless: bool = True,
        user_agent: Optional[str] = None,
        timeout: int = 30000,
        use_stealth: bool = True,
        cookies: Optional[List[Dict[str, Any]]] = None,
        proxy: Optional[str] = None,
    ):
        """
        Initialize the browser manager.
        
        Args:
            headless: Run browser in headless mode
            user_agent: Custom user agent string
            timeout: Default timeout in milliseconds
            use_stealth: Enable stealth mode (fingerprint randomization)
            cookies: List of cookies to set (Playwright cookie format)
            proxy: Proxy server URL (e.g., "http://proxy.example.com:8080")
        """
        self.headless = headless
        self.user_agent = user_agent
        self.timeout = timeout
        self.use_stealth = use_stealth
        self.cookies = cookies or []
        self.proxy = proxy
        self.logger = get_logger()
        
        self.stealth_manager = StealthManager() if use_stealth else None
        
        self._playwright = None
        self._browser = None
        self._context = None
    
    async def __aenter__(self):
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    async def start(self):
        """Start the browser."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise ImportError(
                "Playwright is required. Install with: pip install playwright && playwright install"
            )
        
        self._playwright = await async_playwright().start()
        
        # Configure browser launch options
        launch_options = {
            "headless": self.headless,
        }
        if self.proxy:
            launch_options["proxy"] = {"server": self.proxy}
        
        self._browser = await self._playwright.chromium.launch(**launch_options)
        
        # Configure context with stealth options
        context_options = {}
        
        if self.use_stealth and self.stealth_manager:
            fingerprint = self.stealth_manager.get_fingerprint()
            context_options = self.stealth_manager.get_playwright_context_options()
            if self.user_agent:
                context_options["user_agent"] = self.user_agent
        else:
            context_options = {
                "user_agent": self.user_agent or "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "viewport": {"width": 1920, "height": 1080},
            }
        
        self._context = await self._browser.new_context(**context_options)
        
        # Set cookies if provided
        if self.cookies:
            await self._context.add_cookies(self.cookies)
            self.logger.info(f"Added {len(self.cookies)} cookies to context")
        
        self.logger.info("Browser started" + (" (stealth mode)" if self.use_stealth else ""))
    
    async def close(self):
        """Close the browser."""
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        self.logger.info("Browser closed")
    
    async def load_page(
        self,
        url: str,
        wait_for_selector: Optional[str] = None,
        wait_for_timeout: int = 5000,
        capture_network: bool = True,
        capture_response_bodies: bool = True,
        take_screenshot: bool = False,
        screenshot_path: Optional[Path] = None,
        wait_for_js_variable: Optional[str] = None,
        wait_for_network_idle: bool = True,
        inject_stealth: bool = True,
        wait_for_data_loaded: bool = False,
    ) -> PageLoadResult:
        """
        Load a page and optionally capture network requests.
        
        Args:
            url: URL to load
            wait_for_selector: CSS selector to wait for (indicates page is ready)
            wait_for_timeout: Additional time to wait after page load (ms)
            capture_network: Whether to capture network requests
            capture_response_bodies: Whether to capture response bodies (can be large)
            take_screenshot: Whether to take a screenshot
            screenshot_path: Path to save screenshot
            wait_for_js_variable: JavaScript variable name to wait for (e.g., "window.dataLoaded")
            wait_for_network_idle: Wait for network to be idle
            inject_stealth: Inject stealth scripts to bypass detection
        
        Returns:
            PageLoadResult with page content and captured requests
        """
        if not self._context:
            await self.start()
        
        page = await self._context.new_page()
        network_requests: List[NetworkRequest] = []
        
        # Inject stealth scripts if enabled
        if inject_stealth and self.use_stealth and self.stealth_manager:
            stealth_scripts = self.stealth_manager.inject_stealth_scripts()
            for script in stealth_scripts:
                await page.add_init_script(script)
            self.logger.debug("Injected stealth scripts")
        
        # Setup network request capture
        if capture_network:
            async def handle_response(response):
                try:
                    request = response.request
                    
                    # Get content type from headers
                    content_type = response.headers.get("content-type", "")
                    content_length = response.headers.get("content-length")
                    
                    # Create network request object
                    net_request = NetworkRequest(
                        url=request.url,
                        method=request.method,
                        resource_type=request.resource_type,
                        status=response.status,
                        content_type=content_type,
                        content_length=int(content_length) if content_length else None,
                        headers=dict(response.headers),
                    )
                    
                    # Capture response body for data responses
                    if capture_response_bodies and net_request.is_data_response:
                        try:
                            net_request.response_body = await response.body()
                        except Exception:
                            pass  # Some responses can't be read
                    
                    network_requests.append(net_request)
                except Exception as e:
                    self.logger.debug(f"Error capturing response: {e}")
            
            page.on("response", handle_response)
        
        # Load the page
        start_time = asyncio.get_event_loop().time()
        error = None
        
        try:
            # Add random delay before navigation (stealth)
            if self.use_stealth and self.stealth_manager:
                delay = self.stealth_manager.get_random_delay(base_ms=500, jitter_ms=300)
                await asyncio.sleep(delay)
            
            wait_until = "networkidle" if wait_for_network_idle else "load"
            await page.goto(url, timeout=self.timeout, wait_until=wait_until)
            
            # Wait for JavaScript variable if specified
            if wait_for_js_variable:
                try:
                    await page.wait_for_function(
                        f"typeof {wait_for_js_variable} !== 'undefined'",
                        timeout=wait_for_timeout,
                    )
                    self.logger.debug(f"Detected JavaScript variable: {wait_for_js_variable}")
                except Exception:
                    self.logger.warning(f"JavaScript variable '{wait_for_js_variable}' not found within timeout")
            
            # Wait for selector if specified
            if wait_for_selector:
                try:
                    await page.wait_for_selector(wait_for_selector, timeout=wait_for_timeout)
                except Exception:
                    self.logger.warning(f"Selector '{wait_for_selector}' not found within timeout")
            
            # Wait for network to be idle (if not already waited)
            if wait_for_network_idle:
                try:
                    await page.wait_for_load_state("networkidle", timeout=5000)
                except Exception:
                    pass  # Already idle or timeout
            
            # Additional wait for dynamic content
            await asyncio.sleep(wait_for_timeout / 1000)
            
            # Use smart waiting for data if requested (for financial sites)
            if wait_for_data_loaded:
                self.logger.debug("Using smart waiting for data to load...")
                data_loaded = await self.wait_for_data_loaded(
                    page,
                    timeout=wait_for_timeout,
                    check_interval=500,
                )
                if data_loaded:
                    self.logger.debug("Data loading detected")
                else:
                    self.logger.debug("Data loading timeout, proceeding anyway")
            
        except Exception as e:
            error = str(e)
            self.logger.error(f"Error loading page: {e}")
        
        load_time_ms = int((asyncio.get_event_loop().time() - start_time) * 1000)
        
        # Get page content
        html = await page.content() if not error else ""
        title = await page.title() if not error else ""
        
        # Take screenshot if requested
        screenshot_saved_path = None
        if take_screenshot and not error:
            if screenshot_path is None:
                from .io_utils import generate_run_id, get_output_path
                screenshot_path = get_output_path(
                    f"screenshot_{generate_run_id()}.png",
                    "raw"
                )
            ensure_dir(screenshot_path.parent)
            await page.screenshot(path=str(screenshot_path), full_page=True)
            screenshot_saved_path = screenshot_path
        
        await page.close()
        
        self.logger.info(
            f"Loaded {url} in {load_time_ms}ms, "
            f"captured {len(network_requests)} network requests"
        )
        
        return PageLoadResult(
            url=url,
            html=html,
            title=title,
            network_requests=network_requests,
            screenshot_path=screenshot_saved_path,
            error=error,
            load_time_ms=load_time_ms,
        )
    
    async def evaluate_js(
        self,
        url: str,
        script: str,
        wait_for_selector: Optional[str] = None,
        wait_for_js_variable: Optional[str] = None,
        inject_stealth: bool = True,
    ) -> Any:
        """
        Load a page and evaluate JavaScript.
        
        Args:
            url: URL to load
            script: JavaScript code to evaluate
            wait_for_selector: CSS selector to wait for
            wait_for_js_variable: JavaScript variable name to wait for
            inject_stealth: Inject stealth scripts
        
        Returns:
            Result of JavaScript evaluation
        """
        if not self._context:
            await self.start()
        
        page = await self._context.new_page()
        
        # Inject stealth scripts if enabled
        if inject_stealth and self.use_stealth and self.stealth_manager:
            stealth_scripts = self.stealth_manager.inject_stealth_scripts()
            for script in stealth_scripts:
                await page.add_init_script(script)
        
        try:
            await page.goto(url, timeout=self.timeout, wait_until="networkidle")
            
            if wait_for_js_variable:
                await page.wait_for_function(
                    f"typeof {wait_for_js_variable} !== 'undefined'",
                    timeout=10000,
                )
            
            if wait_for_selector:
                await page.wait_for_selector(wait_for_selector, timeout=10000)
            
            result = await page.evaluate(script)
            return result
        
        finally:
            await page.close()
    
    async def wait_for_data_loaded(
        self,
        page,
        timeout: int = 10000,
        check_interval: int = 500,
        css_classes: Optional[List[str]] = None,
        loading_indicators: Optional[List[str]] = None,
    ) -> bool:
        """
        Wait for data to be loaded on the page by checking for common indicators.
        
        Args:
            page: Playwright page object
            timeout: Maximum time to wait (ms)
            check_interval: Interval between checks (ms)
            css_classes: List of CSS classes that indicate data is loaded
            loading_indicators: List of CSS selectors for loading indicators that should disappear
        
        Returns:
            True if data appears to be loaded, False if timeout
        """
        start_time = asyncio.get_event_loop().time() * 1000
        
        # Default CSS classes that indicate data loading completion
        default_css_classes = [
            "data-loaded", "loaded", "ready", "initialized",
            "table-loaded", "chart-ready", "data-ready",
        ]
        css_classes = css_classes or default_css_classes
        
        # Default loading indicators that should disappear
        default_loading_indicators = [
            ".loading", ".spinner", ".loader", "[data-loading='true']",
            ".skeleton", ".placeholder",
        ]
        loading_indicators = loading_indicators or default_loading_indicators
        
        while (asyncio.get_event_loop().time() * 1000 - start_time) < timeout:
            try:
                # Check for common data indicators
                checks = [
                    # Check for data in window object
                    "typeof window.__INITIAL_STATE__ !== 'undefined'",
                    "typeof window.__DATA__ !== 'undefined'",
                    "typeof window.data !== 'undefined'",
                    "typeof window.chartData !== 'undefined'",
                    "typeof window.seriesData !== 'undefined'",
                    # Check for loaded tables
                    "document.querySelectorAll('table tbody tr').length > 0",
                    "document.querySelectorAll('table tr').length > 1",  # At least header + 1 row
                    # Check for chart data
                    "document.querySelectorAll('[data-chart], [data-series]').length > 0",
                    # Check for specific CSS classes
                    *[f"document.querySelectorAll('.{cls}').length > 0" for cls in css_classes],
                ]
                
                for check in checks:
                    try:
                        result = await page.evaluate(f"({check})")
                        if result:
                            self.logger.debug(f"Data loaded detected via: {check}")
                            # Also check that loading indicators are gone
                            if await self._check_loading_indicators_gone(page, loading_indicators):
                                return True
                    except Exception:
                        continue
                
                # Check if loading indicators have disappeared
                if await self._check_loading_indicators_gone(page, loading_indicators):
                    # Double-check that data is present
                    has_data = await page.evaluate("""
                        () => {
                            return document.querySelectorAll('table tbody tr').length > 0 ||
                                   document.querySelectorAll('[data-chart]').length > 0 ||
                                   typeof window.data !== 'undefined';
                        }
                    """)
                    if has_data:
                        return True
                
                await asyncio.sleep(check_interval / 1000)
            except Exception as e:
                self.logger.debug(f"Error checking data load status: {e}")
                await asyncio.sleep(check_interval / 1000)
        
        return False
    
    async def _check_loading_indicators_gone(
        self,
        page,
        loading_indicators: List[str],
    ) -> bool:
        """Check if loading indicators have disappeared."""
        for indicator in loading_indicators:
            try:
                count = await page.evaluate(f"document.querySelectorAll('{indicator}').length")
                if count > 0:
                    return False
            except Exception:
                continue
        return True
    
    async def wait_for_network_request(
        self,
        page,
        url_pattern: str,
        timeout: int = 10000,
    ) -> Optional[Dict[str, Any]]:
        """
        Wait for a specific network request to complete.
        
        Args:
            page: Playwright page object
            url_pattern: Pattern to match in URL (regex or substring)
            timeout: Maximum time to wait (ms)
        
        Returns:
            Request/response info or None if timeout
        """
        import re
        
        pattern = re.compile(url_pattern) if url_pattern else None
        request_info = None
        
        async def handle_response(response):
            nonlocal request_info
            if pattern and pattern.search(response.url):
                request_info = {
                    "url": response.url,
                    "status": response.status,
                    "headers": dict(response.headers),
                }
                try:
                    request_info["body"] = await response.text()
                except Exception:
                    pass
        
        page.on("response", handle_response)
        
        start_time = asyncio.get_event_loop().time() * 1000
        while (asyncio.get_event_loop().time() * 1000 - start_time) < timeout:
            if request_info:
                page.remove_listener("response", handle_response)
                return request_info
            await asyncio.sleep(100 / 1000)  # Check every 100ms
        
        page.remove_listener("response", handle_response)
        return None
    
    async def wait_for_dom_mutation(
        self,
        page,
        selector: str,
        timeout: int = 10000,
        check_children: bool = True,
    ) -> bool:
        """
        Wait for DOM mutations (elements being added/removed).
        
        Args:
            page: Playwright page object
            selector: CSS selector to watch
            timeout: Maximum time to wait (ms)
            check_children: Whether to check for children being added
        
        Returns:
            True if mutation detected, False if timeout
        """
        initial_count = await page.evaluate(f"document.querySelectorAll('{selector}').length")
        
        start_time = asyncio.get_event_loop().time() * 1000
        while (asyncio.get_event_loop().time() * 1000 - start_time) < timeout:
            current_count = await page.evaluate(f"document.querySelectorAll('{selector}').length")
            
            if check_children:
                # Check if children were added
                children_count = await page.evaluate(f"""
                    () => {{
                        const elements = document.querySelectorAll('{selector}');
                        let total = 0;
                        elements.forEach(el => total += el.children.length);
                        return total;
                    }}
                """)
                if children_count > 0:
                    return True
            
            if current_count > initial_count:
                return True
            
            await asyncio.sleep(200 / 1000)  # Check every 200ms
        
        return False
    
    async def capture_page_context_on_error(
        self,
        page,
        error: Exception,
    ) -> Dict[str, Any]:
        """
        Capture page context when an error occurs for better debugging.
        
        Args:
            page: Playwright page object
            error: The exception that occurred
        
        Returns:
            Dictionary with page context information
        """
        context = {
            "error": str(error),
            "error_type": type(error).__name__,
            "url": page.url,
            "title": await page.title(),
        }
        
        try:
            # Capture screenshot
            screenshot = await page.screenshot()
            context["screenshot"] = screenshot
            
            # Capture console errors
            console_messages = []
            page.on("console", lambda msg: console_messages.append({
                "type": msg.type,
                "text": msg.text,
            }))
            context["console_errors"] = [
                msg for msg in console_messages if msg["type"] == "error"
            ]
            
            # Capture network errors
            network_errors = []
            page.on("response", lambda response: network_errors.append({
                "url": response.url,
                "status": response.status,
            }) if response.status >= 400 else None)
            context["network_errors"] = [
                err for err in network_errors if err["status"] >= 400
            ]
            
        except Exception as e:
            self.logger.warning(f"Error capturing page context: {e}")
            context["context_capture_error"] = str(e)
        
        return context
    
    def get_cookies(self) -> List[Dict[str, Any]]:
        """
        Get cookies from the current browser context.
        
        Returns:
            List of cookies in Playwright format
        """
        if not self._context:
            return []
        
        # Note: This is async, but we return a sync method
        # In practice, this should be called from an async context
        async def _get():
            return await self._context.cookies()
        
        try:
            return asyncio.run(_get())
        except Exception:
            return []


def load_page_sync(
    url: str,
    wait_for_selector: Optional[str] = None,
    wait_for_timeout: int = 5000,
    capture_network: bool = True,
    headless: bool = True,
) -> PageLoadResult:
    """
    Synchronous wrapper for loading a page.
    
    Args:
        url: URL to load
        wait_for_selector: CSS selector to wait for
        wait_for_timeout: Additional wait time in ms
        capture_network: Whether to capture network requests
        headless: Run browser in headless mode
    
    Returns:
        PageLoadResult with page content and captured requests
    """
    async def _load():
        async with BrowserManager(headless=headless) as browser:
            return await browser.load_page(
                url=url,
                wait_for_selector=wait_for_selector,
                wait_for_timeout=wait_for_timeout,
                capture_network=capture_network,
            )
    
    return asyncio.run(_load())


def filter_data_requests(requests: List[NetworkRequest]) -> List[NetworkRequest]:
    """
    Filter network requests to only include potential data endpoints.
    
    Args:
        requests: List of captured network requests
    
    Returns:
        Filtered list of data-relevant requests
    """
    return [
        r for r in requests
        if r.is_data_response
        and r.status == 200
        and not any(x in r.url.lower() for x in [
            "analytics", "tracking", "pixel", "beacon",
            "facebook", "google-analytics", "clarity",
            "fonts", "icons", ".css", ".js",
            "cookie", "consent", "recaptcha",
        ])
    ]

