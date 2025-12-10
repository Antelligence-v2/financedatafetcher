"""
Browser automation utilities using Playwright.
Handles dynamic page loading and network request interception.
"""

import asyncio
import json
import os
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Callable
from pathlib import Path

from .logger import get_logger
from .io_utils import ensure_dir, save_raw_response


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
    ):
        """
        Initialize the browser manager.
        
        Args:
            headless: Run browser in headless mode
            user_agent: Custom user agent string
            timeout: Default timeout in milliseconds
        """
        self.headless = headless
        self.user_agent = user_agent or "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        self.timeout = timeout
        self.logger = get_logger()
        
        self._playwright = None
        self._browser = None
        self._context = None
        self._installation_attempted = False  # Track if we've tried to install browsers
    
    def _check_browser_installed(self) -> bool:
        """
        Check if Playwright browsers are installed.
        
        Returns:
            True if browsers appear to be installed, False otherwise
        """
        try:
            # Check common Playwright browser cache locations
            home = os.path.expanduser("~")
            playwright_paths = [
                os.path.join(home, ".cache", "ms-playwright"),
                os.path.join(home, ".local", "share", "ms-playwright"),
            ]
            
            for base_path in playwright_paths:
                if os.path.exists(base_path):
                    # Look for chromium executable
                    chromium_paths = [
                        os.path.join(base_path, "chromium_headless_shell-*", "chrome-headless-shell-linux64", "chrome-headless-shell"),
                        os.path.join(base_path, "chromium-*", "chrome-linux64", "chrome"),
                    ]
                    for pattern in chromium_paths:
                        import glob
                        matches = glob.glob(pattern)
                        if matches and os.path.exists(matches[0]):
                            self.logger.debug(f"Found Chromium at: {matches[0]}")
                            return True
            return False
        except Exception as e:
            self.logger.debug(f"Error checking browser installation: {e}")
            return False
    
    def _classify_error(self, error: Exception) -> str:
        """
        Classify browser launch error to provide better error messages.
        
        Args:
            error: The exception that occurred
            
        Returns:
            Error classification: 'missing_browser', 'missing_deps', or 'runtime_error'
        """
        error_msg = str(error).lower()
        
        # Check for missing system dependencies
        if any(keyword in error_msg for keyword in [
            "libnspr4", "libnss3", "libatk", "libcairo", "libpango",
            "shared libraries", "cannot open shared object", "no such file or directory"
        ]):
            return "missing_deps"
        
        # Check for missing browser executable
        if any(keyword in error_msg for keyword in [
            "executable", "browser", "not found", "no such file", "chromium",
            "executable doesn't exist"
        ]):
            return "missing_browser"
        
        # Other runtime errors
        return "runtime_error"
    
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
        
        # First, try to launch browser (browsers should be pre-installed during deployment)
        try:
            self._browser = await self._playwright.chromium.launch(headless=self.headless)
            self.logger.info("Browser launched successfully (pre-installed)")
        except Exception as e:
            error_class = self._classify_error(e)
            self.logger.warning(f"Browser launch failed: {error_class}")
            
            # Only attempt runtime installation as last resort
            # Check if we've already attempted installation in this session
            if self._installation_attempted:
                if error_class == "missing_deps":
                    raise RuntimeError(
                        "Playwright browser dependencies are missing. "
                        "System dependencies should be installed via packages.txt during deployment. "
                        "Please ensure packages.txt includes all required libraries (libnss3, libnspr4, etc.)."
                    )
                elif error_class == "missing_browser":
                    raise RuntimeError(
                        "Playwright browsers are not installed. "
                        "Browsers should be installed during deployment via post_install.sh. "
                        "If this persists, check deployment logs for browser installation errors."
                    )
                else:
                    raise RuntimeError(
                        f"Browser launch failed with runtime error: {str(e)}"
                    )
            
            # Check if browsers appear to be installed (but launch failed)
            browsers_exist = self._check_browser_installed()
            
            if error_class == "missing_deps":
                # System dependencies missing - should be in packages.txt
                raise RuntimeError(
                    "Playwright browser dependencies are missing. "
                    "This indicates packages.txt may not have been processed correctly during deployment. "
                    "Required libraries: libnss3, libnspr4, libatk1.0-0, libatk-bridge2.0-0, and others. "
                    f"Original error: {str(e)}"
                )
            
            # If browsers don't exist and we haven't tried installing, attempt it
            if not browsers_exist and not self._installation_attempted:
                self.logger.warning("Browsers not found. Attempting runtime installation as fallback...")
                self._installation_attempted = True
                
                try:
                    import subprocess
                    import sys
                    # Try installing without --with-deps first (deps should be from packages.txt)
                    self.logger.info("Installing Playwright browsers (system deps should already be installed)...")
                    result = subprocess.run(
                        [sys.executable, "-m", "playwright", "install", "chromium"],
                        capture_output=True,
                        text=True,
                        timeout=300,  # 5 minute timeout (reduced since deps should be pre-installed)
                    )
                    if result.returncode == 0:
                        self.logger.info("Browsers installed successfully. Retrying launch...")
                        # Close and recreate playwright instance
                        await self._playwright.stop()
                        self._playwright = await async_playwright().start()
                        self._browser = await self._playwright.chromium.launch(headless=self.headless)
                    else:
                        error_output = result.stderr or result.stdout or "Unknown error"
                        self.logger.error(f"Runtime browser installation failed: {error_output}")
                        raise RuntimeError(
                            f"Browser installation failed. This should have been done during deployment. "
                            f"Error: {error_output}"
                        )
                except subprocess.TimeoutExpired:
                    self.logger.error("Browser installation timed out")
                    raise RuntimeError(
                        "Browser installation timed out. Browsers should be pre-installed during deployment. "
                        "Please check deployment logs and ensure post_install.sh runs successfully."
                    )
                except Exception as install_error:
                    self.logger.error(f"Error during browser installation: {install_error}")
                    raise RuntimeError(
                        f"Browser installation failed. This should have been done during deployment. "
                        f"Error: {str(install_error)}"
                    )
            else:
                # Browsers exist but launch failed - likely a runtime error
                raise RuntimeError(
                    f"Browser launch failed even though browsers appear to be installed. "
                    f"This may indicate a system dependency issue or configuration problem. "
                    f"Error: {str(e)}"
                )
        
        self._context = await self._browser.new_context(
            user_agent=self.user_agent,
            viewport={"width": 1920, "height": 1080},
        )
        self.logger.info("Browser started")
    
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
        
        Returns:
            PageLoadResult with page content and captured requests
        """
        if not self._context:
            await self.start()
        
        page = await self._context.new_page()
        network_requests: List[NetworkRequest] = []
        
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
            await page.goto(url, timeout=self.timeout, wait_until="networkidle")
            
            # Wait for selector if specified
            if wait_for_selector:
                try:
                    await page.wait_for_selector(wait_for_selector, timeout=wait_for_timeout)
                except Exception:
                    self.logger.warning(f"Selector '{wait_for_selector}' not found within timeout")
            
            # Additional wait for dynamic content
            await asyncio.sleep(wait_for_timeout / 1000)
            
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
    ) -> Any:
        """
        Load a page and evaluate JavaScript.
        
        Args:
            url: URL to load
            script: JavaScript code to evaluate
            wait_for_selector: CSS selector to wait for
        
        Returns:
            Result of JavaScript evaluation
        """
        if not self._context:
            await self.start()
        
        page = await self._context.new_page()
        
        try:
            await page.goto(url, timeout=self.timeout, wait_until="networkidle")
            
            if wait_for_selector:
                await page.wait_for_selector(wait_for_selector, timeout=10000)
            
            result = await page.evaluate(script)
            return result
        
        finally:
            await page.close()


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
