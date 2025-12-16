"""
Interactive website setup module for the data-fetch framework.
Guides users through adding new websites with yes/no prompts.
"""

import asyncio
from typing import Optional, List, Dict, Any

import pandas as pd

try:
    from rich.console import Console
    from rich.table import Table
    from rich.prompt import Prompt, Confirm
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

from ..utils.logger import get_logger
from ..utils.robots import check_robots_permission, RobotsStatus
from ..utils.config_manager import ConfigManager
from ..scraper.universal_scraper import UniversalScraper, DiscoveryResult
from ..detector.network_inspector import CandidateEndpoint
from .config_generator import ConfigGenerator


class NewWebsiteSetup:
    """
    Interactive setup workflow for adding new websites.
    Uses Rich for beautiful terminal UI.
    """
    
    def __init__(
        self,
        config_manager: Optional[ConfigManager] = None,
        config_generator: Optional[ConfigGenerator] = None,
        use_llm: bool = True,
    ):
        """
        Initialize the setup wizard.
        
        Args:
            config_manager: Config manager for saving configs
            config_generator: Config generator for creating configs
            use_llm: Whether to use LLM for data detection
        """
        self.config_manager = config_manager or ConfigManager()
        self.config_generator = config_generator or ConfigGenerator(self.config_manager)
        self.use_llm = use_llm
        self.logger = get_logger()
        
        if RICH_AVAILABLE:
            self.console = Console()
        else:
            self.console = None
    
    def _print(self, message: str, style: str = ""):
        """Print a message with optional styling."""
        if self.console:
            self.console.print(message, style=style)
        else:
            print(message)
    
    def _print_info(self, message: str):
        """Print an info message."""
        self._print(f"[bold blue][INFO][/bold blue] {message}" if self.console else f"[INFO] {message}")
    
    def _print_success(self, message: str):
        """Print a success message."""
        self._print(f"[bold green][SUCCESS][/bold green] {message}" if self.console else f"[SUCCESS] {message}")
    
    def _print_warning(self, message: str):
        """Print a warning message."""
        self._print(f"[bold yellow][WARNING][/bold yellow] {message}" if self.console else f"[WARNING] {message}")
    
    def _print_error(self, message: str):
        """Print an error message."""
        self._print(f"[bold red][ERROR][/bold red] {message}" if self.console else f"[ERROR] {message}")
    
    def _confirm(self, message: str, default: bool = True) -> bool:
        """Ask for yes/no confirmation."""
        if RICH_AVAILABLE:
            return Confirm.ask(message, default=default)
        else:
            response = input(f"{message} [{'Y/n' if default else 'y/N'}]: ").strip().lower()
            if not response:
                return default
            return response in ("y", "yes")
    
    def _prompt(self, message: str, default: str = "") -> str:
        """Ask for text input."""
        if RICH_AVAILABLE:
            return Prompt.ask(message, default=default)
        else:
            response = input(f"{message} [{default}]: " if default else f"{message}: ")
            return response.strip() or default
    
    def _prompt_choice(self, message: str, choices: List[str]) -> str:
        """Ask user to choose from options."""
        self._print(f"\n{message}")
        for i, choice in enumerate(choices, 1):
            self._print(f"  {i}. {choice}")
        
        while True:
            response = self._prompt("Enter number")
            try:
                idx = int(response) - 1
                if 0 <= idx < len(choices):
                    return choices[idx]
            except ValueError:
                pass
            self._print_error("Invalid choice. Please enter a number.")
    
    def run_setup(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Run the interactive setup workflow.
        
        Args:
            url: URL to set up
        
        Returns:
            Setup result with config if successful, None otherwise
        """
        self._print(Panel(f"[bold]New Website Setup[/bold]\n{url}" if self.console else f"=== New Website Setup ===\n{url}"))
        
        # Step 1: Check robots.txt
        self._print_info("Checking robots.txt permissions...")
        robots_decision = check_robots_permission(url)
        
        if robots_decision.is_disallowed:
            self._print_error(f"Scraping is DISALLOWED by robots.txt: {robots_decision.reason}")
            return None
        elif robots_decision.is_unknown:
            self._print_warning(f"Could not verify robots.txt: {robots_decision.reason}")
            if not self._confirm("Do you want to proceed anyway?", default=False):
                return None
        else:
            self._print_success("Robots.txt allows scraping")
        
        # Step 2: Discover data sources
        self._print_info("Analyzing page and discovering data sources...")
        
        scraper = UniversalScraper(use_llm=self.use_llm)
        discovery = scraper.discover_data_sources_sync(url)
        
        if not discovery.candidate_endpoints and not (
            discovery.page_load_result and discovery.page_load_result.html
        ):
            self._print_error("No data sources found on this page.")
            return None
        
        # Step 3: Show discovered sources
        self._print_info(f"Found {len(discovery.candidate_endpoints)} potential data sources:")
        
        sources = []
        for i, endpoint in enumerate(discovery.candidate_endpoints, 1):
            source_desc = f"API endpoint: {endpoint.url[:60]}... ({endpoint.content_type})"
            if endpoint.detected_structure:
                source_desc += f" - {endpoint.detected_structure}"
            sources.append((endpoint, source_desc))
            self._print(f"  {i}. {source_desc}")
        
        if discovery.page_load_result:
            # Check for HTML tables too
            from ..extractor.table_extractor import TableExtractor
            extractor = TableExtractor()
            tables = extractor.find_tables(discovery.page_load_result.html)
            for i, table in enumerate(tables, len(sources) + 1):
                if table.num_rows > 5:
                    source_desc = f"HTML table: {table.selector} ({table.num_rows} rows, {table.num_cols} cols)"
                    sources.append((table, source_desc))
                    self._print(f"  {i}. {source_desc}")
        
        if not sources:
            self._print_error("No viable data sources found.")
            return None
        
        # Step 4: Let user select source
        self._print("")
        choice_idx = self._prompt("Which data source should we use? (enter number)", "1")
        try:
            selected_idx = int(choice_idx) - 1
            if 0 <= selected_idx < len(sources):
                selected_source, selected_desc = sources[selected_idx]
            else:
                selected_source, selected_desc = sources[0]
        except ValueError:
            selected_source, selected_desc = sources[0]
        
        self._print_info(f"Selected: {selected_desc}")
        
        # Step 5: Show detected fields
        field_names = []
        if isinstance(selected_source, CandidateEndpoint):
            field_names = selected_source.field_names
            extraction_strategy = "api_json"
            data_source = {
                "type": "api",
                "endpoint": selected_source.url,
                "method": "GET",
            }
        else:
            field_names = selected_source.headers if hasattr(selected_source, "headers") else []
            extraction_strategy = "dom_table"
            data_source = {
                "type": "table",
                "selector": selected_source.selector if hasattr(selected_source, "selector") else "table",
            }
        
        if field_names:
            self._print_info(f"Detected fields: {', '.join(field_names[:10])}{'...' if len(field_names) > 10 else ''}")
            
            if not self._confirm("Do these fields look correct?"):
                self._print("You can manually configure field mappings after saving.")
        
        # Step 6: Generate field mappings
        field_mappings = self.config_generator.suggest_field_mappings(field_names)
        
        if field_mappings:
            self._print_info("Suggested field mappings:")
            for target, source in field_mappings.items():
                self._print(f"  {target} -> {source}")
            
            if not self._confirm("Use these mappings?"):
                field_mappings = {}
        
        # Step 7: Test extraction
        self._print_info("Testing data extraction...")
        
        try:
            result = scraper.scrape(url, override_robots=True)
            
            if result.success and result.data is not None:
                self._print_success(f"Extracted {result.rows_extracted} rows successfully!")
                
                # Show sample
                if len(result.data) > 0:
                    self._print("\nSample data (first 5 rows):")
                    self._print(result.data.head().to_string())
                
                if not self._confirm("\nDoes this data look correct?"):
                    self._print("Setup cancelled.")
                    return None
            else:
                self._print_warning(f"Extraction returned no data: {result.error}")
                if not self._confirm("Continue with setup anyway?", default=False):
                    return None
        except Exception as e:
            self._print_warning(f"Test extraction failed: {e}")
            if not self._confirm("Continue with setup anyway?", default=False):
                return None
        
        # Step 8: Save configuration
        if self._confirm("\nSave this configuration?"):
            config = self.config_generator.generate_config(
                url=url,
                extraction_strategy=extraction_strategy,
                data_source=data_source,
                field_mappings=field_mappings,
                robots_status=robots_decision.status.value,
            )
            
            if self.config_generator.save_config(config):
                self._print_success(f"Configuration saved! Site ID: {config.id}")
                self._print(f"You can now use: python main.py scrape --site {config.id}")
                
                return {
                    "success": True,
                    "site_id": config.id,
                    "config": config,
                    "sample_data": result.data if result.success else None,
                }
            else:
                self._print_error("Failed to save configuration.")
                return None
        
        return None
    
    def run_setup_non_interactive(
        self,
        url: str,
        override_robots: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Run setup without user interaction (auto-select best options).
        
        Args:
            url: URL to set up
            override_robots: Override robots.txt for UNKNOWN status
        
        Returns:
            Setup result or None
        """
        self.logger.info(f"Running non-interactive setup for {url}")
        
        # Check robots
        robots_decision = check_robots_permission(url)
        if robots_decision.is_disallowed:
            self.logger.error("Scraping disallowed by robots.txt")
            return None
        if robots_decision.is_unknown and not override_robots:
            self.logger.warning("Robots.txt unknown, use override_robots=True to proceed")
            return None
        
        # Discover and extract
        scraper = UniversalScraper(use_llm=self.use_llm)
        discovery = scraper.discover_data_sources_sync(url)
        
        if not discovery.candidate_endpoints:
            self.logger.error("No data sources found")
            return None
        
        # Use best endpoint
        best_endpoint = discovery.candidate_endpoints[0]
        
        # Generate config
        config = self.config_generator.generate_config(
            url=url,
            extraction_strategy="api_json",
            data_source={
                "type": "api",
                "endpoint": best_endpoint.url,
                "method": "GET",
            },
            field_mappings=self.config_generator.suggest_field_mappings(
                best_endpoint.field_names
            ),
            robots_status=robots_decision.status.value,
        )
        
        self.config_generator.save_config(config)
        
        return {
            "success": True,
            "site_id": config.id,
            "config": config,
        }

