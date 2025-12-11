#!/usr/bin/env python3
"""
Data-Fetch CLI - Financial Data Scraper Framework

A command-line tool for extracting financial data from websites
and exporting to Excel.

Usage:
    python main.py scrape --site theblock_btc_eth_volume_7dma
    python main.py scrape --url https://example.com/data
    python main.py setup --url https://example.com/data
    python main.py list-sites
    python main.py test --url https://example.com/data
"""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

import click

# Load environment variables from data-fetch/.env
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        # Try parent .env as fallback
        load_dotenv(dotenv_path=None)
except Exception:
    pass  # Continue without .env if there are issues


@click.group()
@click.version_option(version="0.1.0", prog_name="data-fetch")
@click.option("--debug/--no-debug", default=False, help="Enable debug logging")
def cli(debug: bool):
    """
    Data-Fetch: Financial Data Scraper Framework
    
    Extract financial data from websites and export to Excel.
    """
    from src.utils.logger import setup_logging
    
    level = "DEBUG" if debug else os.getenv("LOG_LEVEL", "INFO")
    setup_logging(level=level)


@cli.command()
@click.option("--site", "-s", help="Site ID from configuration")
@click.option("--url", "-u", help="URL to scrape (uses universal scraper)")
@click.option("--output", "-o", help="Output Excel file path")
@click.option("--override-robots", is_flag=True, help="Override robots.txt for UNKNOWN status")
@click.option("--no-export", is_flag=True, help="Skip Excel export")
@click.option("--use-fallbacks/--no-fallbacks", default=True, help="Use fallback sources if primary fails")
def scrape(
    site: str,
    url: str,
    output: str,
    override_robots: bool,
    no_export: bool,
    use_fallbacks: bool,
):
    """
    Scrape data from a configured site or URL.
    
    Examples:
    
        python main.py scrape --site theblock_btc_eth_volume_7dma
        
        python main.py scrape --url https://www.theblock.co/data/crypto-markets/spot/btc-and-eth-total-exchange-volume-7dma
    """
    from src.utils.logger import get_logger
    from src.utils.config_manager import ConfigManager
    from src.pipeline.pipeline_runner import PipelineRunner
    from src.exporter.excel_exporter import ExcelExporter
    
    logger = get_logger()
    
    if not site and not url:
        click.echo("Error: Either --site or --url must be provided", err=True)
        sys.exit(1)
    
    # Setup pipeline
    config_manager = ConfigManager()
    
    # Custom exporter if output path specified
    exporter = None
    if output:
        output_path = Path(output)
        exporter = ExcelExporter(output_dir=output_path.parent)
    
    runner = PipelineRunner(
        config_manager=config_manager,
        exporter=exporter,
    )
    
    # Determine fallbacks
    fallback_sites = None
    if use_fallbacks and site:
        fallback_sites = ["coingecko_btc_market_chart", "cryptocompare_exchange_volume"]
    
    # Run pipeline
    click.echo(f"Starting scrape...")
    
    result = runner.run(
        site_id=site,
        url=url,
        override_robots=override_robots,
        export=not no_export,
        fallback_sites=fallback_sites,
    )
    
    # Print results
    if result.success:
        click.echo(click.style("✓ Scrape successful!", fg="green", bold=True))
        click.echo(f"  Source: {result.source_used}")
        click.echo(f"  Rows extracted: {result.scraper_result.rows_extracted if result.scraper_result else 0}")
        
        if result.scraper_result and result.scraper_result.date_range[0]:
            click.echo(f"  Date range: {result.scraper_result.date_range[0]} to {result.scraper_result.date_range[1]}")
        
        if result.validation_result and result.validation_result.warnings:
            click.echo(f"  Warnings: {len(result.validation_result.warnings)}")
        
        if result.output_path:
            click.echo(f"  Output: {result.output_path}")
        
        click.echo(f"  Time: {result.run_time_seconds:.2f}s")
    else:
        click.echo(click.style("✗ Scrape failed!", fg="red", bold=True))
        click.echo(f"  Error: {result.error}")
        
        if result.sources_failed:
            click.echo("  Sources tried:")
            for source, error in result.sources_failed.items():
                click.echo(f"    - {source}: {error}")
        
        sys.exit(1)


@cli.command()
@click.option("--url", "-u", required=True, help="URL to set up")
@click.option("--non-interactive", is_flag=True, help="Run without interactive prompts")
@click.option("--override-robots", is_flag=True, help="Override robots.txt for UNKNOWN status")
def setup(url: str, non_interactive: bool, override_robots: bool):
    """
    Set up a new website for scraping.
    
    This command guides you through discovering data sources on a page
    and creating a configuration for future scrapes.
    
    Example:
    
        python main.py setup --url https://example.com/data
    """
    from src.setup.new_website_setup import NewWebsiteSetup
    
    setup_wizard = NewWebsiteSetup()
    
    if non_interactive:
        result = setup_wizard.run_setup_non_interactive(url, override_robots)
    else:
        result = setup_wizard.run_setup(url)
    
    if result and result.get("success"):
        click.echo(click.style("\n✓ Setup complete!", fg="green", bold=True))
        click.echo(f"  Site ID: {result['site_id']}")
        click.echo(f"\nYou can now scrape with:")
        click.echo(f"  python main.py scrape --site {result['site_id']}")
    else:
        click.echo(click.style("\n✗ Setup failed or cancelled", fg="red"))
        sys.exit(1)


@cli.command("list-sites")
def list_sites():
    """
    List all configured sites.
    
    Shows site IDs, names, and URLs from the configuration file.
    """
    from src.utils.config_manager import ConfigManager
    
    config_manager = ConfigManager()
    sites = config_manager.list_sites()
    
    if not sites:
        click.echo("No sites configured yet.")
        click.echo("\nAdd a site with:")
        click.echo("  python main.py setup --url <website_url>")
        return
    
    click.echo(f"Configured sites ({len(sites)}):\n")
    
    for site in sites:
        click.echo(f"  {click.style(site['id'], fg='cyan', bold=True)}")
        click.echo(f"    Name: {site['name']}")
        click.echo(f"    URL:  {site['page_url']}")
        click.echo()


@cli.command()
@click.option("--url", "-u", required=True, help="URL to test")
@click.option("--override-robots", is_flag=True, help="Override robots.txt for UNKNOWN status")
def test(url: str, override_robots: bool):
    """
    Test data extraction from a URL without saving.
    
    Performs discovery and shows what data would be extracted.
    
    Example:
    
        python main.py test --url https://example.com/data
    """
    from src.utils.robots import check_robots_permission
    from src.scraper.universal_scraper import UniversalScraper
    
    click.echo(f"Testing: {url}\n")
    
    # Check robots.txt
    click.echo("Checking robots.txt...")
    robots = check_robots_permission(url)
    
    status_color = {
        "ALLOWED": "green",
        "DISALLOWED": "red",
        "UNKNOWN": "yellow",
    }
    
    click.echo(f"  Status: {click.style(robots.status.value, fg=status_color.get(robots.status.value, 'white'))}")
    click.echo(f"  Reason: {robots.reason}")
    
    if robots.is_disallowed:
        click.echo(click.style("\n✗ Cannot proceed - scraping is disallowed", fg="red"))
        sys.exit(1)
    
    if robots.is_unknown and not override_robots:
        click.echo(click.style("\n⚠ Use --override-robots to proceed with UNKNOWN status", fg="yellow"))
        sys.exit(1)
    
    # Discover data sources
    click.echo("\nDiscovering data sources...")
    
    scraper = UniversalScraper(use_llm=True)
    discovery = scraper.discover_data_sources_sync(url)
    
    click.echo(f"  Network requests captured: {len(discovery.page_load_result.network_requests) if discovery.page_load_result else 0}")
    click.echo(f"  Candidate endpoints: {len(discovery.candidate_endpoints)}")
    
    if discovery.candidate_endpoints:
        click.echo("\n  Top candidates:")
        for i, endpoint in enumerate(discovery.candidate_endpoints[:3], 1):
            click.echo(f"    {i}. {endpoint.url[:60]}...")
            click.echo(f"       Type: {endpoint.content_type}")
            click.echo(f"       Confidence: {endpoint.confidence_score:.2f}")
            if endpoint.field_names:
                click.echo(f"       Fields: {', '.join(endpoint.field_names[:5])}")
    
    # Test extraction
    click.echo("\nTesting extraction...")
    
    try:
        result = scraper.scrape(url, override_robots=True, save_raw=False)
        
        if result.success and result.data is not None:
            click.echo(click.style(f"\n✓ Extracted {len(result.data)} rows", fg="green"))
            click.echo(f"  Columns: {', '.join(result.data.columns)}")
            
            if len(result.data) > 0:
                click.echo("\n  Sample (first 3 rows):")
                click.echo(result.data.head(3).to_string())
        else:
            click.echo(click.style(f"\n✗ Extraction failed: {result.error}", fg="red"))
    
    except Exception as e:
        click.echo(click.style(f"\n✗ Error: {e}", fg="red"))
        sys.exit(1)


@cli.command()
@click.option("--url", "-u", required=True, help="URL to check")
def check_robots(url: str):
    """
    Check robots.txt permissions for a URL.
    
    Example:
    
        python main.py check-robots --url https://example.com/data
    """
    from src.utils.robots import check_robots_permission
    
    click.echo(f"Checking robots.txt for: {url}\n")
    
    decision = check_robots_permission(url)
    
    status_color = {
        "ALLOWED": "green",
        "DISALLOWED": "red",
        "UNKNOWN": "yellow",
    }
    
    click.echo(f"Status: {click.style(decision.status.value, fg=status_color.get(decision.status.value, 'white'), bold=True)}")
    click.echo(f"Reason: {decision.reason}")
    
    if decision.robots_url:
        click.echo(f"Robots URL: {decision.robots_url}")
    
    if decision.status.value == "ALLOWED":
        click.echo(click.style("\n✓ You can scrape this URL", fg="green"))
    elif decision.status.value == "DISALLOWED":
        click.echo(click.style("\n✗ Scraping this URL is not allowed", fg="red"))
    else:
        click.echo(click.style("\n⚠ Could not determine permissions - use with caution", fg="yellow"))


if __name__ == "__main__":
    cli()

