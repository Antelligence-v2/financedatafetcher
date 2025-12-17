"""
Streamlit frontend for Data-Fetch framework.
Simple monochrome interface for scraping financial data.
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import sys
import os
import plotly.graph_objects as go

# Load environment variables from .env file FIRST, before any other imports
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=True)  # override=True ensures env vars take precedence
    else:
        # Try parent directory
        parent_env = Path(__file__).parent.parent / ".env"
        if parent_env.exists():
            load_dotenv(parent_env, override=True)
        else:
            load_dotenv(override=True)  # Try default locations
except ImportError:
    # dotenv not installed, skip
    pass

# Add src to path - ensure we can import from src directory
app_dir = Path(__file__).parent.absolute()
# Add the app directory to Python path (where src/ folder is located)
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))

from src.api.frontend_api import FrontendAPI
from src.news.rss_client import RSSClient, Headline
import yaml

# Helper function to format large numbers as millions
def format_millions(value):
    """Format numbers >= 1,000,000 as X.XXM (millions with 2 decimal places)."""
    if pd.isna(value):
        return value
    try:
        num = float(value)
        if abs(num) >= 1_000_000:
            return f"{num / 1_000_000:.2f}M"
        return value
    except (ValueError, TypeError):
        return value

def format_dataframe_for_display(df):
    """Format numeric columns in dataframe - values >= 1M shown as X.XXM."""
    if df is None or df.empty:
        return df
    
    display_df = df.copy()
    for col in display_df.columns:
        # Check if column is numeric
        if pd.api.types.is_numeric_dtype(display_df[col]):
            # Apply formatting to values >= 1 million
            display_df[col] = display_df[col].apply(format_millions)
    return display_df

def check_browser_available():
    """Check if Playwright browser is available for scraping."""
    try:
        from playwright.sync_api import sync_playwright
        # Just check if the module imports - don't actually launch browser
        return True
    except ImportError:
        return False

# Startup checks
def check_environment():
    """Check environment setup and display warnings if needed."""
    warnings = []
    
    # Check OpenAI API key
    # Environment variables are loaded from .env file or system environment
    openai_key = os.getenv("OPENAI_API_KEY")
    
    if not openai_key:
        warnings.append("OpenAI API key not found. LLM-powered data detection will be disabled.")
    
    # Check Alpha Vantage API key
    # Environment variables are loaded from .env file or system environment
    alphavantage_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    
    if not alphavantage_key:
        warnings.append("Alpha Vantage API key not found. Alpha Vantage data sources will be disabled.")
    
    # Playwright browser check removed - no warnings displayed
    
    return warnings

# Run startup checks (only once, cached)
@st.cache_resource
def get_startup_warnings():
    return check_environment()

startup_warnings = get_startup_warnings()

# Page configuration
st.set_page_config(
    page_title="Data-Fetch: Financial Data Scraper",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Main header
st.title("Finance Data Fetcher")

# Custom CSS for monochrome theme
st.markdown("""
    <style>
    .main {
        background-color: #FFFFFF;
    }
    .stButton>button {
        background-color: #000000;
        color: #FFFFFF;
        border: 1px solid #000000;
    }
    .stButton>button:hover {
        background-color: #333333;
        border: 1px solid #333333;
    }
    .stTextInput>div>div>input {
        border: 1px solid #CCCCCC;
    }
    .stSelectbox>div>div>select {
        border: 1px solid #CCCCCC;
    }
    .stCheckbox>label {
        color: #000000;
    }
    h1, h2, h3 {
        color: #000000;
    }
    .stDataFrame {
        border: 1px solid #CCCCCC;
    }
    </style>
    """, unsafe_allow_html=True)

# Initialize API
@st.cache_resource
def get_api():
    return FrontendAPI()

api = get_api()

# Display startup warnings if any
if startup_warnings:
    with st.expander("Environment Warnings", expanded=True):
        for warning in startup_warnings:
            st.warning(warning)

# Get configured sites (needed for tabs)
sites = api.get_configured_sites()

# Load news sources configuration
@st.cache_data
def load_news_sources():
    """Load news sources from YAML config."""
    try:
        config_path = Path(__file__).parent / "config" / "news_sources.yaml"
        if config_path.exists():
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                return config.get('sources', [])
    except Exception as e:
        st.error(f"Error loading news sources: {e}")
    return []

news_sources = load_news_sources()

# Initialize RSS client
@st.cache_resource
def get_rss_client():
    return RSSClient(timeout=10)

rss_client = get_rss_client()

# Main content
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Crypto",
    "Market Sentiment",
    "Dental ETFs",
    "Fintech News",
    "Dev"
])

with tab1:
    st.subheader("Crypto Data Sources")

    # Filter crypto-related sites (those with asset field)
    crypto_sites = [s for s in sites if s.get("asset") is not None]

    if crypto_sites:
        # Get unique asset types from the sites
        asset_types = list(set(s.get("asset", "general") for s in crypto_sites))
        asset_types = sorted(asset_types)

        # Asset display names
        asset_labels = {
            "all": "All Assets",
            "bitcoin": "Bitcoin",
            "ethereum": "Ethereum",
            "both": "BTC & ETH",
            "general": "General Crypto"
        }

        # Create two columns for dropdowns
        col1, col2 = st.columns(2)

        with col1:
            # Asset selector
            selected_asset = st.selectbox(
                "Select Asset",
                options=["all"] + asset_types,
                format_func=lambda x: asset_labels.get(x, x.title()),
                key="crypto_asset_select"
            )

        # Filter sites based on selected asset
        if selected_asset == "all":
            filtered_sites = crypto_sites
        else:
            filtered_sites = [s for s in crypto_sites if s.get("asset") == selected_asset]

        # Sort filtered sites alphabetically
        filtered_sites = sorted(filtered_sites, key=lambda x: x.get('name', '').lower())

        with col2:
            # Source selector (based on filtered sites)
            if filtered_sites:
                source_options = {s["id"]: s["name"].replace(' (TOTAL3)', '').replace('TOTAL3', '') for s in filtered_sites}
                selected_source_id = st.selectbox(
                    "Select Source",
                    options=list(source_options.keys()),
                    format_func=lambda x: source_options.get(x, x),
                    key="crypto_source_select"
                )
            else:
                selected_source_id = None
                st.info("No sources available for selected asset.")

        # Display selected source details and scrape button
        if selected_source_id:
            selected_site = next((s for s in filtered_sites if s["id"] == selected_source_id), None)

            if selected_site:
                st.markdown("---")

                # Source info card
                col_info, col_action = st.columns([3, 1])

                with col_info:
                    display_name = selected_site['name'].replace(' (TOTAL3)', '').replace('TOTAL3', '')
                    st.markdown(f"### {display_name}")

                    # Description
                    description = selected_site.get('metadata', {}).get('notes', 'No description')
                    if description:
                        description = description.split('\n')[0].strip()
                    st.caption(description)

                    # Asset badge and link
                    asset_badge = asset_labels.get(selected_site.get("asset", "general"), "General")
                    st.markdown(f"**Asset:** {asset_badge} · [View Source →]({selected_site['page_url']})")

                with col_action:
                    st.write("")  # Spacing
                    scrape_clicked = st.button("Scrape Data", key="crypto_scrape_btn", type="primary", use_container_width=True)

                # Handle scraping
                if scrape_clicked:
                    with st.spinner(f"Scraping {selected_site['name']}... This may take a moment."):
                        progress_bar = st.progress(0)
                        status_text = st.empty()

                        status_text.text("Loading site configuration...")
                        progress_bar.progress(33)

                        result = api.scrape_configured_site(
                            site_id=selected_source_id,
                            use_stealth=True,
                            override_robots=False,
                        )

                        progress_bar.progress(100)
                        status_text.text("Complete!")

                        # Store result in session state
                        st.session_state['crypto_result'] = result
                        st.session_state['crypto_result_site'] = selected_site

                # Display results if available
                if 'crypto_result' in st.session_state and st.session_state.get('crypto_result_site', {}).get('id') == selected_source_id:
                    result = st.session_state['crypto_result']
                    site = st.session_state['crypto_result_site']

                    st.markdown("---")

                    if result["success"]:
                        st.success(f"Successfully extracted {result['rows']} rows of data!")

                        # Show warnings if any
                        if result["warnings"]:
                            with st.expander("Validation Warnings", expanded=False):
                                for warning in result["warnings"]:
                                    st.warning(warning)

                        # Data preview (latest first)
                        st.subheader("Data Preview")
                        preview_rows = st.slider("Rows to display:", 10, min(100, result["rows"]), 50, key="crypto_preview_slider")

                        # Sort by date descending if date column exists (latest first)
                        preview_data = result["data"].copy()
                        date_cols = [c for c in preview_data.columns if any(d in c.lower() for d in ['date', 'time', 'timestamp', 'datetime'])]
                        if date_cols:
                            try:
                                preview_data[date_cols[0]] = pd.to_datetime(preview_data[date_cols[0]], errors='coerce')
                                preview_data = preview_data.sort_values(date_cols[0], ascending=False, na_position='last')
                            except:
                                preview_data = preview_data.sort_values(date_cols[0], ascending=False, na_position='last')

                        # Format large numbers as millions
                        display_data = format_dataframe_for_display(preview_data.head(preview_rows))
                        st.dataframe(display_data, use_container_width=True)

                        # Download section
                        st.subheader("Download")
                        excel_bytes, filename = api.export_to_excel(result["data"], filename=f"{selected_source_id}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}")

                        if excel_bytes:
                            st.download_button(
                                label="Download Excel File",
                                data=excel_bytes,
                                file_name=filename,
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                type="primary",
                                key="crypto_download_btn"
                            )
                            st.info(f"File size: {len(excel_bytes) / 1024:.2f} KB")
                        else:
                            st.error("Failed to generate Excel file")
                    else:
                        st.error(f"Scraping failed: {result['error']}")
    else:
        st.info("No crypto data sources configured. Please add crypto sites to websites.yaml.")

with tab2:
    st.header("Market Sentiment Indicators")
    st.markdown("17 indicators from FRED, University of Michigan, and DG ECFIN")

    # Indicator metadata for card display
    INDICATORS = [
        # FRED
        {"id": "fred_consumer_confidence", "short_name": "Consumer Confidence",
         "description": "OECD standardized consumer confidence index", "source": "FRED"},
        {"id": "fred_10y_breakeven_inflation", "short_name": "10Y Breakeven Inflation",
         "description": "10-year breakeven inflation rate", "source": "FRED"},
        {"id": "fred_consumer_sentiment", "short_name": "Consumer Sentiment",
         "description": "University of Michigan Consumer Sentiment Index", "source": "FRED"},
        {"id": "fred_5y5y_forward_inflation", "short_name": "5Y Forward Inflation",
         "description": "5-year, 5-year forward inflation expectation", "source": "FRED"},
        {"id": "fred_oecd_amplitude_adjusted", "short_name": "OECD Amplitude Adjusted",
         "description": "OECD amplitude adjusted consumer confidence", "source": "FRED"},
        {"id": "fred_cleveland_1yr_inflation", "short_name": "1Y Inflation Expectations",
         "description": "Cleveland Fed 1-year inflation expectations", "source": "FRED"},
        {"id": "fred_cleveland_10yr_inflation", "short_name": "10Y Inflation Expectations",
         "description": "Cleveland Fed 10-year inflation expectations", "source": "FRED"},

        # UMich - special handling (single source, multiple fields)
        {"id": "umich_consumer_surveys", "field": "sentiment", "short_name": "Consumer Sentiment",
         "description": "Index of Consumer Sentiment (ICS_ALL)", "source": "UMich"},
        {"id": "umich_consumer_surveys", "field": "current_conditions", "short_name": "Current Conditions",
         "description": "Current Economic Conditions (ICC)", "source": "UMich"},
        {"id": "umich_consumer_surveys", "field": "consumer_expectations", "short_name": "Consumer Expectations",
         "description": "Consumer Expectations (ICE)", "source": "UMich"},
        {"id": "umich_consumer_surveys", "field": "year_ahead_inflation", "short_name": "Year-Ahead Inflation",
         "description": "Year Ahead Inflation (PX_MD)", "source": "UMich"},
        {"id": "umich_consumer_surveys", "field": "long_run_inflation", "short_name": "Long-Run Inflation",
         "description": "Long Run Inflation (PX5_MD)", "source": "UMich"},

        # DG ECFIN - special handling (single source, multiple fields)
        {"id": "dg_ecfin_surveys", "field": "esi_eu", "short_name": "Economic Sentiment (EU)",
         "description": "Economic Sentiment Indicator - EU", "source": "DG ECFIN"},
        {"id": "dg_ecfin_surveys", "field": "esi_ea", "short_name": "Economic Sentiment (EA)",
         "description": "Economic Sentiment Indicator - Euro Area", "source": "DG ECFIN"},
        {"id": "dg_ecfin_surveys", "field": "eei_eu", "short_name": "Employment Expectations (EU)",
         "description": "Employment Expectations Indicator - EU", "source": "DG ECFIN"},
        {"id": "dg_ecfin_surveys", "field": "eei_ea", "short_name": "Employment Expectations (EA)",
         "description": "Employment Expectations Indicator - Euro Area", "source": "DG ECFIN"},
        {"id": "dg_ecfin_surveys", "field": "flash_consumer_confidence_ea", "short_name": "Flash Consumer Confidence",
         "description": "Flash Consumer Confidence - Euro Area", "source": "DG ECFIN"},
    ]

    # Initialize session state for storing fetched data
    if 'indicator_data' not in st.session_state:
        st.session_state.indicator_data = {}
    if 'fetching' not in st.session_state:
        st.session_state.fetching = set()

    def render_indicator_card(indicator):
        """Render a single indicator card"""
        # Unique key for this indicator
        card_key = f"{indicator['id']}_{indicator.get('field', 'main')}"

        with st.container(border=True):
            # Header (no icon)
            st.markdown(f"### {indicator['short_name']}")
            st.caption(indicator['description'])
            
            # Add source link
            site_config = next((s for s in sites if s.get('id') == indicator['id']), None)
            if site_config and site_config.get('page_url'):
                st.markdown(f"[View Source →]({site_config['page_url']})")

            # Status display
            data = st.session_state.indicator_data.get(card_key)
            is_fetching = card_key in st.session_state.fetching

            if data:
                latest_value = data.get('latest_value')
                latest_date = data.get('latest_date')
                if latest_value and latest_date:
                    st.success(f"✓ Latest: **{latest_value:.2f}** ({latest_date.strftime('%b %Y')})")

                # Expandable section for chart and data
                with st.expander("View Chart & Data", expanded=False):
                    # Chart with constrained zoom
                    if 'chart_data' in data:
                        chart_data = data['chart_data']

                        # Create Plotly figure
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(
                            x=chart_data.index,
                            y=chart_data.values,
                            mode='lines',
                            line=dict(color='#1f77b4', width=2),
                            name=indicator['short_name']
                        ))

                        # Set axis ranges to prevent zooming beyond FULL dataset
                        if 'full_date_range' in data:
                            min_date, max_date = data['full_date_range']
                        else:
                            # Fallback to chart data range
                            min_date = chart_data.index.min()
                            max_date = chart_data.index.max()

                        fig.update_xaxes(
                            range=[min_date, max_date],
                            rangemode='normal',  # Prevents zooming beyond range
                            fixedrange=False  # Allow zooming within range
                        )

                        fig.update_layout(
                            height=250,
                            margin=dict(l=0, r=0, t=0, b=0),
                            showlegend=False,
                            hovermode='x unified'
                        )

                        st.plotly_chart(fig, width='stretch')

                    # Data table - show ALL data (latest first)
                    if 'data' in data:
                        df_full = data['data']

                        # Prepare display dataframe (sorted latest first)
                        if 'value' in df_full.columns:
                            # FRED format
                            display_df = df_full[['date', 'value']].copy()
                            display_df['date'] = pd.to_datetime(display_df['date'])
                            display_df = display_df.sort_values('date', ascending=False)
                            display_df['date'] = display_df['date'].dt.strftime('%Y-%m-%d')
                            # Format large numbers as millions
                            display_df = format_dataframe_for_display(display_df)
                            st.markdown(f"**Complete Data ({len(display_df)} rows)**")
                        else:
                            # UMich/DG ECFIN format
                            field_name = indicator.get('field')
                            if field_name and field_name in df_full.columns:
                                display_df = df_full[['date', field_name]].copy()
                                display_df['date'] = pd.to_datetime(display_df['date'])
                                display_df = display_df.sort_values('date', ascending=False)
                                display_df['date'] = display_df['date'].dt.strftime('%Y-%m-%d')
                                # Format large numbers as millions
                                display_df = format_dataframe_for_display(display_df)
                                st.markdown(f"**Complete Data ({len(display_df)} rows)**")
                            else:
                                display_df = df_full.copy()
                                date_cols = [c for c in display_df.columns if 'date' in c.lower()]
                                if date_cols:
                                    display_df = display_df.sort_values(date_cols[0], ascending=False)
                                display_df = format_dataframe_for_display(display_df)
                                st.markdown(f"**Complete Data ({len(display_df)} rows)**")

                        st.dataframe(display_df, width='stretch', hide_index=True, height=400)
            elif is_fetching:
                st.info("⏳ Fetching data...")
            else:
                st.caption("🔵 Not fetched")

    def fetch_indicator(indicator, card_key):
        """Fetch data for a single indicator"""
        st.session_state.fetching.add(card_key)

        try:
            # Fetch the source data
            site_id = indicator['id']

            result = api.scrape_configured_site(
                site_id=site_id,
                use_stealth=False,
                override_robots=False,
            )

            if result["success"] and result["data"] is not None and not result["data"].empty:
                df = result["data"]

                # Handle different data formats
                if "value" in df.columns:
                    # FRED format: single series (newest data first)
                    latest_row = df.iloc[0] if len(df) > 0 else None
                    if latest_row is not None:
                        # Get ALL data and reverse to chronological order for chart
                        chart_df = df.copy()
                        chart_df = chart_df.iloc[::-1]  # Reverse to oldest->newest

                        # Store full date range from entire dataset
                        full_dates = pd.to_datetime(df['date'])

                        st.session_state.indicator_data[card_key] = {
                            "data": df,
                            "latest_value": latest_row["value"],
                            "latest_date": pd.to_datetime(latest_row["date"]),
                            "chart_data": chart_df.set_index('date')['value'],
                            "full_date_range": (full_dates.min(), full_dates.max())
                        }
                else:
                    # UMich/DG ECFIN format: multiple fields
                    field_name = indicator.get('field')
                    if field_name and field_name in df.columns:
                        latest_row = df.iloc[-1] if len(df) > 0 else None
                        if latest_row is not None:
                            # Store full date range from entire dataset
                            full_dates = pd.to_datetime(df['date'])

                            st.session_state.indicator_data[card_key] = {
                                "data": df,
                                "latest_value": latest_row[field_name],
                                "latest_date": pd.to_datetime(latest_row["date"]),
                                "chart_data": df.set_index('date')[field_name],  # ALL data, not just tail(24)
                                "full_date_range": (full_dates.min(), full_dates.max())
                            }
        except Exception as e:
            st.error(f"Error fetching {indicator['short_name']}: {str(e)[:100]}")
        finally:
            st.session_state.fetching.discard(card_key)

        st.rerun()

    # Get all sentiment sites (still needed for validation)
    sentiment_sites = [s for s in sites if
                       s.get("id", "").startswith(("fred_", "umich_", "dg_ecfin_"))]

    if sentiment_sites:
        st.caption(f"17 indicators from FRED, University of Michigan, and DG ECFIN")

        # Fetch All button
        col1, col2, col3 = st.columns([2, 2, 6])
        with col1:
            if st.button("Fetch All Indicators", type="primary", use_container_width=True):
                # Mark all indicators as fetching
                for indicator in INDICATORS:
                    card_key = f"{indicator['id']}_{indicator.get('field', 'main')}"
                    st.session_state.fetching.add(card_key)
                st.rerun()

        with col2:
            # Show progress if any indicators are being fetched
            fetching_count = len(st.session_state.fetching)
            if fetching_count > 0:
                st.info(f"⏳ Fetching {fetching_count} indicator(s)...")

        # Fetch indicators that are marked for fetching (one at a time per rerun)
        if st.session_state.fetching:
            # Get the first indicator to fetch
            for indicator in INDICATORS:
                card_key = f"{indicator['id']}_{indicator.get('field', 'main')}"
                if card_key in st.session_state.fetching:
                    # Only fetch if not already fetched
                    if card_key not in st.session_state.indicator_data:
                        fetch_indicator(indicator, card_key)
                        break  # Fetch one at a time, rerun will continue with next
                    else:
                        # Already fetched, just remove from fetching set
                        st.session_state.fetching.discard(card_key)

        st.divider()

        # Group indicators by source
        fred_indicators = [i for i in INDICATORS if i['source'] == 'FRED']
        umich_indicators = [i for i in INDICATORS if i['source'] == 'UMich']
        dg_ecfin_indicators = [i for i in INDICATORS if i['source'] == 'DG ECFIN']

        # FRED Section
        st.markdown("### FRED Market Sentiment (7 indicators)")
        for indicator in fred_indicators:
            render_indicator_card(indicator)

        st.divider()

        # UMich Section
        st.markdown("### University of Michigan Consumer Surveys (5 fields)")
        for indicator in umich_indicators:
            render_indicator_card(indicator)

        st.divider()

        # DG ECFIN Section
        st.markdown("### DG ECFIN EU Surveys (5 indicators)")
        for indicator in dg_ecfin_indicators:
            render_indicator_card(indicator)
    else:
        st.info("No market sentiment indicators configured. Please add them to websites.yaml.")

with tab3:
    st.subheader("Dental ETFs Data Sources")
    st.markdown("""
    This tab provides access to dental-themed ETF and stock data from multiple sources.
    Data is scraped in real-time when you click the fetch buttons.
    """)
    
    # Get dental ETF sites from configuration (exclude Yahoo Finance, FinanceCharts, and SIC 3843 from UI)
    dental_sites = [s for s in sites if s.get("id", "").startswith("dental_") 
                    and s.get("id") != "dental_yahoo_etf_holdings"
                    and s.get("id") != "dental_financecharts_performance"
                    and s.get("id") != "dental_fintel_sic_3843"]
    # Sort alphabetically by name
    dental_sites = sorted(dental_sites, key=lambda x: x.get('name', '').lower())
    
    # Initialize session state for dental tab
    if "dental_results" not in st.session_state:
        st.session_state.dental_results = {}
    
    # Data Source Descriptions
    DENTAL_SOURCE_INFO = {
        "dental_swingtradebot_etf_list": {
            "description": "Lists all ETFs with dental theme exposure and their weighting",
            "data_points": "Symbol, Name, Grade, % Change, Weighting, Holdings"
        },
        "dental_portfoliopilot_risk_return": {
            "description": "Risk/return metrics for dental stocks",
            "data_points": "Ticker, Expected Return, Sharpe, Beta, Vol, P/E, Div Yield"
        }
    }
    
    if dental_sites:
        # Display data sources in 2 columns
        st.subheader("Available Data Sources")
        cols = st.columns(2)
        
        for idx, site in enumerate(dental_sites):
            site_id = site["id"]
            info = DENTAL_SOURCE_INFO.get(site_id, {"description": "Dental ETF data", "data_points": "Various"})
            
            with cols[idx % 2]:
                with st.container():
                    st.markdown(f"### {site['name']}")
                    st.caption(info['description'])
                    st.markdown(f"**Data points:** {info['data_points']}")
                    # Fix: Use the actual page_url from config, not a hardcoded link
                    page_url = site.get('page_url', '#')
                    if page_url and page_url != '#':
                        st.markdown(f"[View Source →]({page_url})")
                    st.markdown("---")
        
        # Fetch Data Section
        st.subheader("Fetch Dental ETF Data")
        
        # Top row: Two buttons side by side
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("Fetch Risk/Return Metrics", key="fetch_portfoliopilot", use_container_width=True):
                site = next((s for s in dental_sites if s["id"] == "dental_portfoliopilot_risk_return"), None)
                if site:
                    with st.spinner(f"Fetching {site['name']}..."):
                        try:
                            result = api.scrape_configured_site(
                                site_id=site["id"],
                                use_stealth=True,
                                override_robots=False,
                            )
                            st.session_state.dental_results[site["id"]] = (result, site)
                        except Exception as e:
                            st.session_state.dental_results[site["id"]] = (
                                {"success": False, "error": str(e), "data": None, "rows": 0},
                                site
                            )
        
        with col2:
            if st.button("Fetch SwingTradeBot ETF List", key="fetch_swingtradebot", use_container_width=True):
                site = next((s for s in dental_sites if s["id"] == "dental_swingtradebot_etf_list"), None)
                if site:
                    with st.spinner(f"Fetching {site['name']}..."):
                        try:
                            result = api.scrape_configured_site(
                                site_id=site["id"],
                                use_stealth=True,
                                override_robots=False,
                            )
                            st.session_state.dental_results[site["id"]] = (result, site)
                        except Exception as e:
                            st.session_state.dental_results[site["id"]] = (
                                {"success": False, "error": str(e), "data": None, "rows": 0},
                                site
                            )
        
        # Bottom: Big "Fetch All Sources" button (full width)
        if st.button("Fetch All Sources", type="primary", key="fetch_all_dental", use_container_width=True):
            # Filter out SIC 3843 from fetch all since it doesn't work
            working_sites = [s for s in dental_sites if s["id"] != "dental_fintel_sic_3843"]
            
            with st.spinner("Fetching all dental ETF data..."):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                for idx, site in enumerate(working_sites):
                    site_id = site["id"]
                    progress = (idx + 1) / len(working_sites)
                    progress_bar.progress(progress)
                    status_text.text(f"Fetching {site['name']}... ({idx + 1}/{len(working_sites)})")
                    
                    try:
                        # Standard scrape
                        result = api.scrape_configured_site(
                            site_id=site_id,
                            use_stealth=True,
                            override_robots=False,
                        )
                        
                        st.session_state.dental_results[site_id] = (result, site)
                    except Exception as e:
                        st.session_state.dental_results[site_id] = (
                            {"success": False, "error": str(e), "data": None, "rows": 0},
                            site
                        )
                
                progress_bar.progress(100)
                status_text.text("Complete!")
        
        # Display Results Section
        if st.session_state.dental_results:
            st.subheader("Results")
            
            # Summary metrics
            successful = sum(1 for r, _ in st.session_state.dental_results.values() if r.get("success"))
            total = len(st.session_state.dental_results)
            st.info(f"Successfully fetched: {successful}/{total} data sources")
            
            # Display each result
            for site_id, (result, site) in st.session_state.dental_results.items():
                info = DENTAL_SOURCE_INFO.get(site_id, {})
                
                with st.expander(f"{site['name']}", expanded=result.get("success", False)):
                    if result.get("success") and result.get("data") is not None and not result["data"].empty:
                        st.success(f"Extracted {result['rows']} rows")
                        
                        # Data preview (latest first)
                        preview_rows = st.slider(
                            "Rows to display:",
                            5, min(50, result["rows"]), 10,
                            key=f"dental_preview_{site_id}"
                        )
                        # Sort by date descending if date column exists (latest first)
                        preview_data = result["data"].copy()
                        date_cols = [c for c in preview_data.columns if 'date' in c.lower()]
                        if date_cols:
                            preview_data = preview_data.sort_values(date_cols[0], ascending=False)
                        # Format large numbers as millions
                        display_data = format_dataframe_for_display(preview_data.head(preview_rows))
                        st.dataframe(display_data, width='stretch')
                        
                        # Download button
                        excel_bytes, filename = api.export_to_excel(
                            result["data"],
                            filename=f"dental_{site_id}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}"
                        )
                        
                        if excel_bytes:
                            st.download_button(
                                label=f"Download {site['name']} Excel",
                                data=excel_bytes,
                                file_name=filename,
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=f"dental_download_{site_id}"
                            )
                    else:
                        st.error(f"Failed: {result.get('error', 'Unknown error')}")
            
            # Combined Export Option
            st.subheader("Export All Data")
            
            # Gather all successful dataframes
            all_dfs = {}
            for site_id, (result, site) in st.session_state.dental_results.items():
                if result.get("success") and result.get("data") is not None and not result["data"].empty:
                    # Use short name for sheet
                    sheet_name = site_id.replace("dental_", "")[:31]  # Excel sheet name limit
                    all_dfs[sheet_name] = result["data"]
            
            if all_dfs:
                if st.button("Export All to Single Excel (Multiple Sheets)", key="export_all_dental"):
                    try:
                        # Combine into multi-sheet Excel
                        excel_bytes, filename = api.export_dental_to_excel(
                            all_dfs,
                            filename=f"dental_etf_all_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}"
                        )
                        if excel_bytes:
                            st.success("Combined Excel file generated!")
                            st.download_button(
                                label="Download Combined Excel",
                                data=excel_bytes,
                                file_name=filename,
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_combined_dental"
                            )
                        else:
                            st.error("Failed to generate combined Excel file")
                    except Exception as e:
                        st.error(f"Export failed: {str(e)}")
    else:
        st.info("No dental ETF data sources configured. Please add them to websites.yaml with IDs starting with 'dental_'.")

with tab4:
    st.header("📰 Fintech News")
    st.markdown("Stay updated with the latest financial technology news and market insights.")
    
    if not news_sources:
        st.warning("No news sources configured. Please check config/news_sources.yaml")
    else:
        # Multi-select for sources
        source_names = [s.get('name', 'Unknown') for s in news_sources]
        selected_sources = st.multiselect(
            "Select news sources:",
            source_names,
            default=source_names[:4],  # Default to first 4
            key="news_sources_select"
        )
        
        # Display selected providers as hyperlinks on a single line
        if selected_sources:
            provider_links = []
            for source in news_sources:
                if source.get('name') in selected_sources:
                    homepage = source.get('homepage_url', '#')
                    name = source.get('name', 'Unknown')
                    if homepage != '#':
                        provider_links.append(f"[{name}]({homepage})")
                    else:
                        provider_links.append(name)
            
            if provider_links:
                st.markdown("**Selected Providers:** " + " | ".join(provider_links))
                st.markdown("---")
        
        # Fetch headlines button
        if st.button("🔄 Refresh Headlines", key="refresh_headlines"):
            # Clear cache to force refresh
            st.cache_data.clear()
            st.rerun()
        
        # Fetch and display 5 headlines
        if selected_sources:
            with st.spinner("Fetching latest headlines..."):
                # Prepare feeds list
                feeds_to_fetch = [
                    {
                        'rss_url': s.get('rss_url'),
                        'source_name': s.get('name')
                    }
                    for s in news_sources
                    if s.get('name') in selected_sources and s.get('rss_url')
                ]
                
                # Fetch headlines (cached)
                # Convert to hashable format for caching
                feeds_key = tuple(sorted([(f['rss_url'], f['source_name']) for f in feeds_to_fetch]))
                
                @st.cache_data(ttl=300)  # Cache for 5 minutes
                def fetch_headlines_cached(feeds_key_tuple):
                    # Convert back to list of dicts
                    feeds_list = [{'rss_url': url, 'source_name': name} for url, name in feeds_key_tuple]
                    return rss_client.fetch_multiple_feeds(feeds_list, max_headlines=5)
                
                headlines = fetch_headlines_cached(feeds_key)
                
                if headlines:
                    st.subheader("Latest Headlines")
                    for idx, headline in enumerate(headlines):
                        with st.container():
                            st.markdown(f"**{headline.title}**")
                            st.caption(f"📰 {headline.source_name}")
                            if headline.published_at:
                                st.caption(f"🕒 {headline.published_at.strftime('%Y-%m-%d %H:%M')}")
                            st.markdown(f"[Read more →]({headline.link})")
                            if idx < len(headlines) - 1:
                                st.markdown("---")
                else:
                    st.info("No headlines found. Please check your internet connection and RSS feed URLs.")
        else:
            st.info("Select news sources above to see headlines.")

with tab5:
    st.header("🔧 Dev")
    st.markdown("""
    This tab is for testing new features before they are added to the production website.
    
    Use this space to experiment with new functionality, UI changes, and data sources.
    """)
    st.info("This is a development/testing environment. Features here are experimental and may change.")


