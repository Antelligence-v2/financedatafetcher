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

# Startup checks
def check_environment():
    """Check environment setup and display warnings if needed."""
    warnings = []
    
    # Check OpenAI API key
    openai_key = os.getenv("OPENAI_API_KEY")
    if not openai_key:
        # Try Streamlit secrets (only available in Streamlit Cloud)
        try:
            openai_key = st.secrets.get("OPENAI_API_KEY", None)
        except (AttributeError, FileNotFoundError):
            # Secrets not available (local development without secrets.toml)
            pass
    
    if not openai_key:
        warnings.append("⚠️ OpenAI API key not found. LLM-powered data detection will be disabled.")
    
    # Check Alpha Vantage API key
    alphavantage_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    if not alphavantage_key:
        # Try Streamlit secrets (only available in Streamlit Cloud)
        try:
            alphavantage_key = st.secrets.get("ALPHA_VANTAGE_API_KEY", None)
        except (AttributeError, FileNotFoundError):
            # Secrets not available (local development without secrets.toml)
            pass
    
    if not alphavantage_key:
        warnings.append("⚠️ Alpha Vantage API key not found. Alpha Vantage data sources will be disabled.")

    # Check FRED API key
    fred_key = os.getenv("FRED_API_KEY")
    if not fred_key:
        # Try Streamlit secrets (only available in Streamlit Cloud)
        try:
            fred_key = st.secrets.get("FRED_API_KEY", None)
        except (AttributeError, FileNotFoundError):
            # Secrets not available (local development without secrets.toml)
            pass

    if not fred_key:
        warnings.append("⚠️ FRED API key not found. Market sentiment indicators will be disabled.")

    # Check Playwright browsers (non-blocking check)
    # Note: On Streamlit Cloud, browsers are installed automatically via post-install
    try:
        from playwright.sync_api import sync_playwright
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                browser.close()
        except Exception as e:
            error_msg = str(e).lower()
            # Check if it's a missing browser error
            if any(keyword in error_msg for keyword in ["executable", "browser", "not found", "no such file"]):
                warnings.append("⚠️ Playwright browsers not installed. Browser automation will be disabled. The app will attempt to install browsers automatically when needed.")
            else:
                warnings.append(f"⚠️ Playwright browser check failed: {str(e)[:100]}")
    except ImportError:
        warnings.append("⚠️ Playwright not installed. Browser automation will not work.")
    except Exception as e:
        # Don't block app startup if Playwright check fails
        warnings.append(f"⚠️ Playwright check failed: {str(e)[:100]}")
    
    return warnings

# Run startup checks (only once, cached)
@st.cache_resource
def get_startup_warnings():
    return check_environment()

startup_warnings = get_startup_warnings()

# Page configuration
st.set_page_config(
    page_title="Data-Fetch: Financial Data Scraper",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

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

# Header
st.title("📊 Data-Fetch: Financial Data Scraper")
st.markdown("---")
st.markdown("Extract financial data from websites and download as Excel files.")

# Display startup warnings if any
if startup_warnings:
    with st.expander("⚠️ Environment Warnings", expanded=True):
        for warning in startup_warnings:
            st.warning(warning)

# Sidebar
with st.sidebar:
    st.header("Configuration")
    
    # Scraping options
    use_stealth = st.checkbox("Enable Stealth Mode", value=True, help="Bypass basic bot detection")
    override_robots = st.checkbox("Override robots.txt", value=False, help="Proceed even if robots.txt is UNKNOWN")
    use_fallbacks = st.checkbox("Use Fallback Sources", value=True, help="Try alternative data sources if primary fails")
    
    st.markdown("---")
    
    # Configured sites
    st.subheader("Configured Sites")
    sites = api.get_configured_sites()
    if sites:
        site_options = ["None"] + [f"{s['id']} - {s['name']}" for s in sites]
        selected_site = st.selectbox("Select a configured site:", site_options)
        
        # Show site info if selected
        if selected_site != "None":
            site_id = selected_site.split(" - ")[0]
            site_info = next((s for s in sites if s["id"] == site_id), None)
            if site_info:
                with st.expander("Site Information", expanded=False):
                    st.write(f"**URL:** {site_info['page_url']}")
                    st.write(f"**Strategy:** {site_info.get('extraction_strategy', 'N/A')}")
                    st.write(f"**API Key:** {site_info.get('api_key_status', 'N/A')}")
                    if site_info.get('requires_subscription'):
                        st.warning("⚠️ This site requires a paid subscription")
                    st.write(f"**Robots.txt:** {site_info.get('robots_status', 'UNKNOWN')}")
    else:
        selected_site = "None"
        st.info("No sites configured. Use URL input instead.")

# Main content
tab1, tab2, tab3, tab4 = st.tabs([
    "Scrape from URL", 
    "Scrape from Configured Site", 
    "📰 Fintech News",
    "📈 Market Sentiment"  # New tab for FRED market sentiment data
])

with tab1:
    st.header("Scrape from URL")
    
    url_input = st.text_input(
        "Enter website URL:",
        placeholder="https://www.example.com/financial-data",
        help="Enter the URL of the financial data page you want to scrape"
    )
    
    col1, col2 = st.columns([1, 4])
    with col1:
        scrape_button = st.button("Scrape Data", type="primary")
    
    if scrape_button and url_input:
        if not url_input.startswith(("http://", "https://")):
            st.error("Please enter a valid URL starting with http:// or https://")
        else:
            with st.spinner("Scraping data... This may take a moment."):
                # Show progress
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                status_text.text("Step 1/4: Discovering data sources...")
                progress_bar.progress(25)
                
                # Scrape
                result = api.scrape_url(
                    url=url_input,
                    use_stealth=use_stealth,
                    override_robots=override_robots,
                    use_fallbacks=use_fallbacks,
                )
                
                progress_bar.progress(100)
                status_text.text("Complete!")
                
                # Display results
                if result["success"]:
                    st.success(f"✅ Successfully extracted {result['rows']} rows of data!")
                    
                    # Show warnings if any
                    if result["warnings"]:
                        with st.expander("⚠️ Validation Warnings", expanded=False):
                            for warning in result["warnings"]:
                                st.warning(warning)
                    
                    # Data preview
                    st.subheader("Data Preview")
                    preview_rows = st.slider("Rows to display:", 10, min(100, result["rows"]), 50)
                    st.dataframe(result["data"].head(preview_rows), width='stretch')
                    
                    # Data statistics
                    with st.expander("📊 Data Statistics", expanded=False):
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Rows", result["rows"])
                        with col2:
                            st.metric("Total Columns", len(result["columns"]))
                        with col3:
                            if result["metadata"].get("date_range"):
                                date_range = result["metadata"]["date_range"]
                                if date_range[0] and date_range[1]:
                                    st.metric("Date Range", f"{str(date_range[0])[:10]} to {str(date_range[1])[:10]}")
                    
                    # Download section
                    st.subheader("Download")
                    excel_bytes, filename = api.export_to_excel(result["data"])
                    
                    if excel_bytes:
                        st.download_button(
                            label="📥 Download Excel File",
                            data=excel_bytes,
                            file_name=filename,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            type="primary",
                        )
                        st.info(f"File size: {len(excel_bytes) / 1024:.2f} KB")
                    else:
                        st.error("Failed to generate Excel file")
                
                else:
                    st.error(f"❌ Scraping failed: {result['error']}")
                    if result["warnings"]:
                        with st.expander("Warnings", expanded=False):
                            for warning in result["warnings"]:
                                st.warning(warning)

with tab2:
    st.header("Scrape from Configured Site")
    
    if selected_site and selected_site != "None":
        site_id = selected_site.split(" - ")[0]
        site_info = next((s for s in sites if s["id"] == site_id), None)
        
        if site_info:
            st.info(f"**Site:** {site_info['name']}\n\n**URL:** {site_info['page_url']}")
            
            col1, col2 = st.columns([1, 4])
            with col1:
                scrape_site_button = st.button("Scrape This Site", type="primary")
            
            if scrape_site_button:
                with st.spinner("Scraping data... This may take a moment."):
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    status_text.text("Step 1/3: Loading site configuration...")
                    progress_bar.progress(33)
                    
                    result = api.scrape_configured_site(
                        site_id=site_id,
                        use_stealth=use_stealth,
                        override_robots=override_robots,
                    )
                    
                    progress_bar.progress(100)
                    status_text.text("Complete!")
                    
                    if result["success"]:
                        st.success(f"✅ Successfully extracted {result['rows']} rows of data!")
                        
                        # Show warnings if any
                        if result["warnings"]:
                            with st.expander("⚠️ Validation Warnings", expanded=False):
                                for warning in result["warnings"]:
                                    st.warning(warning)
                        
                        # Data preview
                        st.subheader("Data Preview")
                        preview_rows = st.slider("Rows to display:", 10, min(100, result["rows"]), 50, key="site_preview")
                        st.dataframe(result["data"].head(preview_rows), width='stretch')
                        
                        # Download section
                        st.subheader("Download")
                        excel_bytes, filename = api.export_to_excel(result["data"], filename=f"{site_id}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}")
                        
                        if excel_bytes:
                            st.download_button(
                                label="📥 Download Excel File",
                                data=excel_bytes,
                                file_name=filename,
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                type="primary",
                            )
                            st.info(f"File size: {len(excel_bytes) / 1024:.2f} KB")
                        else:
                            st.error("Failed to generate Excel file")
                    
                    else:
                        st.error(f"❌ Scraping failed: {result['error']}")
    else:
        st.info("Select a configured site from the sidebar to scrape data.")

with tab3:
    st.header("📰 Latest Fintech News")
    st.markdown("Stay updated with the latest financial technology news and market insights.")
    
    # News sources configuration
    news_sources = [
        {
            "name": "CoinDesk",
            "url": "https://www.coindesk.com",
            "description": "Cryptocurrency and blockchain news"
        },
        {
            "name": "The Block",
            "url": "https://www.theblock.co",
            "description": "Crypto markets and data"
        },
        {
            "name": "Decrypt",
            "url": "https://decrypt.co",
            "description": "Crypto news and analysis"
        },
        {
            "name": "CoinTelegraph",
            "url": "https://cointelegraph.com",
            "description": "Bitcoin and cryptocurrency news"
        },
    ]
    
    # Display news sources
    st.subheader("Available News Sources")
    cols = st.columns(2)
    
    for idx, source in enumerate(news_sources):
        with cols[idx % 2]:
            with st.container():
                st.markdown(f"### {source['name']}")
                st.caption(source['description'])
                st.markdown(f"[Visit Website →]({source['url']})")
                st.markdown("---")
    
    # News scraping section
    st.subheader("Scrape News Articles")
    st.info("💡 Tip: Use the 'Scrape from URL' tab to extract news articles from these sources.")
    
    # Quick links to scrape news
    st.markdown("### Quick Scrape Links")
    news_url_input = st.text_input(
        "Enter news article URL:",
        placeholder="https://www.coindesk.com/...",
        key="news_url"
    )
    
    if st.button("Scrape News Article", key="scrape_news"):
        if news_url_input:
            if not news_url_input.startswith(("http://", "https://")):
                st.error("Please enter a valid URL starting with http:// or https://")
            else:
                with st.spinner("Scraping news article... This may take a moment."):
                    result = api.scrape_url(
                        url=news_url_input,
                        use_stealth=use_stealth,
                        override_robots=override_robots,
                        use_fallbacks=use_fallbacks,
                    )
                    
                    if result["success"]:
                        st.success(f"✅ Successfully extracted {result['rows']} rows of data!")
                        st.dataframe(result["data"], width='stretch')
                    else:
                        st.error(f"❌ Scraping failed: {result.get('error', 'Unknown error')}")

with tab4:
    st.header("📈 Market Sentiment Indicators")
    st.markdown("17 indicators from FRED, University of Michigan, and DG ECFIN")

    # Check for FRED API key
    fred_api_key = os.getenv("FRED_API_KEY")
    if not fred_api_key:
        st.warning("⚠️ FRED_API_KEY not found in environment variables. Please set it in your .env file.")
        st.info("Get your API key from: https://fred.stlouisfed.org/docs/api/api_key.html")
    else:
        st.success("✅ FRED API key found")

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

            # Status display
            data = st.session_state.indicator_data.get(card_key)
            is_fetching = card_key in st.session_state.fetching

            if data:
                latest_value = data.get('latest_value')
                latest_date = data.get('latest_date')
                if latest_value and latest_date:
                    st.success(f"✓ Latest: **{latest_value:.2f}** ({latest_date.strftime('%b %Y')})")

                # Expandable section for chart and data
                with st.expander("📊 View Chart & Data", expanded=False):
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

                        st.plotly_chart(fig, use_container_width=True)

                    # Data table - show ALL data
                    if 'data' in data:
                        df_full = data['data']

                        # Prepare display dataframe
                        if 'value' in df_full.columns:
                            # FRED format (newest first)
                            display_df = df_full[['date', 'value']].copy()
                            display_df['date'] = pd.to_datetime(display_df['date']).dt.strftime('%Y-%m-%d')
                            st.markdown(f"**Complete Data ({len(display_df)} rows)**")
                        else:
                            # UMich/DG ECFIN format (oldest first)
                            field_name = indicator.get('field')
                            if field_name and field_name in df_full.columns:
                                display_df = df_full[['date', field_name]].copy()
                                display_df['date'] = pd.to_datetime(display_df['date']).dt.strftime('%Y-%m-%d')
                                st.markdown(f"**Complete Data ({len(display_df)} rows)**")
                            else:
                                display_df = df_full.copy()
                                st.markdown(f"**Complete Data ({len(display_df)} rows)**")

                        st.dataframe(display_df, use_container_width=True, hide_index=True, height=400)
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
        st.subheader("📊 Market Sentiment Indicators")
        st.caption(f"17 indicators from FRED, University of Michigan, and DG ECFIN")

        # Fetch All button
        col1, col2, col3 = st.columns([2, 2, 6])
        with col1:
            if st.button("🚀 Fetch All Indicators", type="primary", use_container_width=True):
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
        st.markdown("### 🇺🇸 FRED Market Sentiment (7 indicators)")
        for indicator in fred_indicators:
            render_indicator_card(indicator)

        st.divider()

        # UMich Section
        st.markdown("### 🎓 University of Michigan Consumer Surveys (5 fields)")
        for indicator in umich_indicators:
            render_indicator_card(indicator)

        st.divider()

        # DG ECFIN Section
        st.markdown("### 🇪🇺 DG ECFIN EU Surveys (5 indicators)")
        for indicator in dg_ecfin_indicators:
            render_indicator_card(indicator)
    else:
        st.info("No market sentiment indicators configured. Please add them to websites.yaml.")

# Footer
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: #666666;'>"
    "Data-Fetch Framework | Financial Data Scraper"
    "</div>",
    unsafe_allow_html=True
)

