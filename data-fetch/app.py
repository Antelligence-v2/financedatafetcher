"""
Streamlit frontend for Data-Fetch framework.
Simple monochrome interface for scraping financial data.
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import sys
import os

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
    
    # Check Playwright browsers (non-blocking check)
    # Note: On Streamlit Cloud, browsers should be installed during deployment via post_install.sh
    try:
        from playwright.sync_api import sync_playwright
        
        # First check if browsers appear to be installed
        home = os.path.expanduser("~")
        playwright_paths = [
            os.path.join(home, ".cache", "ms-playwright"),
            os.path.join(home, ".local", "share", "ms-playwright"),
        ]
        browsers_found = False
        for base_path in playwright_paths:
            if os.path.exists(base_path):
                import glob
                chromium_patterns = [
                    os.path.join(base_path, "chromium_headless_shell-*", "chrome-headless-shell-linux64", "chrome-headless-shell"),
                    os.path.join(base_path, "chromium-*", "chrome-linux64", "chrome"),
                ]
                for pattern in chromium_patterns:
                    if glob.glob(pattern):
                        browsers_found = True
                        break
                if browsers_found:
                    break
        
        # Try to launch browser to verify it works
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                browser.close()
            # If we get here, browsers are working
        except Exception as e:
            error_msg = str(e).lower()
            # Classify the error
            if any(keyword in error_msg for keyword in [
                "libnspr4", "libnss3", "libatk", "libcairo", "libpango",
                "shared libraries", "cannot open shared object"
            ]):
                warnings.append(
                    "⚠️ Playwright browser dependencies missing. "
                    "System libraries should be installed via packages.txt during deployment. "
                    "Browser automation may not work until dependencies are installed."
                )
            elif any(keyword in error_msg for keyword in [
                "executable", "browser", "not found", "no such file", "chromium",
                "executable doesn't exist"
            ]):
                if browsers_found:
                    warnings.append(
                        "⚠️ Playwright browsers found but launch failed. "
                        "This may indicate a configuration issue. Browser automation may not work."
                    )
                else:
                    warnings.append(
                        "⚠️ Playwright browsers not installed. "
                        "Browsers should be installed during deployment via post_install.sh. "
                        "Browser automation will be disabled."
                    )
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
    initial_sidebar_state="collapsed",
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

# Display startup warnings if any
if startup_warnings:
    with st.expander("⚠️ Environment Warnings", expanded=True):
        for warning in startup_warnings:
            st.warning(warning)

# Get configured sites (needed for tabs)
sites = api.get_configured_sites()

# Main content
tab1, tab2 = st.tabs([
    "Configured Websites",
    "Market Sentiment"
])

with tab1:
    if sites:
        st.subheader("Available Configured Websites")
        
        # Display sites in columns (card-based layout)
        cols = st.columns(2)
        scrape_results = {}
        
        for idx, site in enumerate(sites):
            with cols[idx % 2]:
                with st.container():
                    st.markdown(f"### {site['name']}")
                    description = site.get('metadata', {}).get('notes', site.get('extraction_strategy', 'No description'))
                    st.caption(description)
                    st.markdown(f"[View Website →]({site['page_url']})")
                    
                    # Scrape button for this site
                    site_id = site["id"]
                    button_key = f"scrape_{site_id}"
                    if st.button("Scrape", key=button_key, type="primary"):
                        # Process scraping immediately
                        with st.spinner(f"Scraping {site['name']}... This may take a moment."):
                            progress_bar = st.progress(0)
                            status_text = st.empty()
                            
                            status_text.text("Step 1/3: Loading site configuration...")
                            progress_bar.progress(33)
                            
                            result = api.scrape_configured_site(
                                site_id=site_id,
                                use_stealth=True,
                                override_robots=False,
                            )
                            
                            progress_bar.progress(100)
                            status_text.text("Complete!")
                            scrape_results[site_id] = (result, site)
                    
                    st.markdown("---")
        
        # Display results for any site that was scraped
        for site_id, (result, site) in scrape_results.items():
            if result["success"]:
                st.success(f"✅ Successfully extracted {result['rows']} rows of data from {site['name']}!")
                
                # Show warnings if any
                if result["warnings"]:
                    with st.expander("⚠️ Validation Warnings", expanded=False):
                        for warning in result["warnings"]:
                            st.warning(warning)
                
                # Data preview
                st.subheader(f"Data Preview - {site['name']}")
                preview_rows = st.slider("Rows to display:", 10, min(100, result["rows"]), 50, key=f"preview_{site_id}")
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
                        key=f"download_{site_id}"
                    )
                    st.info(f"File size: {len(excel_bytes) / 1024:.2f} KB")
                else:
                    st.error("Failed to generate Excel file")
            
            else:
                st.error(f"❌ Scraping {site['name']} failed: {result['error']}")
    else:
        st.info("No configured websites available. Please add sites to websites.yaml.")

with tab2:
    # Check for FRED API key
    fred_api_key = os.getenv("FRED_API_KEY")
    if not fred_api_key:
        st.warning("⚠️ FRED_API_KEY not found in environment variables. Please set it in your .env file.")
        st.info("Get your API key from: https://fred.stlouisfed.org/docs/api/api_key.html")
    else:
        st.success("✅ FRED API key found")
    
    # Get FRED sentiment sites
    fred_sites = [s for s in sites if s.get("id", "").startswith("fred_")]
    
    if fred_sites:
        st.subheader("Available Market Sentiment Indicators")
        
        # Display indicators in columns
        cols = st.columns(2)
        for idx, site in enumerate(fred_sites):
            with cols[idx % 2]:
                with st.container():
                    st.markdown(f"### {site['name']}")
                    st.caption(site.get('metadata', {}).get('notes', 'No description'))
                    st.markdown(f"[View on FRED →]({site['page_url']})")
                    st.markdown("---")
        
        # Bulk scraping option
        st.subheader("Fetch Market Sentiment Data")
        
        col1, col2 = st.columns([1, 4])
        with col1:
            scrape_all_button = st.button("Fetch All Indicators", type="primary")
        
        if scrape_all_button:
            if not fred_api_key:
                st.error("FRED API key is required. Please set FRED_API_KEY in your .env file.")
            else:
                with st.spinner("Fetching market sentiment data from FRED... This may take a moment."):
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    all_data = []
                    latest_values = {}
                    
                    for idx, site in enumerate(fred_sites):
                        site_id = site["id"]
                        progress = (idx + 1) / len(fred_sites)
                        progress_bar.progress(progress)
                        status_text.text(f"Fetching {site['name']}... ({idx + 1}/{len(fred_sites)})")
                        
                        try:
                            result = api.scrape_configured_site(
                                site_id=site_id,
                                use_stealth=False,  # Not needed for API
                                override_robots=False,
                            )
                            
                            if result["success"] and result["data"] is not None and not result["data"].empty:
                                df = result["data"].copy()
                                # Add series name if not present
                                if "series_name" not in df.columns:
                                    df["series_name"] = site["name"]
                                
                                all_data.append(df)
                                
                                # Get latest value
                                if "value" in df.columns and "date" in df.columns:
                                    latest_row = df.iloc[0]  # Already sorted by date desc
                                    latest_values[site["name"]] = {
                                        "value": latest_row.get("value"),
                                        "date": latest_row.get("date"),
                                    }
                        except Exception as e:
                            st.warning(f"Failed to fetch {site['name']}: {str(e)[:100]}")
                    
                    progress_bar.progress(100)
                    status_text.text("Complete!")
                    
                    if all_data:
                        # Combine all data
                        combined_df = pd.concat(all_data, ignore_index=True)
                        
                        st.success(f"✅ Successfully fetched data from {len(all_data)} indicators!")
                        
                        # Display latest values as metrics
                        st.subheader("Latest Values")
                        metric_cols = st.columns(len(latest_values))
                        for idx, (name, data) in enumerate(latest_values.items()):
                            with metric_cols[idx % len(metric_cols)]:
                                value = data.get("value")
                                date = data.get("date")
                                if pd.notna(value):
                                    st.metric(
                                        label=name.split(" - ")[-1] if " - " in name else name,
                                        value=f"{float(value):.2f}" if isinstance(value, (int, float)) else str(value),
                                        delta=None
                                    )
                                    if pd.notna(date):
                                        st.caption(f"Date: {date}")
                        
                        # Display time series chart
                        st.subheader("Time Series Data")
                        if "date" in combined_df.columns and "value" in combined_df.columns:
                            # Pivot for charting
                            chart_df = combined_df.pivot_table(
                                index="date",
                                columns="series_name",
                                values="value",
                                aggfunc="first"
                            )
                            st.line_chart(chart_df)
                        
                        # Data preview
                        st.subheader("Data Preview")
                        preview_rows = st.slider("Rows to display:", 10, min(100, len(combined_df)), 50, key="sentiment_preview")
                        st.dataframe(combined_df.head(preview_rows), width='stretch')
                        
                        # Export option
                        if st.button("Export to Excel", key="export_sentiment"):
                            try:
                                excel_bytes, filename = api.export_to_excel(combined_df)
                                if excel_bytes:
                                    st.success("✅ Excel file generated successfully!")
                                    st.download_button(
                                        label="Download Excel File",
                                        data=excel_bytes,
                                        file_name=filename,
                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                    )
                                else:
                                    st.error("Failed to generate Excel file")
                            except Exception as e:
                                st.error(f"Export failed: {str(e)}")
                    else:
                        st.error("No data was successfully fetched. Please check your API key and try again.")
    else:
        st.info("No FRED market sentiment indicators configured. Please add them to websites.yaml.")



