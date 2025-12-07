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

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

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
    try:
        from playwright.sync_api import sync_playwright
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                browser.close()
        except Exception as e:
            # Try to auto-install browsers if missing (for Streamlit Cloud)
            try:
                import subprocess
                import sys
                # Install Chromium browser
                result = subprocess.run(
                    [sys.executable, "-m", "playwright", "install", "chromium"],
                    capture_output=True,
                    text=True,
                    timeout=300  # 5 minute timeout
                )
                if result.returncode == 0:
                    # Retry browser launch after installation
                    try:
                        with sync_playwright() as p:
                            browser = p.chromium.launch(headless=True)
                            browser.close()
                    except Exception:
                        warnings.append("⚠️ Playwright browsers installed but still not working. Browser automation may be disabled.")
                else:
                    warnings.append(f"⚠️ Failed to auto-install Playwright browsers: {result.stderr}")
            except Exception as install_error:
                warnings.append(f"⚠️ Playwright browsers may not be installed. Auto-install failed: {str(install_error)}")
    except ImportError:
        warnings.append("⚠️ Playwright not installed. Browser automation will not work.")
    except Exception as e:
        # Don't block app startup if Playwright check fails
        warnings.append(f"⚠️ Playwright check failed: {str(e)}")
    
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
    else:
        selected_site = "None"
        st.info("No sites configured. Use URL input instead.")

# Main content
tab1, tab2 = st.tabs(["Scrape from URL", "Scrape from Configured Site"])

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

# Footer
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: #666666;'>"
    "Data-Fetch Framework | Financial Data Scraper"
    "</div>",
    unsafe_allow_html=True
)

