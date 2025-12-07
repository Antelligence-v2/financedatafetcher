#!/bin/bash
# Setup script for Streamlit Cloud deployment
# This script installs Playwright browsers
# 
# For Streamlit Cloud: Add this as a post-install command in Advanced Settings:
#   bash setup_streamlit_cloud.sh
#
# Or run manually: playwright install chromium

echo "Installing Playwright browsers for Streamlit Cloud..."
python -m playwright install chromium

echo "Playwright setup complete!"
echo "Chromium browser is now available for web scraping."

