#!/bin/bash
# Setup script for Playwright browsers
# This script installs Chromium browser for Playwright
# 
# For Streamlit Cloud: Add this as a post-install command in Advanced Settings:
#   playwright install chromium
#
# For local development: Run this script after installing requirements

echo "Installing Playwright browsers..."
playwright install chromium

echo "Playwright setup complete!"
echo "Chromium browser is now available for web scraping."


