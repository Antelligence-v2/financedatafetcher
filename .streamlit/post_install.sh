#!/bin/bash
# Post-install script for Streamlit Cloud
# This script runs automatically after package installation
# Install Playwright browsers

echo "Installing Playwright browsers..."
python -m playwright install chromium --with-deps

echo "Playwright setup complete!"

