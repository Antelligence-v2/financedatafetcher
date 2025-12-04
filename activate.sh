#!/bin/bash
# Quick activation script for the virtual environment

echo "Activating Python virtual environment..."
source venv/bin/activate

echo "Virtual environment activated!"
echo ""
echo "To deactivate, run: deactivate"
echo ""
echo "To start the Docker services, run:"
echo "  docker-compose up -d --build"
echo ""
echo "To view logs, run:"
echo "  docker-compose logs -f"











