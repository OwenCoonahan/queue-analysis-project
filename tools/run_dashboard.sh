#!/bin/bash
# Launch the Market Intelligence Dashboard
# Usage: ./run_dashboard.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "Virtual environment not found. Creating..."
    /opt/homebrew/bin/python3.12 -m venv .venv
    .venv/bin/pip install -r requirements.txt
fi

# Activate and run
echo "Starting Market Intelligence Dashboard..."
echo "Open http://localhost:8501 in your browser"
echo ""
.venv/bin/streamlit run intel_dashboard.py --server.headless true
