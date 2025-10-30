#!/bin/bash
# Mac double-click script to start Streamlit dashboard

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Change to that directory
cd "$SCRIPT_DIR"

echo "Starting African Infrastructure Dashboard..."
echo "Directory: $SCRIPT_DIR"
echo ""

# Run Streamlit
streamlit run app.py

# Keep terminal open after script ends
echo ""
echo "Press any key to close..."
read -n 1
