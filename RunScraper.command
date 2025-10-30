#!/bin/bash
# Mac double-click script to run the scraper

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Change to that directory
cd "$SCRIPT_DIR"

echo "==============================================="
echo "African Infrastructure News Scraper"
echo "==============================================="
echo "Directory: $SCRIPT_DIR"
echo ""
echo "This will scrape news from 44+ sources across 8 countries."
echo "Estimated time: 5-15 minutes"
echo ""

# Run the master scraper
python3 master_scraper.py

# Show completion status
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Scraping completed successfully!"
    echo "Data saved to: combined_data.csv"
else
    echo ""
    echo "⚠️  Scraping completed with some errors."
    echo "Check the output above for details."
fi

echo ""
echo "Press any key to close..."
read -n 1
