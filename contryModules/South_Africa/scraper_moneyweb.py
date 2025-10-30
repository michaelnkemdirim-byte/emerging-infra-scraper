#!/usr/bin/env python3
"""
Moneyweb - RSS Feed Scraper
Scrapes financial, infrastructure, energy, and technology news from moneyweb.co.za
"""

import requests
import csv
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import List, Dict, Any
from html import unescape
from bs4 import BeautifulSoup

# Configuration
BASE_URL = "https://www.moneyweb.co.za"
COUNTRY = "South Africa"
SOURCE_NAME = "Moneyweb"

# Date filtering - Last 7 days only
DAYS_BACK = 7
DATE_FILTER = datetime.now() - timedelta(days=DAYS_BACK)

# RSS Feed URLs by category
RSS_FEEDS = {
    'Main Feed': 'https://www.moneyweb.co.za/feed/',
    'News': 'https://www.moneyweb.co.za/category/news/feed/',
    'Economy': 'https://www.moneyweb.co.za/category/economy/feed/',
    'Mineweb': 'https://www.moneyweb.co.za/category/mineweb/feed/'
}

# Comprehensive infrastructure keywords for filtering
SEARCH_KEYWORDS = [
    # Infrastructure - Transportation
    'infrastructure',
    'construction',
    'road',
    'highway',
    'expressway',
    'railway',
    'port',
    'airport',
    'terminal',
    'runway',
    'bridge',
    'industrial zone',
    'free trade zone',
    'logistics hub',
    'container terminal',
    'freight corridor',
    'dry port',

    # Infrastructure - Utilities
    'water supply',
    'sanitation',
    'wastewater',
    'sewage',
    'water treatment',
    'waste management',
    'recycling',

    # Infrastructure - Development
    'smart city',
    'sez',
    'special economic zone',
    'industrial park',

    # Economic
    'investment',
    'finance',
    'trade',
    'export',
    'import',
    'cryptocurrency',
    'crypto',
    'blockchain',
    'fintech',
    'banking',
    'economy',
    'mobile money',
    'remittances',
    'venture capital',
    'private equity',
    'forex',
    'foreign exchange',
    'inflation',
    'gdp',

    # Energy
    'power',
    'electricity',
    'energy',
    'solar',
    'renewable',
    'thermal power',
    'nuclear',
    'wind power',
    'hydroelectric',
    'biofuel',
    'bioenergy',
    'geothermal',
    'battery storage',
    'grid infrastructure',
    'ppa',
    'power purchase agreement',
    'lng',
    'liquefied natural gas',
    'gas-to-power',
    'coal-to-power',
    'eskom',
    'load shedding',
    'loadshedding',

    # Technology
    '5g',
    'broadband',
    'fiber',
    'internet',
    'telecom',
    'ai',
    'artificial intelligence',
    'digital',
    'cybersecurity',
    'data center',
    'iot',
    'internet of things',
    'cloud computing',
    'mobile banking',
    'satellite internet',
    'starlink',
    'e-commerce',
    'digital payments',
    'biometric',
    'digital id',
    'drone',
    'api'
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}


def clean_html(text: str) -> str:
    """Remove HTML tags and decode HTML entities"""
    if not text:
        return ""

    # Decode HTML entities
    text = unescape(text)

    # Remove HTML tags using BeautifulSoup
    soup = BeautifulSoup(text, 'html.parser')
    text = soup.get_text()

    # Clean up whitespace
    text = ' '.join(text.split())

    return text


def is_relevant_article(title: str, description: str) -> bool:
    """Check if article contains relevant keywords"""
    text = (title + ' ' + description).lower()
    return any(keyword.lower() in text for keyword in SEARCH_KEYWORDS)


def parse_date(date_str: str) -> str:
    """Parse RSS date to ISO format"""
    try:
        # RSS date format: "Thu, 30 Oct 2025 02:08:33 +0000"
        dt = datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %z')
        return dt.strftime('%Y-%m-%d')
    except:
        try:
            # Alternative format without timezone
            dt = datetime.strptime(date_str[:25], '%a, %d %b %Y %H:%M:%S')
            return dt.strftime('%Y-%m-%d')
        except:
            return ""


def is_within_date_range(date_str: str) -> bool:
    """Check if date is within last 7 days"""
    if not date_str:
        return False
    try:
        article_date = datetime.strptime(date_str, '%Y-%m-%d')
        return article_date >= DATE_FILTER
    except:
        return False


def scrape_rss_feed(feed_url: str, category_name: str) -> List[Dict[str, Any]]:
    """Scrape a single RSS feed"""
    print(f"\n  Scraping: {category_name}")

    try:
        response = requests.get(feed_url, headers=HEADERS, timeout=30)
        response.raise_for_status()

        # Parse XML
        root = ET.fromstring(response.content)

        articles = []
        items = root.findall('.//item')

        print(f"    Found {len(items)} items in feed")

        for item in items:
            try:
                # Extract fields
                title = item.find('title')
                link = item.find('link')
                description = item.find('description')
                pub_date = item.find('pubDate')

                if title is None or link is None:
                    continue

                title_text = clean_html(title.text) if title.text else ""
                link_text = link.text.strip() if link.text else ""
                desc_text = clean_html(description.text) if description is not None and description.text else ""
                date_text = pub_date.text if pub_date is not None and pub_date.text else ""

                # Parse date
                date_iso = parse_date(date_text)

                # Filter: only last 7 days
                if not is_within_date_range(date_iso):
                    continue

                # Filter: check if relevant
                if not is_relevant_article(title_text, desc_text):
                    continue

                # Create summary (prefer description, fallback to title)
                summary = desc_text if len(desc_text) > 20 else title_text
                summary = summary[:497] + "..." if len(summary) > 500 else summary

                articles.append({
                    'country': COUNTRY,
                    'source': SOURCE_NAME,
                    'title': title_text.replace(',', ' '),
                    'date_iso': date_iso,
                    'summary': summary.replace(',', ' ').replace('\n', ' '),
                    'url': link_text,
                    'category': '',  # Will be filled by AI
                    'status': ''     # Will be filled by AI
                })

            except Exception as e:
                continue

        print(f"    Kept {len(articles)} relevant articles from last {DAYS_BACK} days")
        return articles

    except Exception as e:
        print(f"    Error scraping {category_name}: {e}")
        return []


def scrape_all_feeds():
    """Scrape all RSS feeds"""
    print(f"Starting Moneyweb RSS scraper")
    print("=" * 60)
    print(f"Collecting articles from last {DAYS_BACK} days")
    print(f"Date filter: {DATE_FILTER.strftime('%Y-%m-%d')} to present")
    print("=" * 60)

    all_data = []
    seen_urls = set()

    for category, feed_url in RSS_FEEDS.items():
        articles = scrape_rss_feed(feed_url, category)

        # Deduplicate across feeds
        for article in articles:
            if article['url'] not in seen_urls:
                seen_urls.add(article['url'])
                all_data.append(article)

    print(f"\n\nTotal unique articles collected: {len(all_data)}")
    return all_data


def save_to_csv(data: List[Dict], output_file: str):
    """Save data to CSV file"""
    if not data:
        print("No data to save")
        return

    fieldnames = ['country', 'source', 'title', 'date_iso', 'summary', 'url', 'category', 'status']

    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

        print(f"\nData saved to: {output_file}")

        # Print summary
        print("\n" + "=" * 60)
        print("SCRAPING SUMMARY")
        print("=" * 60)
        print(f"Total records: {len(data)}")

        # Date coverage
        dates = [d['date_iso'] for d in data if d['date_iso']]
        if dates:
            dates.sort()
            print(f"\nDate range:")
            print(f"  Oldest: {dates[0]}")
            print(f"  Newest: {dates[-1]}")

        print("\nNote: Category and status fields are empty - will be filled by AI processing")
        print("=" * 60)

    except Exception as e:
        print(f"Error saving to CSV: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Scrape Moneyweb RSS feeds')
    parser.add_argument('--output', '-o', default='moneyweb_data.csv', help='Output CSV file')

    args = parser.parse_args()

    # Run scraper
    data = scrape_all_feeds()

    # Save to CSV
    if data:
        save_to_csv(data, args.output)
    else:
        print("No articles collected!")
