#!/usr/bin/env python3
"""
TechCentral - WordPress API Scraper
Scrapes technology, infrastructure, and business news from techcentral.co.za
"""

import requests
import csv
from datetime import datetime, timedelta
from typing import List, Dict, Any
from html import unescape
from bs4 import BeautifulSoup

# Configuration
BASE_URL = "https://techcentral.co.za"
API_URL = f"{BASE_URL}/wp-json/wp/v2/posts"
COUNTRY = "South Africa"
SOURCE_NAME = "TechCentral"

# Date filtering - Last 7 days only
DAYS_BACK = 7
DATE_FILTER = datetime.now() - timedelta(days=DAYS_BACK)

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


def is_relevant_article(title: str, excerpt: str) -> bool:
    """Check if article contains relevant keywords"""
    text = (title + ' ' + excerpt).lower()
    return any(keyword.lower() in text for keyword in SEARCH_KEYWORDS)


def parse_date(date_str: str) -> str:
    """Parse WordPress date to ISO format"""
    try:
        # WordPress date format: "2025-10-29T16:54:03"
        dt = datetime.strptime(date_str[:19], '%Y-%m-%dT%H:%M:%S')
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


def fetch_wordpress_posts(page: int = 1, per_page: int = 100) -> List[Dict]:
    """Fetch posts from WordPress API"""
    import time

    max_retries = 3
    for attempt in range(max_retries):
        try:
            params = {
                'page': page,
                'per_page': per_page,
                '_embed': 1  # Include embedded data
            }

            response = requests.get(API_URL, params=params, headers=HEADERS, timeout=60)
            response.raise_for_status()

            return response.json()
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                print(f"    Attempt {attempt + 1} failed, retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"    Error fetching page {page} after {max_retries} attempts: {e}")
                return []

    return []


def scrape_techcentral():
    """Scrape TechCentral WordPress API"""
    print(f"Starting TechCentral WordPress API scraper")
    print("=" * 60)
    print(f"Collecting articles from last {DAYS_BACK} days")
    print(f"Date filter: {DATE_FILTER.strftime('%Y-%m-%d')} to present")
    print("=" * 60)

    all_data = []
    seen_urls = set()
    page = 1

    while True:
        print(f"\n  Fetching page {page}...")
        posts = fetch_wordpress_posts(page=page, per_page=100)

        if not posts:
            print(f"    No more posts found")
            break

        print(f"    Found {len(posts)} posts")

        articles_this_page = 0
        stop_pagination = False

        for post in posts:
            try:
                # Extract fields
                title_raw = post.get('title', {}).get('rendered', '')
                title = clean_html(title_raw)

                url = post.get('link', '')

                excerpt_raw = post.get('excerpt', {}).get('rendered', '')
                excerpt = clean_html(excerpt_raw)

                date_str = post.get('date', '')
                date_iso = parse_date(date_str)

                # Check if we've gone too far back in time
                if not is_within_date_range(date_iso):
                    stop_pagination = True
                    continue

                # Skip duplicates
                if url in seen_urls:
                    continue

                # Filter: check if relevant
                if not is_relevant_article(title, excerpt):
                    continue

                seen_urls.add(url)

                # Create summary (use excerpt, fallback to title)
                summary = excerpt if len(excerpt) > 20 else title
                summary = summary[:497] + "..." if len(summary) > 500 else summary

                all_data.append({
                    'country': COUNTRY,
                    'source': SOURCE_NAME,
                    'title': title.replace(',', ' '),
                    'date_iso': date_iso,
                    'summary': summary.replace(',', ' ').replace('\n', ' '),
                    'url': url,
                    'category': '',  # Will be filled by AI
                    'status': ''     # Will be filled by AI
                })

                articles_this_page += 1

            except Exception as e:
                continue

        print(f"    Kept {articles_this_page} relevant articles from this page")

        # Stop if we've gone past our date range
        if stop_pagination:
            print(f"    Reached articles older than {DAYS_BACK} days, stopping pagination")
            break

        # Stop if no articles found on this page
        if len(posts) < 100:
            print(f"    Reached last page")
            break

        page += 1

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

    parser = argparse.ArgumentParser(description='Scrape TechCentral WordPress API')
    parser.add_argument('--output', '-o', default='techcentral_data.csv', help='Output CSV file')

    args = parser.parse_args()

    # Run scraper
    data = scrape_techcentral()

    # Save to CSV
    if data:
        save_to_csv(data, args.output)
    else:
        print("No articles collected!")
