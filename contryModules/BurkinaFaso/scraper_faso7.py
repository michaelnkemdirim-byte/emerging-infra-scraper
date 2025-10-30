#!/usr/bin/env python3
"""
Scraper for Faso7 (Burkina Faso)
Website: https://faso7.com
Method: WordPress REST API with Patchright (bypasses CAPTCHA)
"""

from patchright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import csv
from datetime import datetime, timedelta
from html import unescape
import argparse
import json
import time
import warnings

# Suppress BeautifulSoup warnings
warnings.filterwarnings('ignore', category=UserWarning, module='bs4')

# Configuration
API_BASE_URL = 'https://faso7.com/wp-json/wp/v2/posts'
DATE_FILTER_DAYS = 7  # Changed to 7 days to match other scrapers
DATE_AFTER = (datetime.now() - timedelta(days=DATE_FILTER_DAYS)).strftime('%Y-%m-%dT00:00:00')

# Comprehensive keyword list (French) based on project requirements
KEYWORDS = [
    # Infrastructure - Transportation
    'infrastructure', 'route', 'autoroute', 'pont', 'transport',
    'port', 'corridor', 'maritime',
    'chemin de fer', 'ferroviaire', 'train',
    'aéroport', 'aviation',

    # Infrastructure - Industrial & Urban Development
    'zone industrielle', 'parc industriel', 'industrialisation',
    'ville intelligente', 'développement urbain',
    'construction',

    # Infrastructure - Utilities
    'eau', 'assainissement', 'barrage',

    # Economic - Finance & Trade
    'finance', 'bancaire', 'banque', 'commerce', 'économie', 'économique',
    'investissement', 'exportation', 'importation',
    'fintech', 'mobile money', 'bourse',
    'crypto', 'cryptomonnaie', 'blockchain', 'bitcoin',

    # Energy
    'énergie', 'électricité', 'centrale électrique',
    'solaire', 'éolien', 'hydroélectrique',
    'énergie renouvelable', 'géothermie',
    'nucléaire', 'thermique',
    'pétrole', 'gaz',

    # Technology
    'technologie', 'numérique', 'digital',
    'télécommunications', 'télécom', '5g', '4g',
    'internet', 'fibre optique', 'centre de données',
    'tic', 'innovation', 'startup',
    'cybersécurité', 'e-gouvernement', 'transformation digitale'
]

def clean_html(raw_html):
    """Remove HTML tags and clean text"""
    if not raw_html:
        return ""
    soup = BeautifulSoup(raw_html, 'html.parser')
    text = soup.get_text(separator=' ', strip=True)
    return unescape(text)

def fetch_with_playwright(url, page, first_request=False):
    """Fetch URL using Playwright and return JSON data"""
    try:
        page.goto(url, wait_until='networkidle', timeout=20000)

        # Wait for CAPTCHA auto-solve (only on first request)
        if first_request:
            time.sleep(5)  # 5 seconds for first page to load
        else:
            time.sleep(1)  # 1 second wait for subsequent requests

        # Get page content
        content = page.evaluate('() => document.body.innerText')

        # Parse JSON
        try:
            data = json.loads(content)
            return data
        except json.JSONDecodeError:
            return None

    except Exception as e:
        return None

def scrape_faso7():
    """Main scraping function for Faso7"""
    print("="*70)
    print("Scraping Faso7 (Burkina Faso) with Patchright")
    print("="*70)
    print(f"Date range: Last {DATE_FILTER_DAYS} days (after {DATE_AFTER.split('T')[0]})")
    print()

    all_data = []
    seen_urls = set()

    with sync_playwright() as p:
        # Launch browser with headless=False to bypass CAPTCHA
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()  # Create one page to reuse

        print("Searching keywords...")
        print("-" * 70)

        for idx, keyword in enumerate(KEYWORDS):
            print(f"  Searching '{keyword}'...", end=" ", flush=True)

            # Build API URL
            url = f"{API_BASE_URL}?search={keyword}&after={DATE_AFTER}&per_page=20"

            # Fetch data (first_request=True only for first keyword)
            posts = fetch_with_playwright(url, page, first_request=(idx == 0))

            if not posts or not isinstance(posts, list):
                print("no results")
                continue

            print(f"found {len(posts)} posts")

            # Process each post
            for post in posts:
                try:
                    article_url = post.get('link', '')

                    # Skip duplicates
                    if article_url in seen_urls:
                        continue

                    seen_urls.add(article_url)

                    # Extract fields
                    title_raw = post.get('title', {}).get('rendered', '')
                    title = clean_html(title_raw)

                    # Parse date
                    date_str = post.get('date', '')
                    date_iso = date_str.split('T')[0] if 'T' in date_str else ''

                    # Get excerpt/content for summary
                    excerpt_raw = post.get('excerpt', {}).get('rendered', '')
                    content_raw = post.get('content', {}).get('rendered', '')

                    if excerpt_raw:
                        summary = clean_html(excerpt_raw)
                    else:
                        summary = clean_html(content_raw)

                    if len(summary) > 500:
                        summary = summary[:497] + '...'

                    # Create article entry
                    article = {
                        'country': 'Burkina Faso',
                        'source': 'Faso7',
                        'title': title,
                        'date_iso': date_iso,
                        'summary': summary,
                        'url': article_url,
                        'category': '',  # Will be filled by AI
                        'status': ''
                    }

                    all_data.append(article)

                except Exception as e:
                    continue

        page.close()  # Close the page after all searches
        browser.close()

    print("-" * 70)
    print(f"Total unique articles collected: {len(all_data)}")
    print("="*70)

    return all_data

def save_to_csv(data, output_file):
    """Save data to CSV file"""
    if not data:
        print("No data to save!")
        return

    fieldnames = ['country', 'source', 'title', 'date_iso', 'summary', 'url', 'category', 'status']

    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    print(f"\n✓ Data saved to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape Faso7 for infrastructure news')
    parser.add_argument('--output', default='faso7_data.csv', help='Output CSV file')
    args = parser.parse_args()

    data = scrape_faso7()
    save_to_csv(data, args.output)
