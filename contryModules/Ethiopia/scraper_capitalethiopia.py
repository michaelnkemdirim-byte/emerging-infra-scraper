#!/usr/bin/env python3
"""
Scraper for Capital Ethiopia
Website: https://capitalethiopia.com
Type: WordPress news portal with infrastructure coverage
Focus: Business, infrastructure, energy, construction news
"""

import requests
import csv
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from html import unescape
import time
import argparse

# Configuration
BASE_URL = 'https://capitalethiopia.com'
WP_API_URL = f'{BASE_URL}/wp-json/wp/v2/posts'

# Date filter: last 30 days for weekly reports
DATE_30_DAYS_AGO = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%dT%H:%M:%S')

# Search keywords for infrastructure content
SEARCH_KEYWORDS = [
    'infrastructure',
    'construction',
    'road',
    'highway',
    'expressway',
    'railway',
    'port',
    'bridge',
    'industrial park',
    'special economic zone',
    'SEZ',
    'smart city',
    'urban development',
    'housing',
    'electricity',
    'power plant',
    'transmission',
    'dam'
]

def clean_html(raw_html):
    """Remove HTML tags and clean text"""
    if not raw_html:
        return ""
    soup = BeautifulSoup(raw_html, 'html.parser')
    text = soup.get_text(separator=' ', strip=True)
    return unescape(text)

def is_infrastructure_relevant(title: str, summary: str) -> bool:
    """Check if article is infrastructure-related (avoid false positives)"""
    text = f"{title} {summary}".lower()

    # Exclude purely financial/business articles without infrastructure context
    if any(word in text for word in ['stock market', 'forex', 'banking sector', 'insurance']):
        # Only include if also mentions infrastructure
        if not any(kw in text for kw in ['infrastructure', 'construction', 'road', 'railway', 'port', 'industrial']):
            return False

    return True

def fetch_posts_for_keyword(keyword: str, page: int = 1, per_page: int = 100) -> list:
    """
    Fetch posts from WordPress API for a specific keyword
    Returns: list of posts
    """
    params = {
        'search': keyword,
        'page': page,
        'per_page': per_page,
        'after': DATE_30_DAYS_AGO,
        '_embed': 1
    }

    try:
        response = requests.get(WP_API_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"Error fetching keyword '{keyword}' page {page}: {e}")
        return []

def extract_article_data(post):
    """Extract relevant data from a WordPress post"""
    try:
        # Basic fields
        title = clean_html(post.get('title', {}).get('rendered', ''))
        url = post.get('link', '')

        # Date published
        date_str = post.get('date', '')
        if date_str:
            date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            date_published = date_obj.strftime('%Y-%m-%d')
        else:
            date_published = ''

        # Summary (excerpt or content preview)
        excerpt = clean_html(post.get('excerpt', {}).get('rendered', ''))
        if not excerpt:
            content = clean_html(post.get('content', {}).get('rendered', ''))
            excerpt = content[:300] + '...' if len(content) > 300 else content

        # Filter out non-infrastructure articles
        if not is_infrastructure_relevant(title, excerpt):
            return None

        # Source
        source = 'Capital Ethiopia'

        # Category - leave empty for AI to fill based on content
        category = ''

        # Status - leave empty for AI to determine
        status = ''

        return {
            'country': 'Ethiopia',
            'source': source,
            'title': title,
            'date_iso': date_published,
            'summary': excerpt,
            'url': url,
            'category': category,
            'status': status
        }

    except Exception as e:
        print(f"Error extracting article data: {e}")
        return None

def scrape_capitalethiopia():
    """Main scraping function for Capital Ethiopia"""
    print("="*70)
    print("Scraping Capital Ethiopia (capitalethiopia.com)")
    print("="*70)
    print(f"Fetching posts from last 30 days (after {DATE_30_DAYS_AGO[:10]})")
    print(f"Searching with {len(SEARCH_KEYWORDS)} infrastructure keywords")
    print()

    all_data = []
    seen_urls = set()

    for keyword in SEARCH_KEYWORDS:
        print(f"\nSearching for: {keyword}")
        page = 1

        while True:
            posts = fetch_posts_for_keyword(keyword, page)

            if not posts:
                break

            for post in posts:
                article = extract_article_data(post)
                if article and article['title'] and article['url']:
                    # Deduplicate by URL
                    if article['url'] not in seen_urls:
                        seen_urls.add(article['url'])
                        all_data.append(article)
                        print(f"  ✓ {article['date_iso']}: {article['title'][:70]}")

            # Only check first page per keyword to reduce API calls
            break

        time.sleep(0.5)  # Be polite to the server

    print()
    print(f"Total articles collected: {len(all_data)}")

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
    parser = argparse.ArgumentParser(description='Scrape Capital Ethiopia for infrastructure news')
    parser.add_argument('--output', default='capitalethiopia_data.csv', help='Output CSV file')
    args = parser.parse_args()

    data = scrape_capitalethiopia()
    save_to_csv(data, args.output)
