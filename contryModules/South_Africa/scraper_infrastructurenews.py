#!/usr/bin/env python3
"""
Infrastructure News - Scraper
Scrapes infrastructure news from infrastructurenews.co.za
"""

import requests
import csv
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
BASE_URL = "https://infrastructurenews.co.za"
API_URL = f"{BASE_URL}/wp-json/wp/v2/posts"
COUNTRY = "South Africa"
SOURCE_NAME = "Infrastructure News"

# Get current date and calculate 30 days ago
NOW = datetime.now()
CURRENT_YEAR = NOW.year
CURRENT_MONTH = NOW.month
DAYS_BACK = 30  # Only fetch articles from last 30 days
DATE_30_DAYS_AGO = (NOW - timedelta(days=DAYS_BACK)).strftime('%Y-%m-%dT00:00:00')

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}


def is_current_year(date_str: str) -> bool:
    """Check if date is in current year"""
    if not date_str:
        return False

    try:
        # WordPress API date format: 2025-06-23T09:03:00
        date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return date_obj.year == CURRENT_YEAR
    except:
        return False


def parse_date(date_str: str) -> str:
    """Parse WordPress API date to ISO format"""
    try:
        date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return date_obj.strftime('%Y-%m-%d')
    except:
        return ""


def fetch_posts_from_api(page: int = 1, per_page: int = 100) -> tuple:
    """Fetch posts from WordPress API with date filtering"""
    try:
        params = {
            'page': page,
            'per_page': per_page,
            '_embed': 1,
            'after': DATE_30_DAYS_AGO  # Only fetch posts from last 30 days
        }

        response = requests.get(API_URL, params=params, headers=HEADERS, timeout=30, verify=False)
        response.raise_for_status()

        total_pages = int(response.headers.get('X-WP-TotalPages', 1))
        total_posts = int(response.headers.get('X-WP-Total', 0))

        return response.json(), total_pages, total_posts
    except Exception as e:
        print(f"  Error fetching page {page}: {e}")
        return [], 0, 0


def process_post(post: Dict) -> Dict[str, Any]:
    """Process a single post from API"""
    try:
        # Extract basic fields
        title = post.get('title', {}).get('rendered', '')
        date_iso_raw = post.get('date', '')
        link = post.get('link', '')

        # Parse date (API already filtered by year)
        date_iso = parse_date(date_iso_raw)

        # Remove HTML entities from title
        title = BeautifulSoup(title, 'html.parser').get_text()

        # Extract content
        content_html = post.get('content', {}).get('rendered', '')
        excerpt_html = post.get('excerpt', {}).get('rendered', '')

        soup = BeautifulSoup(content_html or excerpt_html, 'html.parser')
        content_text = soup.get_text(separator=' ', strip=True)

        # Create summary (first 500 chars)
        summary = content_text[:497] + "..." if len(content_text) > 500 else content_text

        # Skip if no meaningful content
        if len(title) < 10 or len(summary) < 50:
            return None

        return {
            'country': COUNTRY,
            'source': SOURCE_NAME,
            'title': title.replace(',', ' '),
            'date_iso': date_iso,
            'summary': summary.replace(',', ' '),
            'url': link,
            'category': '',  # Will be filled by AI
            'status': ''     # Will be filled by AI
        }
    except Exception as e:
        print(f"  Error processing post: {e}")
        return None


def scrape_all_posts():
    """Scrape all posts from Infrastructure News via WordPress API"""
    print(f"Starting scraper for {SOURCE_NAME}")
    print("="*60)
    print(f"Collecting articles from last {DAYS_BACK} days")
    print(f"Date filter: {DATE_30_DAYS_AGO} to present")
    print("="*60)

    # Step 1: Get first page to determine total pages
    print("\nStep 1: Fetching posts from WordPress API...")
    posts_data, total_pages, total_posts = fetch_posts_from_api(page=1, per_page=100)

    if not posts_data:
        print("  ERROR: Could not fetch posts from API!")
        return []

    print(f"  Total posts available: {total_posts}")
    print(f"  Total pages to fetch: {total_pages}")

    # Collect all posts
    all_posts = posts_data

    # Fetch remaining pages if any
    if total_pages > 1:
        print(f"\nStep 2: Fetching remaining {total_pages - 1} pages...")
        for page in range(2, total_pages + 1):
            print(f"  Fetching page {page}/{total_pages}...", end='\r')
            posts_data, _, _ = fetch_posts_from_api(page=page, per_page=100)
            if posts_data:
                all_posts.extend(posts_data)
            time.sleep(0.5)

    print(f"\n  Fetched {len(all_posts)} total posts")

    # Step 3: Process posts
    print(f"\nStep 3: Processing posts...")
    all_data = []
    seen_urls = set()  # Track URLs to avoid duplicates
    seen_titles = set()  # Track titles to avoid duplicates
    skipped = 0

    for idx, post in enumerate(all_posts, 1):
        result = process_post(post)
        if result:
            # Deduplicate by URL and title
            if result['url'] not in seen_urls and result['title'] not in seen_titles:
                seen_urls.add(result['url'])
                seen_titles.add(result['title'])
                all_data.append(result)
            else:
                skipped += 1
        else:
            skipped += 1

        print(f"  Progress: {idx}/{len(all_posts)} processed ({len(all_data)} kept, {skipped} skipped)", end='\r')

    print(f"\n  Successfully processed {len(all_data)} articles from last {DAYS_BACK} days")
    print(f"  Skipped: {skipped} (insufficient content)")

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
        print("\n" + "="*60)
        print("SCRAPING SUMMARY")
        print("="*60)
        print(f"Total records: {len(data)}")

        # Date coverage
        dates = [d['date_iso'] for d in data if d['date_iso']]
        if dates:
            dates.sort()
            print(f"\nDate range:")
            print(f"  Oldest: {dates[0]}")
            print(f"  Newest: {dates[-1]}")

        print("\nNote: Category and status fields are empty - will be filled by AI processing")
        print("="*60)

    except Exception as e:
        print(f"Error saving to CSV: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Scrape Infrastructure News')
    parser.add_argument('--output', '-o', default='infrastructurenews_data.csv', help='Output CSV file')

    args = parser.parse_args()

    # Run scraper
    data = scrape_all_posts()

    # Save to CSV
    if data:
        save_to_csv(data, args.output)
    else:
        print("No articles collected!")
