#!/usr/bin/env python3
"""
TanzaniaInvest - WordPress API Scraper
Scrapes infrastructure news from tanzaniainvest.com
Categories: Construction, Transport, Energy, Real Estate
"""

import requests
import csv
from datetime import datetime, timedelta
from typing import List, Dict
from bs4 import BeautifulSoup

# Configuration
BASE_URL = "https://www.tanzaniainvest.com"
API_URL = f"{BASE_URL}/wp-json/wp/v2/posts"
COUNTRY = "Tanzania"
SOURCE_NAME = "TanzaniaInvest"
DAYS_BACK = 30

# Calculate date 30 days ago
NOW = datetime.now()
DATE_30_DAYS_AGO = (NOW - timedelta(days=DAYS_BACK)).strftime('%Y-%m-%dT00:00:00')

# Infrastructure-related categories with IDs
CATEGORIES = {
    'Construction': 93,
    'Transport': 13,
    'Energy': 109,
    'Real Estate': 106
}


def fetch_posts_from_api(page: int = 1, per_page: int = 100) -> tuple:
    """Fetch posts from WordPress API"""
    # Get all infrastructure category IDs
    category_ids = ','.join(str(id) for id in CATEGORIES.values())

    params = {
        'page': page,
        'per_page': per_page,
        'categories': category_ids,
        'after': DATE_30_DAYS_AGO,
        '_embed': 1
    }

    try:
        response = requests.get(API_URL, params=params, timeout=30)

        if response.status_code == 200:
            total_pages = int(response.headers.get('X-WP-TotalPages', 1))
            return response.json(), total_pages
        elif response.status_code == 400:
            # No posts found
            return [], 0
        else:
            print(f"API request failed: {response.status_code}")
            return [], 0

    except Exception as e:
        print(f"Error fetching from API: {e}")
        return [], 0


def extract_summary(post: dict) -> str:
    """Extract clean summary from post"""
    # Try excerpt first
    if 'excerpt' in post and post['excerpt'].get('rendered'):
        summary = BeautifulSoup(post['excerpt']['rendered'], 'html.parser').get_text()
        summary = ' '.join(summary.split())  # Clean whitespace
        if len(summary) > 50:
            return summary[:497] + "..." if len(summary) > 500 else summary

    # Fallback to content preview
    if 'content' in post and post['content'].get('rendered'):
        content = BeautifulSoup(post['content']['rendered'], 'html.parser').get_text()
        content = ' '.join(content.split())
        return content[:497] + "..." if len(content) > 500 else content[:500]

    return ""


def get_category_name(post: dict) -> str:
    """Extract category name from embedded data"""
    try:
        if '_embedded' in post and 'wp:term' in post['_embedded']:
            categories = post['_embedded']['wp:term'][0]
            if categories:
                # Return first category name
                return categories[0]['name']
    except:
        pass
    return "Infrastructure"


def scrape_all_posts() -> List[Dict]:
    """Scrape all infrastructure posts from last 30 days"""
    print(f"Starting scraper for {SOURCE_NAME}")
    print("="*60)
    print(f"Collecting articles from last {DAYS_BACK} days")
    print(f"Date filter: {DATE_30_DAYS_AGO[:10]} to present")
    print(f"Categories: {', '.join(CATEGORIES.keys())}")
    print("="*60)

    all_data = []
    page = 1

    while True:
        print(f"\nFetching page {page}...")
        posts, total_pages = fetch_posts_from_api(page)

        if not posts:
            print(f"  No more posts found")
            break

        print(f"  Found {len(posts)} posts on page {page}/{total_pages}")

        for post in posts:
            try:
                # Extract data
                title = BeautifulSoup(post['title']['rendered'], 'html.parser').get_text()
                date_str = post['date']
                date_iso = datetime.fromisoformat(date_str.replace('Z', '+00:00')).strftime('%Y-%m-%d')
                url = post['link']
                summary = extract_summary(post)
                wp_category = get_category_name(post)

                # Clean text
                title = title.strip().replace(',', ' ')
                summary = summary.strip().replace(',', ' ').replace('\n', ' ')

                all_data.append({
                    'country': COUNTRY,
                    'source': SOURCE_NAME,
                    'title': title,
                    'date_iso': date_iso,
                    'summary': summary,
                    'url': url,
                    'category': '',  # Will be filled by AI (WP categories not precise enough)
                    'status': ''
                })

            except Exception as e:
                print(f"  Error processing post: {e}")
                continue

        # Check if more pages
        if page >= total_pages:
            break

        page += 1

    print(f"\n\nTotal articles collected: {len(all_data)}")
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

        print("\nNote: Category and status will be filled by AI processing")
        print("="*60)

    except Exception as e:
        print(f"Error saving to CSV: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Scrape TanzaniaInvest')
    parser.add_argument('--output', '-o', default='tanzaniainvest_data.csv', help='Output CSV file')

    args = parser.parse_args()

    # Run scraper
    data = scrape_all_posts()

    # Save to CSV
    if data:
        save_to_csv(data, args.output)
    else:
        print("No articles collected!")
