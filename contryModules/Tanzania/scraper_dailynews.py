#!/usr/bin/env python3
"""
Daily News Tanzania - WordPress API Scraper
Scrapes infrastructure news from dailynews.co.tz
Search-based scraper for infrastructure-related keywords
"""

import requests
import csv
from datetime import datetime, timedelta
from typing import List, Dict
from bs4 import BeautifulSoup

# Configuration
BASE_URL = "https://dailynews.co.tz"
API_URL = f"{BASE_URL}/wp-json/wp/v2/posts"
COUNTRY = "Tanzania"
SOURCE_NAME = "Daily News Tanzania"
DAYS_BACK = 30

# Calculate date 30 days ago
NOW = datetime.now()
DATE_30_DAYS_AGO = (NOW - timedelta(days=DAYS_BACK)).strftime('%Y-%m-%dT00:00:00')
DATE_THRESHOLD = NOW - timedelta(days=DAYS_BACK)  # For client-side date validation

# Infrastructure-related search keywords
SEARCH_KEYWORDS = [
    'infrastructure',
    'construction',
    'road',
    'railway',
    'port',
    'highway',
    'bridge',
    'airport',
    'SGR',
    'TANROADS'
]


def fetch_posts_for_keyword(keyword: str, per_page: int = 100) -> List[Dict]:
    """Fetch all posts for a specific keyword"""
    params = {
        'per_page': per_page,
        'search': keyword,
        'after': DATE_30_DAYS_AGO,
        '_embed': 1
    }

    try:
        response = requests.get(API_URL, params=params, timeout=30)

        if response.status_code == 200:
            return response.json()
        else:
            return []

    except Exception as e:
        print(f"  Error fetching {keyword}: {e}")
        return []


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


def is_infrastructure_relevant(title: str, summary: str) -> bool:
    """Check if article is actually infrastructure-related (avoid false positives)"""
    text = f"{title} {summary}".lower()

    # Exclude pure political/election news without infrastructure context
    political_only = ['election', 'vote', 'party', 'manifesto', 'campaign']
    infra_keywords = ['infrastructure', 'construction', 'build', 'road', 'railway', 'port', 'bridge',
                      'airport', 'sgr', 'tanroads', 'highway', 'project']

    # If it's political content, must also contain infrastructure keywords
    is_political = any(word in text for word in political_only)
    has_infrastructure = any(word in text for word in infra_keywords)

    if is_political:
        return has_infrastructure

    return True  # Non-political articles that matched search are likely relevant


def scrape_all_posts() -> List[Dict]:
    """Scrape all infrastructure posts from last 30 days"""
    print(f"Starting scraper for {SOURCE_NAME}")
    print("="*60)
    print(f"Collecting articles from last {DAYS_BACK} days")
    print(f"Date filter: {DATE_30_DAYS_AGO[:10]} to present")
    print(f"Search keywords: {', '.join(SEARCH_KEYWORDS)}")
    print("="*60)

    all_data = []
    seen_urls = set()

    # Search for each keyword separately
    for keyword in SEARCH_KEYWORDS:
        print(f"\nSearching for: {keyword}")
        posts = fetch_posts_for_keyword(keyword)

        if not posts:
            print(f"  No posts found")
            continue

        print(f"  Found {len(posts)} posts")

        for post in posts:
            try:
                # Extract data
                title = BeautifulSoup(post['title']['rendered'], 'html.parser').get_text()
                date_str = post['date']
                date_iso = datetime.fromisoformat(date_str.replace('Z', '+00:00')).strftime('%Y-%m-%d')
                url = post['link']
                summary = extract_summary(post)

                # Skip duplicates
                if url in seen_urls or title in seen_titles:
                    continue
                seen_urls.add(url)

                seen_titles.add(title)

                # Filter relevance
                if not is_infrastructure_relevant(title, summary):
                    print(f"    Skipping (not infrastructure): {title[:60]}")
                    continue

                # Clean text
                title = title.strip().replace(',', ' ')
                summary = summary.strip().replace(',', ' ').replace('\n', ' ')

                # Client-side date validation (filter articles outside 30-day range)
                try:
                    article_date = datetime.strptime(date_iso, '%Y-%m-%d')
                    if article_date < DATE_THRESHOLD:
                        print(f"    Skipping old article ({date_iso}): {title[:60]}")
                        continue
                except:
                    # If date parsing fails, include for manual review
                    pass

                all_data.append({
                    'country': COUNTRY,
                    'source': SOURCE_NAME,
                    'title': title,
                    'date_iso': date_iso,
                    'summary': summary,
                    'url': url,
                    'category': '',  # Will be filled by AI
                    'status': ''
                })

            except Exception as e:
                print(f"  Error processing post: {e}")
                continue

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

    parser = argparse.ArgumentParser(description='Scrape Daily News Tanzania')
    parser.add_argument('--output', '-o', default='dailynews_data.csv', help='Output CSV file')

    args = parser.parse_args()

    # Run scraper
    data = scrape_all_posts()

    # Save to CSV
    if data:
        save_to_csv(data, args.output)
    else:
        print("No articles collected!")
