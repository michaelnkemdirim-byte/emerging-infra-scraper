#!/usr/bin/env python3
"""
Kenya Railways Corporation (KRC) - News Scraper
Scrapes railway infrastructure project news and announcements
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
BASE_URL = "https://krc.co.ke"
API_URL = f"{BASE_URL}/wp-json/wp/v2/posts"
COUNTRY = "Kenya"
SOURCE_NAME = "Kenya Railways Corporation"

# Date filtering - Last 7 days only
DATE_FILTER_DAYS = 7
DATE_AFTER = (datetime.now() - timedelta(days=DATE_FILTER_DAYS)).strftime('%Y-%m-%dT00:00:00')

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}

# Railway/infrastructure keywords
INFRASTRUCTURE_KEYWORDS = [
    # Rail (primary focus)
    'railway', 'rail', 'train', 'sgr', 'station', 'track', 'locomotive',
    'passenger service', 'freight', 'cargo', 'metro', 'commuter',
    'infrastructure', 'construction', 'upgrade', 'rehabilitation',
    'expansion', 'project', 'line', 'route', 'service', 'transport',
    # Economic
    'finance', 'trade', 'economic', 'investment', 'fintech', 'banking',
    'commerce', 'export', 'crypto', 'stock exchange',
    # Energy
    'solar', 'wind farm', 'hydropower', 'renewable energy', 'geothermal',
    'power plant', 'electricity', 'grid', 'nuclear', 'thermal',
    # Technology
    'digital', 'technology', 'broadband', '5G', 'data center', 'e-government',
    'ICT', 'fiber optic', 'AI', 'cybersecurity', 'telecommunications'
]

# Exclude non-project content
EXCLUDE_PATTERNS = [
    'recruitment', 'vacancy', 'job opportunity', 'career',
    'tender notice', 'tender award', 'procurement',
    'financial results', 'audit report', 'annual report',
    'board meeting', 'agm', 'general meeting'
]


def fetch_posts_from_api(page: int = 1, per_page: int = 100) -> tuple:
    """Fetch posts from WordPress API"""
    try:
        params = {
            'page': page,
            'per_page': per_page,
            '_embed': 1,
            'after': DATE_AFTER  # Only fetch posts from last 7 days
        }

        response = requests.get(API_URL, params=params, headers=HEADERS, timeout=30, verify=False)
        response.raise_for_status()

        total_pages = int(response.headers.get('X-WP-TotalPages', 1))
        total_posts = int(response.headers.get('X-WP-Total', 0))

        return response.json(), total_pages, total_posts
    except Exception as e:
        print(f"  Error fetching page {page}: {e}")
        return [], 0, 0


def is_infrastructure_related(title: str, content: str) -> bool:
    """Check if content is railway/infrastructure-related"""
    text = (title + " " + content).lower()

    # Must have infrastructure/railway keywords
    has_infra = any(keyword in text for keyword in INFRASTRUCTURE_KEYWORDS)
    if not has_infra:
        return False

    # Exclude non-project content
    if any(pattern in text for pattern in EXCLUDE_PATTERNS):
        return False

    return True


def parse_date(date_str: str) -> str:
    """Parse WordPress API date to ISO format"""
    try:
        date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return date_obj.strftime('%Y-%m-%d')
    except:
        return ""


def extract_status(title: str, content: str) -> str:
    """Extract project status"""
    text = (title + " " + content).lower()

    if any(kw in text for kw in ['launched', 'inaugurated', 'opened', 'completed', 'operational', 'commissioned']):
        return "completed"
    elif any(kw in text for kw in ['ongoing', 'under construction', 'upgrading', 'rehabilitating', 'developing']):
        return "ongoing"
    elif any(kw in text for kw in ['planned', 'proposed', 'to be', 'will', 'approval', 'greenlight']):
        return "planned"

    return ""


def process_post(post: Dict) -> Dict[str, Any]:
    """Process a single post from API"""
    try:
        # Extract basic fields
        title = post.get('title', {}).get('rendered', '')
        date_iso = parse_date(post.get('date', ''))
        link = post.get('link', '')

        # Remove HTML entities from title
        title = BeautifulSoup(title, 'html.parser').get_text()

        # Extract content
        content_html = post.get('content', {}).get('rendered', '')
        excerpt_html = post.get('excerpt', {}).get('rendered', '')

        soup = BeautifulSoup(content_html or excerpt_html, 'html.parser')
        content_text = soup.get_text(separator=' ', strip=True)

        # Create summary (first 300 chars)
        summary = content_text[:297] + "..." if len(content_text) > 300 else content_text

        # Skip if no meaningful content
        if len(title) < 10 or len(summary) < 50:
            return None

        # Check if infrastructure-related
        if not is_infrastructure_related(title, summary):
            return None

        # Extract status
        status = extract_status(title, summary)

        return {
            'country': COUNTRY,
            'source': SOURCE_NAME,
            'title': title.replace(',', ' '),
            'date_iso': date_iso,
            'summary': summary.replace(',', ' '),
            'url': link,
            'category': 'rail',  # All KRC content is railway-related
            'status': status
        }
    except Exception as e:
        print(f"  Error processing post: {e}")
        return None


def scrape_all_posts():
    """Scrape all posts from KRC via WordPress API"""
    print(f"Starting scraper for {SOURCE_NAME}")
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

    # Step 3: Process posts in parallel
    print(f"\nStep 3: Processing posts (filtering for railway infrastructure)...")
    all_data = []
    seen_urls = set()  # Track URLs to avoid duplicates
    seen_titles = set()  # Track titles to avoid duplicates
    skipped = 0

    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_post = {executor.submit(process_post, post): post for post in all_posts}

        completed = 0
        for future in as_completed(future_to_post):
            completed += 1

            try:
                result = future.result()
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

                print(f"  Progress: {completed}/{len(all_posts)} processed ({len(all_data)} kept, {skipped} filtered)", end='\r')
            except Exception as e:
                skipped += 1

    print(f"\n  Successfully processed {len(all_data)} railway infrastructure articles")
    print(f"  Filtered out: {skipped} (non-infrastructure content)")

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

        # Status breakdown
        statuses = {}
        for item in data:
            status = item['status'] or 'unknown'
            statuses[status] = statuses.get(status, 0) + 1

        print("\nStatus breakdown:")
        for status, count in sorted(statuses.items()):
            print(f"  {status}: {count}")

        # Date coverage
        dates = [d['date_iso'] for d in data if d['date_iso']]
        if dates:
            dates.sort()
            print(f"\nDate range:")
            print(f"  Oldest: {dates[0]}")
            print(f"  Newest: {dates[-1]}")

        print("="*60)

    except Exception as e:
        print(f"Error saving to CSV: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Scrape Kenya Railways Corporation news')
    parser.add_argument('--output', '-o', default='krc_data.csv', help='Output CSV file')

    args = parser.parse_args()

    # Run scraper
    data = scrape_all_posts()

    # Save to CSV
    if data:
        save_to_csv(data, args.output)
    else:
        print("No railway infrastructure articles collected!")
