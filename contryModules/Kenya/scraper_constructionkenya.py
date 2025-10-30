#!/usr/bin/env python3
"""
Construction Kenya - Infrastructure News Scraper
Scrapes construction and infrastructure project news from Kenya
"""

import requests
import csv
import time
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
BASE_URL = "https://www.constructionkenya.com"
API_URL = f"{BASE_URL}/wp-json/wp/v2/posts"
COUNTRY = "Kenya"
SOURCE_NAME = "Construction Kenya"

# Date filtering - Last 7 days only
DATE_FILTER_DAYS = 7
DATE_AFTER = (datetime.now() - timedelta(days=DATE_FILTER_DAYS)).strftime('%Y-%m-%dT00:00:00')

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}

# Infrastructure keywords - focus on Kenya and regional projects
KENYA_KEYWORDS = [
    'kenya', 'nairobi', 'mombasa', 'kisumu', 'nakuru', 'eldoret', 'thika',
    'kenha', 'kerra', 'kura', 'kpa', 'nms', 'lamu', 'dongo kundu', 'konza',
    'jkia', 'kisumu port', 'naivasha', 'malindi', 'garissa'
]

# Infrastructure categories
INFRASTRUCTURE_KEYWORDS = [
    # Infrastructure
    'road', 'highway', 'bridge', 'port', 'railway', 'rail', 'airport',
    'dam', 'power', 'housing', 'building', 'construction', 'infrastructure',
    'sez', 'special economic zone', 'industrial park', 'bypass', 'interchange',
    'expressway', 'affordable housing', 'water project', 'sewerage',
    # Economic
    'finance', 'trade', 'economic', 'crypto', 'investment', 'fintech',
    'stock exchange', 'banking', 'commerce', 'export', 'capital market',
    # Energy
    'solar', 'wind farm', 'hydropower', 'geothermal', 'renewable energy',
    'nuclear', 'thermal', 'biomass', 'grid', 'substation', 'transmission',
    # Technology
    'digital', 'technology', 'broadband', '5G', 'data center', 'e-government',
    'ICT', 'fiber optic', 'AI', 'cybersecurity', 'telecommunications', 'internet'
]


def fetch_posts_from_api(page: int = 1, per_page: int = 100) -> List[Dict]:
    """Fetch posts from WordPress API"""
    try:
        params = {
            'page': page,
            'per_page': per_page,
            '_embed': 1,  # Include embedded data like featured images
            'after': DATE_AFTER  # Only fetch posts from last 7 days
        }

        response = requests.get(API_URL, params=params, headers=HEADERS, timeout=30, verify=False)
        response.raise_for_status()

        # Get total pages from header
        total_pages = int(response.headers.get('X-WP-TotalPages', 1))
        total_posts = int(response.headers.get('X-WP-Total', 0))

        return response.json(), total_pages, total_posts
    except Exception as e:
        print(f"  Error fetching page {page}: {e}")
        return [], 0, 0


def is_kenya_related(title: str, content: str) -> bool:
    """Check if content is Kenya-related"""
    text = (title + " " + content).lower()
    return any(keyword in text for keyword in KENYA_KEYWORDS)


def is_infrastructure_related(title: str, content: str) -> bool:
    """Check if content is infrastructure-related"""
    text = (title + " " + content).lower()

    # Must have infrastructure keywords
    has_infra = any(keyword in text for keyword in INFRASTRUCTURE_KEYWORDS)
    if not has_infra:
        return False

    # Exclude equipment reviews, product launches, how-to guides, industry news, disputes, lists
    exclude_patterns = [
        'unveils', 'debuts', 'launches new', 'releases new', 'introduces',
        'steps to', 'how to', 'guide to', 'tips for', 'symbols explained',
        'most powerful', 'new range', 'new model', 'upgrades to',
        'ai to unlock', 'tech shaping', 'using ai', 'immigration crackdowns',
        'floor plans', 'what works in', 'overview of the property market',
        'fights to save', 'demolition dispute', 'court battle', 'legal battle',
        'top 10', 'top 15', 'biggest malls', 'tallest buildings', 'list of',
        'feedspot ranks', 'ranked as', 'magazine', 'firms shaping'
    ]

    if any(pattern in text for pattern in exclude_patterns):
        return False

    return True


def parse_date(date_str: str) -> str:
    """Parse WordPress API date to ISO format"""
    try:
        # WordPress API returns dates like: "2025-10-22T10:30:00"
        date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return date_obj.strftime('%Y-%m-%d')
    except:
        return ""


def extract_status(title: str, content: str) -> str:
    """Extract project status from title and content"""
    text = (title + " " + content).lower()

    # Status keywords
    if any(kw in text for kw in ['commissioned', 'inaugurated', 'opened', 'completed', 'launched', 'operational']):
        return "completed"
    elif any(kw in text for kw in ['ongoing', 'under construction', 'construction', 'building', 'developing', 'underway', '% complete']):
        return "ongoing"
    elif any(kw in text for kw in ['planned', 'proposed', 'upcoming', 'to be', 'will', 'approves', 'green light', 'tender', 'contract awarded']):
        return "planned"

    return ""


def determine_category(title: str, content: str) -> str:
    """Determine infrastructure category"""
    text = (title + " " + content).lower()

    if any(kw in text for kw in ['port', 'harbour', 'maritime', 'shipping']):
        return "port"
    elif any(kw in text for kw in ['railway', 'rail', 'sgr', 'train', 'metro']):
        return "rail"
    elif any(kw in text for kw in ['road', 'highway', 'bridge', 'bypass', 'interchange', 'expressway']):
        return "highway"
    elif any(kw in text for kw in ['sez', 'special economic zone', 'industrial park', 'free zone']):
        return "SEZ"
    elif any(kw in text for kw in ['smart city', 'konza', 'tatu city']):
        return "smart city"
    elif any(kw in text for kw in ['housing', 'residential', 'apartments', 'affordable housing']):
        return "infrastructure"  # Housing projects
    elif any(kw in text for kw in ['dam', 'water', 'sewerage', 'pipeline']):
        return "infrastructure"  # Water infrastructure
    elif any(kw in text for kw in ['power', 'energy', 'solar', 'wind', 'geothermal']):
        return "infrastructure"  # Energy infrastructure
    else:
        return "infrastructure"  # General infrastructure


def process_post(post: Dict) -> Dict[str, Any]:
    """Process a single post from API"""
    try:
        # Extract basic fields
        title = post.get('title', {}).get('rendered', '')
        date_iso = parse_date(post.get('date', ''))
        link = post.get('link', '')

        # Remove HTML entities from title
        title = BeautifulSoup(title, 'html.parser').get_text()

        # Extract content/excerpt
        content_html = post.get('content', {}).get('rendered', '')
        excerpt_html = post.get('excerpt', {}).get('rendered', '')

        # Parse content
        soup = BeautifulSoup(content_html or excerpt_html, 'html.parser')
        content_text = soup.get_text(separator=' ', strip=True)

        # Create summary (first 300 chars)
        summary = content_text[:297] + "..." if len(content_text) > 300 else content_text

        # Skip if no meaningful content
        if len(title) < 10 or len(summary) < 50:
            return None

        # STRICT FILTER: Must be Kenya-related AND infrastructure-related
        is_kenya = is_kenya_related(title, summary)

        # Skip if not Kenya-related
        if not is_kenya:
            return None

        # Check if infrastructure-related (actual project, not equipment/how-to)
        if not is_infrastructure_related(title, summary):
            return None

        # Extract status and category
        status = extract_status(title, summary)
        category = determine_category(title, summary)

        return {
            'country': COUNTRY,
            'source': SOURCE_NAME,
            'title': title.replace(',', ' '),  # Remove commas for CSV
            'date_iso': date_iso,
            'summary': summary.replace(',', ' '),  # Remove commas for CSV
            'url': link,
            'category': category,
            'status': status
        }
    except Exception as e:
        print(f"  Error processing post: {e}")
        return None


def scrape_all_posts():
    """Scrape all posts from Construction Kenya via WordPress API"""
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

    # Collect all posts from all pages
    all_posts = posts_data

    # Fetch remaining pages
    if total_pages > 1:
        print(f"\nStep 2: Fetching remaining {total_pages - 1} pages...")
        for page in range(2, total_pages + 1):
            print(f"  Fetching page {page}/{total_pages}...", end='\r')
            posts_data, _, _ = fetch_posts_from_api(page=page, per_page=100)
            if posts_data:
                all_posts.extend(posts_data)
            time.sleep(0.5)  # Be polite to API

    print(f"\n  Fetched {len(all_posts)} total posts")

    # Step 3: Process posts in parallel
    print(f"\nStep 3: Processing posts (filtering for Kenya infrastructure projects only)...")
    all_data = []
    seen_urls = set()  # Track URLs to avoid duplicates
    seen_titles = set()  # Track titles to avoid duplicates
    skipped = 0

    with ThreadPoolExecutor(max_workers=8) as executor:
        # Submit all processing tasks
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

    print(f"\n  Successfully processed {len(all_data)} Kenya infrastructure articles")
    print(f"  Filtered out: {skipped} (non-Kenya or non-project content)")

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

            # Write rows without is_kenya field
            for row in data:
                row_copy = {k: v for k, v in row.items() if k in fieldnames}
                writer.writerow(row_copy)

        print(f"\nData saved to: {output_file}")

        # Print summary
        print("\n" + "="*60)
        print("SCRAPING SUMMARY")
        print("="*60)
        print(f"Total records: {len(data)}")

        # Category breakdown
        categories = {}
        for item in data:
            cat = item['category'] or 'unknown'
            categories[cat] = categories.get(cat, 0) + 1

        print("\nCategory breakdown:")
        for cat, count in sorted(categories.items()):
            print(f"  {cat}: {count}")

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

    parser = argparse.ArgumentParser(description='Scrape Construction Kenya news')
    parser.add_argument('--output', '-o', default='constructionkenya_data.csv', help='Output CSV file')

    args = parser.parse_args()

    # Run scraper
    data = scrape_all_posts()

    # Save to CSV
    if data:
        save_to_csv(data, args.output)
    else:
        print("No infrastructure articles collected!")
