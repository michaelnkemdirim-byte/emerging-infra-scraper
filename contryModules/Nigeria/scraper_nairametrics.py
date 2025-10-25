#!/usr/bin/env python3
"""
Nairametrics - Infrastructure News Scraper
Scrapes infrastructure projects from Real Estate & Construction category
Filters for highways, bridges, rail, airports, ports, power projects
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
BASE_URL = "https://nairametrics.com"
API_URL = f"{BASE_URL}/wp-json/wp/v2/posts"
COUNTRY = "Nigeria"
SOURCE_NAME = "Nairametrics"

# Date filtering - Last 30 days only
DATE_FILTER_DAYS = 30
DATE_AFTER = (datetime.now() - timedelta(days=DATE_FILTER_DAYS)).strftime('%Y-%m-%dT00:00:00')

# Real Estate & Construction category (also has Aviation if needed)
CATEGORIES = {
    'real_estate_construction': 207877,
    'aviation': 304938  # Optional
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}

# Infrastructure keywords - MUST match to be included
INFRASTRUCTURE_KEYWORDS = [
    # Highways/Roads
    'highway', 'expressway', 'road construction', 'road project', 'bridge construction',
    'bridge repair', 'bridge maintenance', 'coastal highway', 'dual carriageway',
    'road rehabilitation', 'road upgrade', 'interchange', 'flyover',
    # Rail
    'rail', 'railway', 'train', 'metro', 'light rail', 'green line', 'red line',
    'blue line', 'rail project', 'rail line', 'rail construction',
    # Ports/Maritime
    'port development', 'seaport', 'port construction', 'terminal construction',
    'jetty', 'wharf', 'maritime infrastructure',
    # Airports/Aviation
    'airport construction', 'airport expansion', 'airport upgrade', 'runway',
    'terminal building', 'aviation infrastructure',
    # Power/Energy Infrastructure
    'power plant', 'power project', 'electricity generation', 'transmission line',
    'substation', 'hydroelectric', 'solar farm', 'wind farm', 'power infrastructure',
    # SEZ/Industrial
    'special economic zone', 'industrial park', 'free trade zone', 'sez',
    # General infrastructure projects
    'infrastructure project', 'infrastructure development', 'construction contract',
    'awarded contract', 'contractor'
]

# Exclude non-project content
EXCLUDE_PATTERNS = [
    # Real estate market news (NOT infrastructure)
    'real estate investment', 'real estate market', 'real estate areas',
    'real estate sector', 'real estate trust', 'reit',
    'property market', 'housing market', 'real estate landscape',
    'rent cost', 'rent price', 'housing allocation', 'apartment prices',
    'property prices', 'land use charge', 'building permit', 'short stay',
    'estate demolition', 'illegal building', 'unapproved building',
    'building regulations', 'diaspora land',
    # Market/commodity news
    'steel rod prices', 'cement prices', 'construction materials price',
    # Administrative/political
    'govt denies', 'govt faults', 'political', 'dispute over',
    # Generic news
    'slips in ranking', 'financial centres index', 'targets',
    'launches online platform',
    # HR/recruitment
    'recruitment', 'vacancy', 'job opportunity', 'career', 'employment',
    # Events
    'workshop', 'training session', 'retreat',
    # People
    'biography', 'appointment', 'promotion', 'resignation'
]


def fetch_posts_from_api(page: int = 1, per_page: int = 100, category_id: int = None) -> tuple:
    """Fetch posts from WordPress API"""
    try:
        params = {
            'page': page,
            'per_page': per_page,
            '_embed': 1,
            'after': DATE_AFTER  # Only fetch posts from last 30 days
        }

        if category_id:
            params['categories'] = category_id

        response = requests.get(API_URL, params=params, headers=HEADERS, timeout=30, verify=False)
        response.raise_for_status()

        total_pages = int(response.headers.get('X-WP-TotalPages', 1))
        total_posts = int(response.headers.get('X-WP-Total', 0))

        return response.json(), total_pages, total_posts
    except Exception as e:
        print(f"  Error fetching page {page}: {e}")
        return [], 0, 0


def parse_date(date_str: str) -> str:
    """Parse WordPress API date to ISO format"""
    try:
        date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return date_obj.strftime('%Y-%m-%d')
    except:
        return ""


def is_infrastructure_content(title: str, content: str) -> bool:
    """Check if content is about actual infrastructure projects"""
    text = (title + " " + content).lower()

    # Must contain at least one infrastructure keyword
    if not any(keyword in text for keyword in INFRASTRUCTURE_KEYWORDS):
        return False

    # Exclude if matches non-project patterns
    if any(pattern in text for pattern in EXCLUDE_PATTERNS):
        return False

    return True


def determine_category(title: str, content: str) -> str:
    """Determine infrastructure category"""
    import re
    text = (title + " " + content).lower()

    # Highway/Road
    if any(kw in text for kw in ['highway', 'expressway', 'road construction', 'road project',
                                   'bridge', 'flyover', 'interchange', 'dual carriageway',
                                   'road rehabilitation', 'road upgrade', 'coastal highway']):
        return "highway"

    # Rail - use word boundaries to avoid matching "trail" in "trailblazer"
    if (re.search(r'\brail\b', text) or
        any(kw in text for kw in ['railway', 'train', 'metro', 'light rail',
                                   'green line', 'red line', 'blue line'])):
        return "rail"

    # Port
    if any(kw in text for kw in ['port development', 'seaport', 'port construction',
                                   'terminal construction', 'jetty', 'wharf', 'maritime']):
        return "port"

    # Airport/Aviation
    if any(kw in text for kw in ['airport', 'runway', 'terminal building', 'aviation infrastructure']):
        return "port"  # Using 'port' for airports as per project schema

    # Power/Energy
    if any(kw in text for kw in ['power plant', 'power project', 'electricity generation',
                                   'transmission line', 'substation', 'hydroelectric',
                                   'solar farm', 'wind farm', 'power infrastructure']):
        return "infrastructure"

    # SEZ
    if any(kw in text for kw in ['special economic zone', 'industrial park',
                                   'free trade zone', 'sez']):
        return "SEZ"

    # Default to infrastructure
    return "infrastructure"


def extract_status(title: str, content: str) -> str:
    """Extract project status"""
    text = (title + " " + content).lower()

    if any(kw in text for kw in ['commissioned', 'completed', 'inaugurated', 'opened',
                                   'operational', 'reopens', 'reopened']):
        return "completed"
    elif any(kw in text for kw in ['ongoing', 'under construction', 'construction',
                                     'developing', 'implementation', 'commences',
                                     'closure for maintenance', 'repair work']):
        return "ongoing"
    elif any(kw in text for kw in ['planned', 'proposed', 'approval', 'approved',
                                     'awarded', 'contract award', 'to commence',
                                     'funding commitment', 'withdraws from',
                                     'drops contractor']):
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
        # Remove " - Nairametrics" suffix
        title = title.replace(' - Nairametrics', '').strip()

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

        # Check if this is infrastructure content
        if not is_infrastructure_content(title, summary):
            return None

        # Determine category
        category = determine_category(title, summary)

        # Extract status
        status = extract_status(title, summary)

        return {
            'country': COUNTRY,
            'source': SOURCE_NAME,
            'title': title.replace(',', ' '),
            'date_iso': date_iso,
            'summary': summary.replace(',', ' '),
            'url': link,
            'category': category,
            'status': status
        }
    except Exception as e:
        print(f"  Error processing post: {e}")
        return None


def scrape_category(category_name: str, category_id: int):
    """Scrape posts from a specific category"""
    print(f"\n{'='*60}")
    print(f"Scraping category: {category_name} (ID: {category_id})")
    print(f"{'='*60}")

    # Step 1: Get first page to determine total pages
    print("\nStep 1: Fetching posts from WordPress API...")
    posts_data, total_pages, total_posts = fetch_posts_from_api(page=1, per_page=100, category_id=category_id)

    if not posts_data:
        print(f"  ERROR: Could not fetch posts from {category_name}!")
        return []

    print(f"  Total posts in category: {total_posts}")
    print(f"  Total pages to fetch: {total_pages}")

    # Collect all posts
    all_posts = posts_data

    # Fetch remaining pages if any
    if total_pages > 1:
        print(f"\nStep 2: Fetching remaining {total_pages - 1} pages...")
        for page in range(2, total_pages + 1):
            print(f"  Fetching page {page}/{total_pages}...", end='\r')
            posts_data, _, _ = fetch_posts_from_api(page=page, per_page=100, category_id=category_id)
            if posts_data:
                all_posts.extend(posts_data)
            time.sleep(0.5)

    print(f"\n  Fetched {len(all_posts)} total posts")

    # Step 3: Process posts in parallel
    print(f"\nStep 3: Processing and filtering posts...")
    category_data = []
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
                        category_data.append(result)
                    else:
                        skipped += 1
                else:
                    skipped += 1

                print(f"  Progress: {completed}/{len(all_posts)} processed ({len(category_data)} kept, {skipped} filtered)", end='\r')
            except Exception as e:
                skipped += 1

    print(f"\n  Successfully extracted {len(category_data)} infrastructure articles")
    print(f"  Filtered out: {skipped} (non-infrastructure/real estate news)")

    return category_data


def scrape_all_posts():
    """Scrape all infrastructure posts from Nairametrics"""
    print(f"Starting scraper for {SOURCE_NAME}")
    print("="*60)

    all_data = []

    # Scrape Real Estate & Construction category
    construction_data = scrape_category("Real Estate & Construction", CATEGORIES['real_estate_construction'])
    all_data.extend(construction_data)

    # Optionally scrape Aviation category
    # aviation_data = scrape_category("Aviation", CATEGORIES['aviation'])
    # all_data.extend(aviation_data)

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

    parser = argparse.ArgumentParser(description='Scrape Nairametrics infrastructure news')
    parser.add_argument('--output', '-o', default='nairametrics_data.csv', help='Output CSV file')

    args = parser.parse_args()

    # Run scraper
    data = scrape_all_posts()

    # Save to CSV
    if data:
        save_to_csv(data, args.output)
    else:
        print("No infrastructure articles collected!")
