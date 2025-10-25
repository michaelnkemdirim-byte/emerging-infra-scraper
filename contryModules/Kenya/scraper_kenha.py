#!/usr/bin/env python3
"""
Kenya National Highways Authority (KeNHA) - Project Scraper
Scrapes highway infrastructure projects from official government portal
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
BASE_URL = "https://kenha.co.ke"
API_URL = f"{BASE_URL}/wp-json/wp/v2/posts"
CATEGORIES_URL = f"{BASE_URL}/wp-json/wp/v2/categories"
COUNTRY = "Kenya"
SOURCE_NAME = "Kenya National Highways Authority"

# Date filtering - Last 30 days only
DATE_FILTER_DAYS = 30
DATE_AFTER = (datetime.now() - timedelta(days=DATE_FILTER_DAYS)).strftime('%Y-%m-%dT00:00:00')

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}


def fetch_categories() -> Dict[int, Dict[str, str]]:
    """Fetch category mappings from WordPress API"""
    try:
        response = requests.get(CATEGORIES_URL, headers=HEADERS, timeout=30, verify=False)
        response.raise_for_status()
        categories = response.json()

        cat_map = {}
        for cat in categories:
            cat_id = cat.get('id')
            cat_name = cat.get('name', '').lower()
            cat_slug = cat.get('slug', '').lower()

            # Map to project status
            status = ""
            if 'completed' in cat_name or 'completed' in cat_slug:
                status = "completed"
            elif 'ongoing' in cat_name or 'ongoing' in cat_slug or 'on-going' in cat_name:
                status = "ongoing"
            elif 'planned' in cat_name or 'planned' in cat_slug:
                status = "planned"
            elif 'maintenance' in cat_name:
                status = "ongoing"  # Maintenance projects are ongoing

            cat_map[cat_id] = {
                'name': cat.get('name', ''),
                'slug': cat_slug,
                'status': status
            }

        return cat_map
    except Exception as e:
        print(f"  Error fetching categories: {e}")
        return {}


def fetch_posts_from_api(page: int = 1, per_page: int = 100) -> tuple:
    """Fetch posts from WordPress API"""
    try:
        params = {
            'page': page,
            'per_page': per_page,
            '_embed': 1,
            'after': DATE_AFTER  # Only fetch posts from last 30 days
        }

        response = requests.get(API_URL, params=params, headers=HEADERS, timeout=30, verify=False)
        response.raise_for_status()

        total_pages = int(response.headers.get('X-WP-TotalPages', 1))
        total_posts = int(response.headers.get('X-WP-Total', 0))

        return response.json(), total_pages, total_posts
    except Exception as e:
        print(f"  Error fetching page {page}: {e}")
        return [], 0, 0


def is_highway_related(title: str, content: str) -> bool:
    """Check if content is highway/road infrastructure-related"""
    text = (title + " " + content).lower()

    # Highway/road infrastructure keywords
    highway_keywords = [
        'road', 'highway', 'bridge', 'bypass', 'expressway', 'interchange',
        'dualling', 'dual carriageway', 'junction', 'roundabout', 'overpass',
        'underpass', 'flyover', 'pavement', 'tarmac', 'construction',
        'rehabilitation', 'upgrade', 'expansion', 'route', 'corridor',
        'kilometre', 'km', 'section', 'phase', 'lot'
    ]

    # Must have highway keywords
    has_highway = any(keyword in text for keyword in highway_keywords)
    if not has_highway:
        return False

    # Exclude non-project content
    exclude_patterns = [
        'recruitment', 'vacancy', 'job opportunity', 'career',
        'tender notice', 'procurement', 'eoi', 'expression of interest',
        'annual report', 'financial results', 'audit',
        'board meeting', 'agm', 'training', 'workshop', 'seminar',
        'press release only', 'statement', 'condolence'
    ]

    if any(pattern in text for pattern in exclude_patterns):
        return False

    return True


def parse_date(date_str: str) -> str:
    """Parse WordPress API date to ISO format"""
    try:
        date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return date_obj.strftime('%Y-%m-%d')
    except:
        return ""


def extract_status_from_content(title: str, content: str) -> str:
    """Extract project status from content (fallback if category doesn't provide status)"""
    text = (title + " " + content).lower()

    if any(kw in text for kw in ['completed', 'commissioned', 'inaugurated', 'opened', 'operational']):
        return "completed"
    elif any(kw in text for kw in ['ongoing', 'under construction', 'construction', 'underway', '% complete']):
        return "ongoing"
    elif any(kw in text for kw in ['planned', 'proposed', 'upcoming', 'approved', 'awarded', 'tender']):
        return "planned"

    return ""


def process_post(post: Dict, cat_map: Dict[int, Dict[str, str]]) -> Dict[str, Any]:
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
        if len(title) < 10 or len(summary) < 30:
            return None

        # Check if highway-related
        if not is_highway_related(title, summary):
            return None

        # Extract status from category
        categories = post.get('categories', [])
        status = ""

        if categories and cat_map:
            for cat_id in categories:
                if cat_id in cat_map:
                    cat_status = cat_map[cat_id].get('status', '')
                    if cat_status:
                        status = cat_status
                        break

        # Fallback: extract status from content
        if not status:
            status = extract_status_from_content(title, summary)

        return {
            'country': COUNTRY,
            'source': SOURCE_NAME,
            'title': title.replace(',', ' '),
            'date_iso': date_iso,
            'summary': summary.replace(',', ' '),
            'url': link,
            'category': 'highway',  # All KeNHA content is highway-related
            'status': status
        }
    except Exception as e:
        print(f"  Error processing post: {e}")
        return None


def scrape_all_posts():
    """Scrape all posts from KeNHA via WordPress API"""
    print(f"Starting scraper for {SOURCE_NAME}")
    print("="*60)

    # Step 1: Fetch category mappings
    print("\nStep 1: Fetching category mappings...")
    cat_map = fetch_categories()
    print(f"  Found {len(cat_map)} categories")

    # Show category -> status mapping
    for cat_id, cat_info in cat_map.items():
        if cat_info['status']:
            print(f"    - {cat_info['name']}: {cat_info['status']}")

    # Step 2: Get first page to determine total pages
    print("\nStep 2: Fetching posts from WordPress API...")
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
        print(f"\nStep 3: Fetching remaining {total_pages - 1} pages...")
        for page in range(2, total_pages + 1):
            print(f"  Fetching page {page}/{total_pages}...", end='\r')
            posts_data, _, _ = fetch_posts_from_api(page=page, per_page=100)
            if posts_data:
                all_posts.extend(posts_data)
            time.sleep(0.5)

    print(f"\n  Fetched {len(all_posts)} total posts")

    # Step 4: Process posts in parallel
    print(f"\nStep 4: Processing posts (filtering for highway infrastructure)...")
    all_data = []
    seen_urls = set()  # Track URLs to avoid duplicates
    seen_titles = set()  # Track titles to avoid duplicates
    skipped = 0

    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_post = {executor.submit(process_post, post, cat_map): post for post in all_posts}

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

    print(f"\n  Successfully processed {len(all_data)} highway infrastructure projects")
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

    parser = argparse.ArgumentParser(description='Scrape Kenya National Highways Authority projects')
    parser.add_argument('--output', '-o', default='kenha_data.csv', help='Output CSV file')

    args = parser.parse_args()

    # Run scraper
    data = scrape_all_posts()

    # Save to CSV
    if data:
        save_to_csv(data, args.output)
    else:
        print("No highway infrastructure projects collected!")
