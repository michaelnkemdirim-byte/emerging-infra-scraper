#!/usr/bin/env python3
"""
Bureau of Public Enterprises (BPE) Nigeria - Infrastructure Scraper
Scrapes infrastructure-related privatization and development news
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
BASE_URL = "https://www.bpe.gov.ng"
API_URL = f"{BASE_URL}/wp-json/wp/v2/posts"
COUNTRY = "Nigeria"
SOURCE_NAME = "Bureau of Public Enterprises"

# Date filtering - Last 7 days only
DATE_FILTER_DAYS = 7
DATE_AFTER = (datetime.now() - timedelta(days=DATE_FILTER_DAYS)).strftime('%Y-%m-%dT00:00:00')

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json"
}

# Infrastructure keywords
INFRASTRUCTURE_KEYWORDS = [
    # Infrastructure
    'infrastructure', 'port', 'railway', 'rail', 'road', 'highway', 'bridge',
    'water', 'housing', 'sez', 'industrial park', 'airport', 'aviation',
    'free zone', 'smart city', 'construction', 'development project',
    # Energy
    'power', 'electricity', 'energy', 'dam', 'solar', 'wind farm', 'hydropower',
    'renewable energy', 'geothermal', 'nuclear', 'thermal', 'grid', 'substation',
    # Economic
    'finance', 'trade', 'economic', 'crypto', 'investment', 'fintech',
    'stock exchange', 'banking', 'commerce', 'export', 'capital market',
    'privatization', 'privatisation',
    # Technology
    'telecom', 'telecommunications', 'digital', 'technology', 'broadband', '5G',
    'data center', 'e-government', 'ICT', 'fiber optic', 'AI', 'cybersecurity',
    'internet'
]

# Exclude non-project content
EXCLUDE_PATTERNS = [
    # HR/recruitment
    'recruitment', 'vacancy', 'job opportunity', 'career',
    'employment disclaimer',
    # Events/training/summits
    'management retreat', 'workshop', 'training',
    'summit', 'conference', 'forum',
    # People/Opinion pieces
    'condolence', 'obituary', 'biography',
    'appointment', 'promotion', 'resignation',
    'present post', 'brief background', 'was born', 'holds a bachelors', 'holds a masters',
    "dg's corner", 'dg corner', 'director general corner',
    'director general of the bureau', 'director general at',
    # Generic pages/announcements
    'investor guide 2006', "investor's guide",
    'why privatisation', 'what is the case for', 'case for privatisation',
    'health sector reform', 'sector reform objectives',
    'promises full support', 'pledges support'
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
    """Check if content is infrastructure-related"""
    text = (title + " " + content).lower()

    # Must have infrastructure keywords
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

    if any(kw in text for kw in ['commissioned', 'completed', 'inaugurated', 'opened', 'operational']):
        return "completed"
    elif any(kw in text for kw in ['ongoing', 'under construction', 'construction', 'developing', 'implementation']):
        return "ongoing"
    elif any(kw in text for kw in ['planned', 'proposed', 'approval', 'approved', 'awarded', 'rfp', 'eoi', 'tender']):
        return "planned"

    return ""


def determine_category(title: str, content: str) -> str:
    """Determine infrastructure category with improved accuracy"""
    text = (title + " " + content).lower()

    # Priority 1: Check power/electricity FIRST (most common in BPE)
    # This prevents "Port Harcourt DisCo" from being categorized as "port"
    if any(kw in text for kw in ['disco', 'discos', 'electricity distribution', 'genco', 'power plant',
                                   'electricity', 'power sector', 'energy sector', 'hydroelectric',
                                   'power project', 'nipp', 'siemens']):
        return "infrastructure"

    # Priority 2: Oil & Gas (prevent "rail" matching in "Kaduna Refining")
    if any(kw in text for kw in ['refining', 'refinery', 'petrochemical', 'nnpc', 'oil and gas']):
        return "infrastructure"

    # Priority 3: Aviation (prevent "highway" matching in "roadshow")
    if any(kw in text for kw in ['aviation', 'airport', 'airline', 'skyway']):
        return "infrastructure"

    # Priority 4: Actual transport infrastructure (with better word boundaries)
    # Port - but exclude "Port Harcourt" city name unless it's actually about a port
    if any(kw in text for kw in ['seaport', 'sea port', 'harbour', 'maritime', 'shipping', 'inland waterways', 'waterway']):
        return "port"
    elif ' port ' in text or text.startswith('port ') or text.endswith(' port'):
        # More specific port matching (word boundary)
        if 'port harcourt disco' not in text and 'port harcourt electricity' not in text:
            return "port"

    # Railway - use word boundaries to avoid matching "trail", "trailer"
    if any(kw in text for kw in [' railway', 'railway ', 'rail line', 'rail transport', 'rail project', 'train', 'metro', 'light rail']):
        return "rail"

    # Highway - use word boundaries to avoid "roadshow", "road map"
    # Only match actual road infrastructure terms
    if any(kw in text for kw in ['highway', 'expressway', 'bridge construction', ' road construction',
                                   'road infrastructure', 'road project', 'road maintenance', 'federal road']):
        return "highway"

    # SEZ and Smart Cities
    if any(kw in text for kw in ['sez', 'special economic zone', 'industrial park', 'free trade zone', 'free zone']):
        return "SEZ"
    elif any(kw in text for kw in ['smart city', 'digital city']):
        return "smart city"

    # Default: General infrastructure (power, water, telecom, housing, etc.)
    return "infrastructure"


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

        # Extract status and category
        status = extract_status(title, summary)
        category = determine_category(title, summary)

        return {
            'country': COUNTRY,
            'source': SOURCE_NAME,
            'title': title.replace(',', ' '),
            'date_iso': date_iso,
            'summary': summary.replace(',', ' '),
            'url': link,
            'category': category,
        }
    except Exception as e:
        print(f"  Error processing post: {e}")
        return None


def scrape_all_posts():
    """Scrape all posts from BPE via WordPress API"""
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
    print(f"\nStep 3: Processing posts (filtering for infrastructure)...")
    all_data = []
    seen_urls = set()  # Track URLs to avoid duplicates
    seen_titles = set()
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

    print(f"\n  Successfully processed {len(all_data)} infrastructure articles")
    print(f"  Filtered out: {skipped} (non-infrastructure content)")

    return all_data


def save_to_csv(data: List[Dict], output_file: str):
    """Save data to CSV file"""
    if not data:
        print("No data to save")
        return

    fieldnames = ['country', 'source', 'title', 'date_iso', 'summary', 'url', 'category']

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

    parser = argparse.ArgumentParser(description='Scrape Bureau of Public Enterprises Nigeria')
    parser.add_argument('--output', '-o', default='bpe_data.csv', help='Output CSV file')

    args = parser.parse_args()

    # Run scraper
    data = scrape_all_posts()

    # Save to CSV
    if data:
        save_to_csv(data, args.output)
    else:
        print("No infrastructure articles collected!")
