#!/usr/bin/env python3
"""
Ghana News Agency - Economic & Business News Scraper
Scrapes economic, finance, trade, and business news from Ghana
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
BASE_URL = "https://gna.org.gh"
API_URL = f"{BASE_URL}/wp-json/wp/v2/posts"
COUNTRY = "Ghana"
SOURCE_NAME = "Ghana News Agency"

# Date filtering - Last 7 days only
DATE_FILTER_DAYS = 7
DATE_AFTER = (datetime.now() - timedelta(days=DATE_FILTER_DAYS)).strftime('%Y-%m-%dT00:00:00')
DATE_THRESHOLD = datetime.now() - timedelta(days=DATE_FILTER_DAYS)  # For client-side validation

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}

# Economic keywords
ECONOMIC_KEYWORDS = [
    # Finance & Banking
    'finance', 'banking', 'bank', 'loan', 'credit', 'debt', 'mortgage',
    'stock market', 'stock exchange', 'ghana stock exchange', 'gse',
    'capital market', 'securities', 'bonds', 'treasury', 'forex',
    # Trade & Commerce
    'trade', 'export', 'import', 'commerce', 'business', 'merchant',
    'trading', 'customs', 'tariff', 'trade agreement', 'free trade',
    # Economy & Policy
    'economic', 'economy', 'gdp', 'inflation', 'deflation', 'recession',
    'monetary policy', 'fiscal policy', 'budget', 'taxation', 'tax', 'revenue',
    'bank of ghana', 'central bank', 'ministry of finance', 'mofep',
    # Investment
    'investment', 'investor', 'venture capital', 'private equity',
    'foreign direct investment', 'fdi', 'portfolio', 'equity', 'shares',
    # Technology & Finance
    'fintech', 'mobile money', 'digital payment', 'e-commerce',
    'blockchain', 'cryptocurrency', 'crypto', 'bitcoin',
    # Corporate
    'merger', 'acquisition', 'ipo', 'listing', 'corporate', 'enterprise',
    'profit', 'revenue', 'earnings', 'dividend', 'shareholder',
    # Energy (related to economic impact)
    'power', 'electricity', 'energy', 'oil', 'gas', 'petroleum', 'fuel',
    'solar', 'renewable energy', 'wind', 'hydro',
    # Technology (related to economic impact)
    'technology', 'digital', 'telecommunications', 'telecom', '5g', 'broadband',
    'data center', 'ict', 'innovation', 'startup', 'tech hub'
]

# Exclude non-relevant content
EXCLUDE_PATTERNS = [
    'obituary', 'condolence', 'funeral', 'death announcement',
    'birthday', 'wedding', 'engagement',
    'photo gallery', 'pictures', 'photos only',
    'sports score', 'match result', 'football league',
    'celebrity gossip', 'entertainment news'
]


def fetch_posts_from_api(page: int = 1, per_page: int = 100) -> tuple:
    """Fetch posts from WordPress API"""
    try:
        params = {
            'page': page,
            'per_page': per_page,
            '_embed': 1,
            'after': DATE_AFTER
        }

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


def is_relevant_content(title: str, content: str) -> bool:
    """Check if content is relevant to economic/energy/technology news"""
    text = (title + " " + content).lower()

    # Must have economic keywords
    has_keywords = any(keyword in text for keyword in ECONOMIC_KEYWORDS)
    if not has_keywords:
        return False

    # Exclude non-relevant patterns
    if any(pattern in text for pattern in EXCLUDE_PATTERNS):
        return False

    return True


def extract_status(title: str, content: str) -> str:
    """Extract project/initiative status"""
    text = (title + " " + content).lower()

    if any(kw in text for kw in ['launched', 'opened', 'completed', 'inaugurated', 'commissioned']):
        return "completed"
    elif any(kw in text for kw in ['ongoing', 'implementing', 'developing', 'in progress', 'underway']):
        return "ongoing"
    elif any(kw in text for kw in ['planned', 'proposed', 'upcoming', 'to be', 'will', 'plans to', 'announced']):
        return "planned"

    return ""


def determine_category(title: str, content: str) -> str:
    """Determine article category"""
    text = (title + " " + content).lower()

    # Check for infrastructure-specific categories (ordered by specificity)

    # Transportation infrastructure
    if any(kw in text for kw in ['port', 'harbour', 'harbor', 'maritime', 'shipping', 'seaport', 'wharf', 'dock']):
        return "port"
    elif any(kw in text for kw in ['airport', 'aviation', 'airstrip', 'terminal', 'runway', 'airline']):
        return "airport"
    elif any(kw in text for kw in ['railway', 'rail', 'train', 'metro', 'tram', 'locomotive', 'track']):
        return "rail"
    elif any(kw in text for kw in ['road', 'highway', 'bridge', 'bypass', 'expressway', 'motorway', 'interchange', 'flyover']):
        return "highway"

    # Economic zones & urban development
    elif any(kw in text for kw in ['special economic zone', 'sez', 'industrial park', 'free zone', 'export processing zone', 'epz', 'industrial estate']):
        return "SEZ"
    elif any(kw in text for kw in ['smart city', 'digital city', 'tech city', 'innovation district', 'tech hub', 'tech park']):
        return "smart city"

    # Utilities & services
    elif any(kw in text for kw in ['water supply', 'water treatment', 'sewage', 'sanitation', 'wastewater', 'desalination', 'pipeline', 'reservoir']):
        return "water"
    elif any(kw in text for kw in ['waste management', 'landfill', 'recycling', 'waste disposal', 'garbage', 'refuse']):
        return "waste"

    # Energy infrastructure (keep as general infrastructure category)
    elif any(kw in text for kw in ['power plant', 'solar', 'wind farm', 'hydro', 'dam', 'electricity', 'grid', 'substation', 'renewable energy', 'nuclear', 'thermal']):
        return "energy"

    # Technology infrastructure (keep as general infrastructure category)
    elif any(kw in text for kw in ['5g', '4g', 'broadband', 'fiber optic', 'fibre optic', 'data center', 'telecom tower', 'telecommunications', 'mobile network', 'internet']):
        return "telecom"

    else:
        # General infrastructure or economic development
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

        # Check if relevant to economic/energy/technology
        if not is_relevant_content(title, summary):
            return None

        # Extract status and category
        status = extract_status(title, summary)
        category = determine_category(title, summary)

        return {
            'country': COUNTRY,
            'source': SOURCE_NAME,
            'title': title.replace(',', ' ').replace('\n', ' '),
            'date_iso': date_iso,
            'summary': summary.replace(',', ' ').replace('\n', ' '),
            'url': link,
            'category': category,
            'status': status
        }
    except Exception as e:
        print(f"  Error processing post: {e}")
        return None


def scrape_all_posts():
    """Scrape all posts from Ghana News Agency via WordPress API"""
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

    # Limit to first 10 pages max (1000 posts) to avoid excessive scraping
    max_pages = min(total_pages, 10)
    if max_pages < total_pages:
        print(f"  NOTE: Limiting to first {max_pages} pages (out of {total_pages}) for performance")

    # Collect all posts
    all_posts = posts_data

    # Fetch remaining pages
    if max_pages > 1:
        print(f"\nStep 2: Fetching remaining {max_pages - 1} pages...")
        for page in range(2, max_pages + 1):
            print(f"  Fetching page {page}/{max_pages}...", end='\r')
            posts_data, _, _ = fetch_posts_from_api(page=page, per_page=100)
            if posts_data:
                all_posts.extend(posts_data)
            time.sleep(0.5)

    print(f"\n  Fetched {len(all_posts)} total posts")

    # Step 3: Process posts in parallel
    print(f"\nStep 3: Processing posts (filtering for economic/energy/technology news)...")
    all_data = []
    seen_urls = set()
    seen_titles = set()
    skipped = 0

    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_post = {executor.submit(process_post, post): post for post in all_posts}

        completed = 0
        for future in as_completed(future_to_post):
            completed += 1

            try:
                result = future.result()
                if result:
                    # Client-side date validation - skip if older than 7 days
                    if result.get('date_iso'):
                        try:
                            article_date = datetime.strptime(result['date_iso'], '%Y-%m-%d')
                            if article_date < DATE_THRESHOLD:
                                skipped += 1
                                continue
                        except:
                            pass  # If date parsing fails, include for manual review

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

    print(f"\n  Successfully processed {len(all_data)} relevant articles")
    print(f"  Filtered out: {skipped} (non-relevant content)")

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

    parser = argparse.ArgumentParser(description='Scrape Ghana News Agency')
    parser.add_argument('--output', '-o', default='gna_data.csv', help='Output CSV file')

    args = parser.parse_args()

    # Run scraper
    data = scrape_all_posts()

    # Save to CSV
    if data:
        save_to_csv(data, args.output)
    else:
        print("No relevant articles collected!")
