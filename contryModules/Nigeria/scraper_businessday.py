#!/usr/bin/env python3
"""
Scraper for BusinessDay Nigeria
Website: https://businessday.ng
Type: WordPress news site
Focus: Business, economy, infrastructure, transport, energy
"""

import csv
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from html import unescape
import time
import argparse
import json
import pycurl
import certifi
from io import BytesIO

# Configuration
BASE_URL = 'https://businessday.ng'
WP_API_URL = f'{BASE_URL}/wp-json/wp/v2/posts'

# Date filter: last 7 days
DATE_7_DAYS_AGO = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%S')
DATE_THRESHOLD = datetime.now() - timedelta(days=7)

# Infrastructure keywords
SEARCH_KEYWORDS = [
    # Infrastructure - Transportation
    'infrastructure',
    'construction',
    'road',
    'highway',
    'expressway',
    'railway',
    'port',
    'airport',
    'terminal',
    'runway',
    'bridge',

    # Infrastructure - Utilities
    'water supply',
    'sanitation',
    'wastewater',
    'sewage',
    'water treatment',
    'waste management',
    'recycling',

    # Infrastructure - Development
    'smart city',
    'sez',
    'special economic zone',
    'industrial park',

    # Economic
    'investment',
    'finance',
    'trade',
    'export',
    'import',
    'cryptocurrency',
    'crypto',
    'blockchain',
    'fintech',
    'banking',
    'economy',

    # Energy
    'power',
    'electricity',
    'energy',
    'solar',
    'renewable',
    'thermal power',
    'nuclear',
    'wind power',
    'hydroelectric',

    # Technology
    '5g',
    'broadband',
    'fiber',
    'internet',
    'telecom',
    'ai',
    'artificial intelligence',
    'digital',
    'cybersecurity',
    'data center'
]

def clean_html(raw_html):
    """Remove HTML tags and clean text"""
    if not raw_html:
        return ""
    soup = BeautifulSoup(raw_html, 'html.parser')
    text = soup.get_text(separator=' ', strip=True)
    return unescape(text)

def fetch_posts_from_api(page: int = 1, per_page: int = 100) -> tuple:
    """
    Fetch posts from WordPress API using pycurl (bypasses Cloudflare detection of requests library)
    Returns: (list of posts, total pages)
    Note: Uses smaller per_page (20) and delays to avoid Cloudflare rate limiting
    """
    # Use smaller per_page to reduce Cloudflare sensitivity
    per_page = min(per_page, 20)

    # Build URL with parameters
    url = f"{WP_API_URL}?page={page}&per_page={per_page}&after={DATE_7_DAYS_AGO}&_embed=1"

    try:
        buffer = BytesIO()
        headers_buffer = BytesIO()

        c = pycurl.Curl()
        c.setopt(c.URL, url)
        c.setopt(c.WRITEDATA, buffer)
        c.setopt(c.HEADERFUNCTION, headers_buffer.write)
        c.setopt(c.USERAGENT, 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36')
        c.setopt(c.CAINFO, certifi.where())
        c.setopt(c.HTTP_VERSION, pycurl.CURL_HTTP_VERSION_1_1)
        c.setopt(c.TIMEOUT, 30)

        c.perform()
        status_code = c.getinfo(c.RESPONSE_CODE)
        c.close()

        if status_code != 200:
            if status_code == 403:
                print(f"  Warning: Cloudflare rate limit hit (403). Wait a few minutes before retrying.")
            print(f"  Error: HTTP {status_code} for page {page}")
            return [], 0

        # Parse headers to get total pages
        headers_text = headers_buffer.getvalue().decode('utf-8')
        total_pages = 1
        for line in headers_text.split('\n'):
            if line.lower().startswith('x-wp-totalpages:'):
                total_pages = int(line.split(':', 1)[1].strip())
                break

        # Parse JSON body
        body = buffer.getvalue()
        posts = json.loads(body.decode('utf-8'))

        return posts, total_pages

    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse JSON for page {page}: {e}")
        return [], 0
    except Exception as e:
        print(f"Error fetching page {page}: {e}")
        return [], 0

def determine_category(title, summary):
    """Determine category based on keywords in title and summary"""
    text = (title + ' ' + summary).lower()

    if any(kw in text for kw in ['airport', 'terminal', 'runway', 'aviation']):
        return 'airport'
    elif any(kw in text for kw in ['railway', 'rail', 'train', 'metro']):
        return 'rail'
    elif any(kw in text for kw in ['port', 'harbor', 'harbour', 'maritime', 'shipping']):
        return 'port'
    elif any(kw in text for kw in ['road', 'highway', 'expressway', 'bridge']):
        return 'highway'
    elif any(kw in text for kw in ['water supply', 'sanitation', 'wastewater', 'sewage', 'water treatment']):
        return 'water'
    elif any(kw in text for kw in ['waste management', 'recycling', 'landfill']):
        return 'waste'
    elif any(kw in text for kw in ['smart city', 'digital city']):
        return 'smart city'
    elif any(kw in text for kw in ['industrial park', 'sez', 'special economic zone']):
        return 'SEZ'
    elif any(kw in text for kw in ['power', 'electricity', 'energy', 'solar', 'renewable', 'thermal power', 'nuclear', 'wind power', 'hydroelectric']):
        return 'energy'
    elif any(kw in text for kw in ['5g', 'broadband', 'fiber', 'internet', 'telecom', 'digital infrastructure']):
        return 'telecom'
    elif any(kw in text for kw in ['investment', 'finance', 'trade', 'export', 'import', 'cryptocurrency', 'crypto', 'blockchain', 'fintech', 'banking', 'economy']):
        return 'economic'
    elif any(kw in text for kw in ['ai', 'artificial intelligence', 'cybersecurity', 'data center', 'technology']):
        return 'technology'
    else:
        return 'infrastructure'

def is_relevant_article(title, summary):
    """Check if article contains relevant keywords"""
    text = (title + ' ' + summary).lower()
    return any(keyword.lower() in text for keyword in SEARCH_KEYWORDS)

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

        # Check relevance
        if not is_relevant_article(title, excerpt):
            return None

        # Source
        source = 'BusinessDay'

        # Category
        category = determine_category(title, excerpt)

        return {
            'country': 'Nigeria',
            'source': source,
            'title': title,
            'date_iso': date_published,
            'summary': excerpt,
            'url': url,
            'category': category,
        }

    except Exception as e:
        print(f"Error extracting article data: {e}")
        return None

def scrape_businessday():
    """Main scraping function for BusinessDay"""
    print("="*70)
    print("Scraping BusinessDay Nigeria (businessday.ng)")
    print("="*70)
    print(f"Fetching posts from last 7 days (after {DATE_7_DAYS_AGO[:10]})")
    print()

    all_data = []
    seen_urls = set()
    seen_titles = set()
    seen_titles = set()
    page = 1

    while True:
        print(f"Fetching page {page}...")
        posts, total_pages = fetch_posts_from_api(page)

        if not posts:
            break

        # Add delay between requests to avoid Cloudflare rate limiting
        if page > 1:
            time.sleep(3)

        for post in posts:
            article = extract_article_data(post)
            if article and article['title'] and article['url']:
                # Deduplicate by URL and title
                url = article['url']
                title = article['title']
                if url in seen_urls or title in seen_titles:
                    continue

                # Client-side date validation
                if article.get('date_iso'):
                    try:
                        article_date = datetime.strptime(article['date_iso'], '%Y-%m-%d')
                        if article_date >= DATE_THRESHOLD:
                            seen_urls.add(url)
                            seen_titles.add(title)
                            all_data.append(article)
                            print(f"  ✓ {article['date_iso']}: {article['title'][:70]}")
                        else:
                            print(f"  ✗ Skipping old article ({article['date_iso']}): {article['title'][:60]}")
                    except:
                        seen_urls.add(url)
                        seen_titles.add(title)
                        all_data.append(article)
                        print(f"  ⚠ {article['date_iso']}: {article['title'][:70]}")
                else:
                    seen_urls.add(url)
                    seen_titles.add(title)
                    all_data.append(article)
                    print(f"  ⚠ No date: {article['title'][:70]}")

        if page >= total_pages:
            break

        page += 1
        time.sleep(1)

    print()
    print(f"Total articles collected: {len(all_data)}")

    return all_data

def save_to_csv(data, output_file):
    """Save data to CSV file"""
    if not data:
        print("No data to save!")
        return

    fieldnames = ['country', 'source', 'title', 'date_iso', 'summary', 'url', 'category']

    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    print(f"\n✓ Data saved to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape BusinessDay for infrastructure news')
    parser.add_argument('--output', default='businessday_data.csv', help='Output CSV file')
    args = parser.parse_args()

    data = scrape_businessday()
    save_to_csv(data, args.output)
