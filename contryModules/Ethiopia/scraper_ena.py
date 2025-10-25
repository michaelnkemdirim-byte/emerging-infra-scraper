#!/usr/bin/env python3
"""
Scraper for Ethiopian News Agency (ENA)
Website: https://www.ena.et/web/eng/search
Method: Parallel HTTP search with category mapping
"""

import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime, timedelta
from html import unescape
import argparse
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Configuration
SEARCH_BASE_URL = 'https://www.ena.et/web/eng/search'
MAX_WORKERS = 15  # Parallel workers

# Date filter: last 90 days (ENA has limited recent infrastructure news)
DATE_30_DAYS_AGO = datetime.now() - timedelta(days=30)

# Search keywords mapped to categories
KEYWORD_CATEGORY_MAP = {
    # Port category
    'port': 'port',

    # Rail category
    'railway': 'rail',

    # Highway category
    'road': 'highway',
    'highway': 'highway',
    'expressway': 'highway',
    'bridge': 'highway',

    # Industrial zone category
    'industrial park': 'industrial zone',
    'industrial zone': 'industrial zone',
    'SEZ': 'industrial zone',

    # Smart city category
    'smart city': 'smart city',
    'urban development': 'smart city',
    'corridor': 'smart city',

    # General infrastructure (will be categorized by AI)
    'infrastructure': 'Infrastructure',
    'construction': 'Infrastructure',
    'transport': 'Infrastructure',
}

def clean_html(raw_html):
    """Remove HTML tags and clean text"""
    if not raw_html:
        return ""
    soup = BeautifulSoup(raw_html, 'html.parser')
    text = soup.get_text(separator=' ', strip=True)
    return unescape(text)

def parse_date(date_str: str) -> str:
    """Parse ENA date format to YYYY-MM-DD"""
    if not date_str:
        return ''

    try:
        # Primary format: "April 25, 2025" (comma-separated)
        date_match = re.search(r'([A-Z][a-z]+)\s+(\d{1,2}),?\s+(\d{4})', date_str)
        if date_match:
            month_name, day, year = date_match.groups()
            date_obj = datetime.strptime(f'{month_name} {day} {year}', '%B %d %Y')
            return date_obj.strftime('%Y-%m-%d')

        return ''
    except:
        return ''

def search_ena(keyword: str) -> list:
    """Search ENA for a keyword and return article URLs"""
    url = f'{SEARCH_BASE_URL}?q={keyword}'

    try:
        response = requests.get(url, timeout=15)
        if response.status_code != 200:
            return []

        soup = BeautifulSoup(response.content, 'html.parser')
        article_links = soup.find_all('a', href=lambda x: x and '/eng/w/eng_' in x)

        articles = []
        for link in article_links:
            href = link.get('href')
            title = link.get_text(strip=True)

            # Clean up URL
            if '?' in href:
                href = href.split('?')[0]

            # Make absolute URL
            if href.startswith('/'):
                href = 'https://www.ena.et' + href

            if href and title and len(title) > 10:
                articles.append({'url': href, 'link_title': title})

        return articles

    except Exception as e:
        print(f"  Error searching '{keyword}': {str(e)[:100]}")
        return []

def scrape_article(url: str, link_title: str) -> dict:
    """Scrape individual article page"""
    try:
        response = requests.get(url, timeout=15)
        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract title
        title_elem = soup.find('h1')
        title = title_elem.get_text(strip=True) if title_elem else link_title

        # Extract date from full text
        full_text = soup.get_text()

        # Look for date pattern in first 1500 characters
        # Replace non-breaking spaces with regular spaces for easier regex matching
        text_clean = full_text[:1500].replace('\xa0', ' ').replace('\u00a0', ' ')

        date_str = ''
        # Primary format: "Addis Ababa, April 25, 2025" or just "April 25, 2025"
        # Make "Addis Ababa" prefix optional
        date_match = re.search(r'(?:Addis\s+Ababa[,\s]+)?([A-Z][a-z]+)\s+(\d{1,2})[,\s]+(\d{4})', text_clean)
        if date_match:
            month_name, day, year = date_match.groups()
            date_str = f'{month_name} {day}, {year}'
        else:
            # Alternative format: "June13/2024" (slash-separated, compact)
            date_match = re.search(r'([A-Z][a-z]+)\s*(\d{1,2})/(\d{4})', text_clean)
            if date_match:
                date_str = f'{date_match.group(1)} {date_match.group(2)}, {date_match.group(3)}'

        date_published = parse_date(date_str)

        # Extract content - find the main article content div
        content_elem = soup.find('div', class_='journal-content-article')
        if not content_elem:
            content_elem = soup.find('article')
        if not content_elem:
            content_elem = soup.find('div', class_=re.compile('content', re.I))

        summary = ''
        if content_elem:
            # Remove navigation, script, style tags
            for unwanted in content_elem.find_all(['script', 'style', 'nav', 'header', 'footer']):
                unwanted.decompose()

            text = content_elem.get_text(separator=' ', strip=True)

            # Remove common navigation text patterns
            text = re.sub(r'Live:.*?English Français.*?About Us', '', text)
            text = re.sub(r'Login Logout.*?About Us', '', text)
            text = re.sub(r'🔇.*?Unmute', '', text)

            # Clean up multiple spaces
            text = re.sub(r'\s+', ' ', text).strip()

            summary = text[:500] + '...' if len(text) > 500 else text

        return {
            'url': url,
            'title': title,
            'date_published': date_published,
            'summary': summary
        }

    except Exception as e:
        return None

def scrape_ena():
    """Main scraping function for ENA"""
    print("="*70)
    print("Scraping Ethiopian News Agency (ENA)")
    print("="*70)
    print(f"Using {MAX_WORKERS} parallel workers")
    print(f"Date range: Last 90 days (after {DATE_30_DAYS_AGO.strftime('%Y-%m-%d')})")
    print()

    # PHASE 1: Search all keywords and collect URLs
    print("PHASE 1: Searching all keywords...")
    print("-" * 70)

    url_to_category = {}  # Map URL to category
    seen_urls = set()
    total_found = 0

    for keyword, category in KEYWORD_CATEGORY_MAP.items():
        print(f"  [{keyword}] → {category}...", end=" ")
        articles = search_ena(keyword)
        print(f"found {len(articles)}")
        total_found += len(articles)

        # Map URLs to categories (first occurrence wins)
        for article in articles:
            url = article['url']
            if url not in url_to_category:
                url_to_category[url] = {
                    'category': category,
                    'link_title': article['link_title']
                }

    unique_urls = list(url_to_category.keys())

    print("-" * 70)
    print(f"Search complete: {total_found} total, {len(unique_urls)} unique URLs")
    print()

    # PHASE 2: Scrape all unique articles in parallel
    print("PHASE 2: Scraping all articles in parallel...")
    print("-" * 70)

    articles_to_scrape = [
        {'url': url, 'link_title': info['link_title'], 'category': info['category']}
        for url, info in url_to_category.items()
    ]

    all_data = []
    seen_urls = set()  # Track URLs to avoid duplicates
    seen_titles = set()  # Track titles to avoid duplicates

    # Scrape with progress bar
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_article = {
            executor.submit(scrape_article, article['url'], article['link_title']): article
            for article in articles_to_scrape
        }

        with tqdm(total=len(articles_to_scrape), desc="Scraping", unit="article") as pbar:
            for future in as_completed(future_to_article):
                article_info = future_to_article[future]
                try:
                    details = future.result()
                    if details:
                        date_published = details['date_published']

                        # Date filter
                        if date_published:
                            try:
                                article_date = datetime.strptime(date_published, '%Y-%m-%d')
                                if article_date < DATE_30_DAYS_AGO:
                                    pbar.update(1)
                                    continue
                            except:
                                pass

                        # Deduplicate by URL and title
                        if details['url'] not in seen_urls and details['title'] not in seen_titles:
                            seen_urls.add(details['url'])
                            seen_titles.add(details['title'])

                            article = {
                                'country': 'Ethiopia',
                                'source': 'Ethiopian News Agency (ENA)',
                                'title': details['title'],
                                'date_iso': date_published,
                                'summary': details['summary'],
                                'url': details['url'],
                                'category': article_info['category'],
                                'status': ''
                            }

                            all_data.append(article)
                except Exception as e:
                    pass

                pbar.update(1)

    print()
    print("="*70)
    print(f"SUMMARY:")
    print(f"  Total URLs found from searches: {total_found}")
    print(f"  Unique URLs scraped: {len(unique_urls)}")
    print(f"  Articles collected (after date filter): {len(all_data)}")
    print("="*70)

    # Deduplicate by URL (final safety check)
    unique_data = {}
    for article in all_data:
        url = article['url']
        if url not in unique_data:
            unique_data[url] = article

    final_data = list(unique_data.values())
    if len(final_data) < len(all_data):
        print(f"Removed {len(all_data) - len(final_data)} duplicates")

    return final_data

def save_to_csv(data, output_file):
    """Save data to CSV file"""
    if not data:
        print("No data to save!")
        return

    fieldnames = ['country', 'source', 'title', 'date_iso', 'summary', 'url', 'category', 'status']

    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    print(f"\n✓ Data saved to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape ENA for infrastructure news')
    parser.add_argument('--output', default='ena_data.csv', help='Output CSV file')
    parser.add_argument('--workers', type=int, default=15, help='Parallel workers (default: 15)')
    args = parser.parse_args()

    MAX_WORKERS = args.workers
    data = scrape_ena()
    save_to_csv(data, args.output)
