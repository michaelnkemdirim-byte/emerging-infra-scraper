#!/usr/bin/env python3
"""
Scraper for Faso7 (Burkina Faso)
Website: https://faso7.com
Method: WordPress REST API with keyword search
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
API_BASE_URL = 'https://faso7.com/wp-json/wp/v2/posts'
MAX_WORKERS = 15
DATE_FILTER_DAYS = 30
DATE_AFTER = (datetime.now() - timedelta(days=DATE_FILTER_DAYS)).strftime('%Y-%m-%dT00:00:00')

# French keywords mapped to categories
KEYWORD_CATEGORY_MAP = {
    # Port category
    'port': 'port',
    'corridor': 'port',

    # Rail category
    'chemin de fer': 'rail',
    'ferroviaire': 'rail',
    'train': 'rail',

    # Highway category
    'route': 'highway',
    'autoroute': 'highway',
    'pont': 'highway',

    # Industrial zone category
    'zone industrielle': 'industrial zone',
    'parc industriel': 'industrial zone',
    'industrialisation': 'industrial zone',

    # Smart city category
    'ville intelligente': 'smart city',
    'développement urbain': 'smart city',
    'numérique': 'smart city',

    # General infrastructure
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

def has_standalone_port(text):
    """
    Check if 'port' appears as a standalone word (not embedded in other words)

    Returns True if standalone 'port' is found
    Returns False if 'port' only appears embedded in words like sport, transport, rapport
    Returns None if 'port' doesn't appear at all (ambiguous - keep for manual review)
    """
    if not text:
        return None

    text_lower = text.lower()

    # If "port" doesn't appear at all, return None (ambiguous - let it pass)
    if 'port' not in text_lower:
        return None

    # Check for standalone "port" with word boundaries
    standalone_pattern = r'\bport\b'

    if re.search(standalone_pattern, text_lower):
        return True  # Found standalone "port"

    # If we get here, "port" only appears embedded in other words
    return False

def search_faso7(keyword: str) -> list:
    """Search Faso7 WordPress API for a keyword"""
    try:
        params = {
            'search': keyword,
            'after': DATE_AFTER,
            'per_page': 100,
            '_fields': 'id,link,title'
        }

        response = requests.get(API_BASE_URL, params=params, timeout=15)
        if response.status_code != 200:
            return []

        data = response.json()
        articles = []

        for item in data:
            article_id = item.get('id')
            url = item.get('link', '')
            title_raw = item.get('title', {}).get('rendered', '')
            title = clean_html(title_raw)

            if url and title and article_id:
                articles.append({'url': url, 'link_title': title, 'article_id': article_id})

        return articles

    except Exception as e:
        print(f"  Error searching '{keyword}': {str(e)[:100]}")
        return []

def scrape_article(article_id: int, url: str, link_title: str) -> dict:
    """Scrape individual article from Faso7"""
    try:
        # Fetch full article details from API using article ID
        article_url = f"https://faso7.com/wp-json/wp/v2/posts/{article_id}?_fields=id,title,date,link,excerpt,content"

        response = requests.get(article_url, timeout=15)
        if response.status_code != 200:
            return None

        data = response.json()

        # Parse date
        date_str = data.get('date', '')
        date_iso = date_str.split('T')[0] if 'T' in date_str else ''

        # Clean title
        title_raw = data.get('title', {}).get('rendered', '')
        title = clean_html(title_raw)

        # Clean excerpt/content for summary
        excerpt_raw = data.get('excerpt', {}).get('rendered', '')
        content_raw = data.get('content', {}).get('rendered', '')

        # Prefer excerpt, fall back to content
        if excerpt_raw:
            summary = clean_html(excerpt_raw)
        else:
            summary = clean_html(content_raw)

        if len(summary) > 500:
            summary = summary[:497] + '...'

        return {
            'url': data.get('link', url),
            'title': title if title else link_title,
            'date_published': date_iso,
            'summary': summary
        }

    except Exception as e:
        return None

def scrape_faso7():
    """Main scraping function for Faso7"""
    print("="*70)
    print("Scraping Faso7 (Burkina Faso)")
    print("="*70)
    print(f"Using {MAX_WORKERS} parallel workers")
    print(f"Date range: Last {DATE_FILTER_DAYS} days (after {DATE_AFTER.split('T')[0]})")
    print()

    # PHASE 1: Search all keywords and collect URLs
    print("PHASE 1: Searching all keywords...")
    print("-" * 70)

    url_to_category = {}
    total_found = 0

    for keyword, category in KEYWORD_CATEGORY_MAP.items():
        print(f"  [{keyword}] → {category}...", end=" ")
        articles = search_faso7(keyword)
        print(f"found {len(articles)}")
        total_found += len(articles)

        # Map URLs to categories (first occurrence wins)
        for article in articles:
            url = article['url']
            if url not in url_to_category:
                url_to_category[url] = {
                    'category': category,
                    'link_title': article['link_title'],
                    'article_id': article['article_id']
                }

    unique_urls = list(url_to_category.keys())

    print("-" * 70)
    print(f"Search complete: {total_found} total, {len(unique_urls)} unique URLs")
    print()

    # PHASE 2: Scrape all unique articles in parallel
    print("PHASE 2: Scraping all articles in parallel...")
    print("-" * 70)

    articles_to_scrape = [
        {'url': url, 'link_title': info['link_title'], 'category': info['category'], 'article_id': info['article_id']}
        for url, info in url_to_category.items()
    ]

    all_data = []

    # Scrape with progress bar
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_article = {
            executor.submit(scrape_article, article['article_id'], article['url'], article['link_title']): article
            for article in articles_to_scrape
        }

        with tqdm(total=len(articles_to_scrape), desc="Scraping", unit="article") as pbar:
            for future in as_completed(future_to_article):
                article_info = future_to_article[future]
                try:
                    details = future.result()
                    if details:
                        article = {
                            'country': 'Burkina Faso',
                            'source': 'Faso7',
                            'title': details['title'],
                            'date_iso': details['date_published'],
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
    print(f"  Articles collected: {len(all_data)}")
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

    # PHASE 3: Filter out false "port" matches
    print()
    print("PHASE 3: Filtering false 'port' matches...")
    print("-" * 70)

    filtered_data = []
    false_matches = 0

    for article in final_data:
        # Only filter "port" category articles
        if article['category'] == 'port':
            full_text = f"{article['title']} {article['summary']}"
            result = has_standalone_port(full_text)

            if result is False:
                # False match - "port" only in embedded words like sport, transport
                false_matches += 1
                continue  # Skip this article

        # Keep all other articles and legitimate/ambiguous port matches
        filtered_data.append(article)

    if false_matches > 0:
        print(f"  Removed {false_matches} false 'port' matches (embedded in sport/transport/rapport/etc)")
        print(f"  Kept {len(filtered_data)} articles ({len(final_data) - false_matches} total - {false_matches} false matches)")

    print("="*70)

    return filtered_data

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
    parser = argparse.ArgumentParser(description='Scrape Faso7 for infrastructure news')
    parser.add_argument('--output', default='faso7_data.csv', help='Output CSV file')
    parser.add_argument('--workers', type=int, default=15, help='Parallel workers (default: 15)')
    args = parser.parse_args()

    MAX_WORKERS = args.workers
    data = scrape_faso7()
    save_to_csv(data, args.output)
