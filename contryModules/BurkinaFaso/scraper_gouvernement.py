#!/usr/bin/env python3
"""
Scraper for Gouvernement.gov.bf (Burkina Faso)
Website: https://gouvernement.gov.bf
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
API_BASE_URL = 'https://gouvernement.gov.bf/wp-json/wp/v2/posts'
MAX_WORKERS = 15
DATE_FILTER_DAYS = 7
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

    # Economic category
    'finance': 'economic',
    'commerce': 'economic',
    'économique': 'economic',
    'crypto': 'economic',
    'investissement': 'economic',
    'fintech': 'economic',
    'bourse': 'economic',
    'marché financier': 'economic',
    'bancaire': 'economic',
    'exportation': 'economic',

    # Energy category
    'solaire': 'energy',
    'éolien': 'energy',
    'hydroélectrique': 'energy',
    'centrale électrique': 'energy',
    'électricité': 'energy',
    'énergie renouvelable': 'energy',
    'géothermie': 'energy',
    'nucléaire': 'energy',
    'thermique': 'energy',
    'barrage': 'energy',

    # Technology category
    'technologie': 'technology',
    'digital': 'technology',
    'internet': 'technology',
    'fibre optique': 'technology',
    '5G': 'technology',
    'centre de données': 'technology',
    'e-gouvernement': 'technology',
    'TIC': 'technology',
    'cybersécurité': 'technology',
    'télécommunications': 'technology',

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

def search_gouvernement(keyword: str) -> list:
    """Search Gouvernement.gov.bf WordPress API for a keyword"""
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
    """Scrape individual article from Gouvernement.gov.bf"""
    try:
        # Fetch full article details from API using article ID
        article_url = f"https://gouvernement.gov.bf/wp-json/wp/v2/posts/{article_id}?_fields=id,title,date,link,excerpt,content"

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

def scrape_gouvernement():
    """Main scraping function for Gouvernement.gov.bf"""
    print("="*70)
    print("Scraping Gouvernement.gov.bf (Burkina Faso)")
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
        articles = search_gouvernement(keyword)
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
    seen_urls = set()  # Track URLs to avoid duplicates
    seen_titles = set()  # Track titles to avoid duplicates

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
                        # Deduplicate by URL
                        if details['url'] not in seen_urls and details['title'] not in seen_titles:
                            seen_urls.add(details['url'])

                            seen_titles.add(details['title'])

                            article = {
                                'country': 'Burkina Faso',
                                'source': 'Gouvernement.gov.bf',
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
    parser = argparse.ArgumentParser(description='Scrape Gouvernement.gov.bf for infrastructure news')
    parser.add_argument('--output', default='gouvernement_data.csv', help='Output CSV file')
    parser.add_argument('--workers', type=int, default=15, help='Parallel workers (default: 15)')
    args = parser.parse_args()

    MAX_WORKERS = args.workers
    data = scrape_gouvernement()
    save_to_csv(data, args.output)
