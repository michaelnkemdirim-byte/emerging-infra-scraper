#!/usr/bin/env python3
"""
Scraper for Ministry of Urban Infrastructure & Development (MUI)
Website: https://mui.gov.et
Type: WordPress with news and press releases
Focus: Infrastructure projects, urban development, housing
"""

import requests
import csv
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from html import unescape
import time
import argparse

# Configuration
BASE_URL = 'https://mui.gov.et'
WP_API_URL = f'{BASE_URL}/wp-json/wp/v2/posts'

# Date filter: last 30 days for weekly reports
DATE_30_DAYS_AGO = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%dT%H:%M:%S')
DATE_THRESHOLD = datetime.now() - timedelta(days=30)  # For client-side date validation

def clean_html(raw_html):
    """Remove HTML tags and clean text"""
    if not raw_html:
        return ""
    soup = BeautifulSoup(raw_html, 'html.parser')
    text = soup.get_text(separator=' ', strip=True)
    return unescape(text)

def fetch_posts_from_api(page: int = 1, per_page: int = 100) -> tuple:
    """
    Fetch posts from WordPress API
    Returns: (list of posts, total pages)
    """
    params = {
        'page': page,
        'per_page': per_page,
        'after': DATE_30_DAYS_AGO,
        '_embed': 1  # Include embedded data (featured image, author, etc.)
    }

    try:
        response = requests.get(WP_API_URL, params=params, timeout=30)
        response.raise_for_status()

        total_pages = int(response.headers.get('X-WP-TotalPages', 1))
        posts = response.json()

        return posts, total_pages

    except requests.exceptions.RequestException as e:
        print(f"Error fetching page {page}: {e}")
        return [], 0

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

        # Source
        source = 'Ministry of Urban Infrastructure & Development'

        # Category - leave empty for AI to fill based on content
        category = ''

        # Status - leave empty for AI to determine
        status = ''

        return {
            'country': 'Ethiopia',
            'source': source,
            'title': title,
            'date_iso': date_published,
            'summary': excerpt,
            'url': url,
            'category': category,
            'status': status
        }

    except Exception as e:
        print(f"Error extracting article data: {e}")
        return None

def scrape_mui():
    """Main scraping function for MUI"""
    print("="*70)
    print("Scraping Ministry of Urban Infrastructure & Development (mui.gov.et)")
    print("="*70)
    print(f"Fetching posts from last 30 days (after {DATE_30_DAYS_AGO[:10]})")
    print()

    all_data = []
    seen_urls = set()  # Track URLs to avoid duplicates
    seen_titles = set()  # Track titles to avoid duplicates
    page = 1

    while True:
        print(f"Fetching page {page}...")
        posts, total_pages = fetch_posts_from_api(page)

        if not posts:
            break

        for post in posts:
            article = extract_article_data(post)
            if article and article['title'] and article['url']:
                # Deduplicate by URL and title
                url = article['url']
                title = article['title']
                if url in seen_urls or title in seen_titles:
                    continue

                # Client-side date validation (filter articles outside 30-day range)
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
                        # If date parsing fails, include for manual review
                        seen_urls.add(url)
                        seen_titles.add(title)
                        all_data.append(article)
                        print(f"  ⚠ {article['date_iso']}: {article['title'][:70]}")
                else:
                    # No date, include for manual review
                    seen_urls.add(url)
                    seen_titles.add(title)
                    all_data.append(article)
                    print(f"  ⚠ No date: {article['title'][:70]}")

        if page >= total_pages:
            break

        page += 1
        time.sleep(1)  # Be polite to the server

    print()
    print(f"Total articles collected: {len(all_data)}")

    return all_data

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
    parser = argparse.ArgumentParser(description='Scrape MUI website for infrastructure news')
    parser.add_argument('--output', default='mui_data.csv', help='Output CSV file')
    args = parser.parse_args()

    data = scrape_mui()
    save_to_csv(data, args.output)
