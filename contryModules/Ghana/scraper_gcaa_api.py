#!/usr/bin/env python3
"""
Ghana Civil Aviation Authority (GCAA) - WordPress API Scraper
Scrapes aviation infrastructure news and updates
"""

import requests
import csv
import time
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from typing import List, Dict, Any

# Configuration
BASE_URL = "https://www.gcaa.com.gh/web"
API_BASE = f"{BASE_URL}/wp-json/wp/v2"
COUNTRY = "Ghana"
SOURCE_NAME = "Ghana Civil Aviation Authority"

# Date filtering - Last 30 days only
DATE_FILTER_DAYS = 30
DATE_AFTER = (datetime.now() - timedelta(days=DATE_FILTER_DAYS)).strftime('%Y-%m-%dT00:00:00')

# Status detection keywords
STATUS_KEYWORDS = {
    'planned': ['proposed', 'planned', 'will', 'upcoming', 'future', 'to be'],
    'ongoing': ['ongoing', 'under construction', 'in progress', 'construction', 'rehabilitation', 'upgrading', 'modernization'],
    'completed': ['completed', 'commissioned', 'inaugurated', 'opened', 'finished', 'launched']
}


def fetch_posts(page=1, per_page=100):
    """Fetch posts from WordPress API with pagination"""
    try:
        url = f"{API_BASE}/posts?per_page={per_page}&page={page}&after={DATE_AFTER}"
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        posts = response.json()
        total_pages = int(response.headers.get('X-WP-TotalPages', 1))
        total_posts = int(response.headers.get('X-WP-Total', 0))

        return posts, total_pages, total_posts
    except Exception as e:
        print(f"Error fetching posts (page {page}): {e}")
        return [], 0, 0


def clean_html(html_content):
    """Remove HTML tags and clean text"""
    if not html_content:
        return ""

    soup = BeautifulSoup(html_content, 'html.parser')

    for script in soup(["script", "style"]):
        script.decompose()

    text = soup.get_text()
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = ' '.join(chunk for chunk in chunks if chunk)

    if len(text) > 300:
        text = text[:297] + "..."

    return text


def extract_status(title, content):
    """Extract project status from title and content"""
    text = (title + " " + content).lower()

    for status, keywords in STATUS_KEYWORDS.items():
        for keyword in keywords:
            if keyword in text:
                return status

    return ""


def parse_post(post):
    """Parse a WordPress post into our data schema"""
    try:
        title = post.get('title', {}).get('rendered', '').strip()
        url = post.get('link', '')

        # Parse date to ISO format
        date_str = post.get('date', '')
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
            date_iso = date_obj.strftime('%Y-%m-%d')
        except:
            date_iso = date_str.split('T')[0] if 'T' in date_str else date_str

        # Extract content and create summary
        content_html = post.get('content', {}).get('rendered', '')
        excerpt_html = post.get('excerpt', {}).get('rendered', '')

        if excerpt_html:
            summary = clean_html(excerpt_html)
        else:
            summary = clean_html(content_html)

        # Category - All GCAA content is aviation/airport infrastructure
        category = 'Infrastructure'

        # Extract status
        status = extract_status(title, summary)

        return {
            'country': COUNTRY,
            'source': SOURCE_NAME,
            'title': title,
            'date_iso': date_iso,
            'summary': summary,
            'url': url,
            'category': category,
            'status': status
        }

    except Exception as e:
        print(f"Error parsing post: {e}")
        return None


def scrape_all_posts():
    """Scrape all posts from WordPress API"""
    print(f"Starting scraper for {SOURCE_NAME}")
    print("="*60)

    all_data = []
    seen_urls = set()  # Track URLs to avoid duplicates
    seen_titles = set()  # Track titles to avoid duplicates
    page = 1

    print("\nFetching posts...")
    while True:
        posts, total_pages, total_posts = fetch_posts(page=page, per_page=100)

        if not posts:
            break

        print(f"Processing page {page}/{total_pages} ({len(posts)} posts)")

        for post in posts:
            data = parse_post(post)
            if data and data['title']:
                # Deduplicate by URL and title
                if data['url'] not in seen_urls and data['title'] not in seen_titles:
                    # Replace commas in text fields to avoid CSV issues
                    data['title'] = data['title'].replace(',', ' -')
                    data['summary'] = data['summary'].replace(',', ' -')
                    seen_urls.add(data['url'])
                    seen_titles.add(data['title'])
                    all_data.append(data)

        if page >= total_pages:
            break

        page += 1
        time.sleep(0.5)

    print(f"\nTotal posts scraped: {len(all_data)}")
    return all_data


def save_to_csv(data, output_file):
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

        print(f"Data saved to: {output_file}")

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

    parser = argparse.ArgumentParser(description='Scrape Ghana Civil Aviation Authority')
    parser.add_argument('--output', '-o', default='gcaa_data.csv', help='Output CSV file')

    args = parser.parse_args()

    data = scrape_all_posts()

    if data:
        save_to_csv(data, args.output)
    else:
        print("No data collected!")
