#!/usr/bin/env python3
"""
Ghana Ministry of Roads & Highways - WordPress API Scraper
Scrapes infrastructure project data using WordPress REST API
"""

import requests
import csv
import time
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from typing import List, Dict, Any

# Configuration
BASE_URL = "https://mrh.gov.gh"
API_BASE = f"{BASE_URL}/wp-json/wp/v2"
COUNTRY = "Ghana"
SOURCE_NAME = "Ministry of Roads & Highways"

# Date filtering - Last 7 days only
DATE_FILTER_DAYS = 7
DATE_AFTER = (datetime.now() - timedelta(days=DATE_FILTER_DAYS)).strftime('%Y-%m-%dT00:00:00')

# Category mapping - WordPress categories to our infrastructure categories
CATEGORY_MAPPING = {
    'road-works': 'highway',
    'bridges': 'bridge',
    'project-commissioning': 'Infrastructure',
    'inspection': 'Infrastructure',
    'sod-cutting': 'Infrastructure',
    'procurement-notices': 'Infrastructure',
    'announcement': 'Infrastructure',
    'press-release': 'Infrastructure',
    'news': 'Infrastructure'
}

# Status detection keywords
STATUS_KEYWORDS = {
    'planned': ['proposed', 'planned', 'sod cutting', 'sod-cutting', 'upcoming', 'future'],
    'ongoing': ['ongoing', 'under construction', 'in progress', 'construction', 'rehabilitation', 'upgrading'],
    'completed': ['completed', 'commissioned', 'inaugurated', 'opened', 'finished', 'delivered']
}


def fetch_categories():
    """Fetch all WordPress categories"""
    try:
        response = requests.get(f"{API_BASE}/categories?per_page=100", timeout=30)
        response.raise_for_status()
        categories = response.json()

        # Create slug to ID mapping
        cat_map = {}
        for cat in categories:
            cat_map[cat['slug']] = {
                'id': cat['id'],
                'name': cat['name'],
                'count': cat['count']
            }
        return cat_map
    except Exception as e:
        print(f"Error fetching categories: {e}")
        return {}


def fetch_posts(page=1, per_page=100, category_id=None):
    """Fetch posts from WordPress API with pagination"""
    try:
        url = f"{API_BASE}/posts?per_page={per_page}&page={page}&after={DATE_AFTER}"
        if category_id:
            url += f"&categories={category_id}"

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        posts = response.json()

        # Check if there are more pages
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

    # Remove script and style elements
    for script in soup(["script", "style"]):
        script.decompose()

    # Get text
    text = soup.get_text()

    # Clean up whitespace
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = ' '.join(chunk for chunk in chunks if chunk)

    # Limit length for summary
    if len(text) > 300:
        text = text[:297] + "..."

    return text


def extract_status(title, content):
    """Extract project status from title and content"""
    text = (title + " " + content).lower()

    # Check for status keywords
    for status, keywords in STATUS_KEYWORDS.items():
        for keyword in keywords:
            if keyword in text:
                return status

    return ""  # Unknown status


def map_category(wp_categories, cat_map):
    """Map WordPress categories to our infrastructure categories"""
    if not wp_categories:
        return "Infrastructure"

    # Priority order for infrastructure categories (most specific first)
    # Bridges and specific infrastructure types should come before general categories
    priority_categories = ['bridges', 'sod-cutting', 'road-works', 'procurement-notices', 'inspection', 'project-commissioning']

    # Get category slugs for this post
    post_slugs = []
    for cat_id in wp_categories:
        for slug, info in cat_map.items():
            if info['id'] == cat_id:
                post_slugs.append(slug)
                break

    # Return the highest priority infrastructure category
    for priority_slug in priority_categories:
        if priority_slug in post_slugs:
            return CATEGORY_MAPPING.get(priority_slug, "Infrastructure")

    # Fallback to any mapped category
    for slug in post_slugs:
        if slug in CATEGORY_MAPPING:
            return CATEGORY_MAPPING[slug]

    return "Infrastructure"


def parse_post(post, cat_map):
    """Parse a WordPress post into our data schema"""
    try:
        # Extract basic fields
        title = post.get('title', {}).get('rendered', '').strip()
        url = post.get('link', '')

        # Parse date to ISO format (YYYY-MM-DD)
        date_str = post.get('date', '')
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
            date_iso = date_obj.strftime('%Y-%m-%d')
        except:
            date_iso = date_str.split('T')[0] if 'T' in date_str else date_str

        # Extract content and create summary
        content_html = post.get('content', {}).get('rendered', '')
        excerpt_html = post.get('excerpt', {}).get('rendered', '')

        # Use excerpt if available, otherwise extract from content
        if excerpt_html:
            summary = clean_html(excerpt_html)
        else:
            summary = clean_html(content_html)

        # Map category
        wp_categories = post.get('categories', [])
        category = map_category(wp_categories, cat_map)

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
        }

    except Exception as e:
        print(f"Error parsing post: {e}")
        return None


def scrape_all_posts():
    """Scrape all posts from WordPress API"""
    print(f"Starting scraper for {SOURCE_NAME}")
    print("="*60)

    # Fetch categories
    print("Fetching categories...")
    cat_map = fetch_categories()
    print(f"Found {len(cat_map)} categories")

    # Fetch all posts with pagination
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
            data = parse_post(post, cat_map)
            if data and data['title']:  # Only add if we have a title
                # Deduplicate by URL and title
                if data['url'] not in seen_urls and data['title'] not in seen_titles:
                    seen_urls.add(data['url'])
                    seen_titles.add(data['title'])
                    all_data.append(data)

        # Check if there are more pages
        if page >= total_pages:
            break

        page += 1
        time.sleep(0.5)  # Be polite to the API

    print(f"\nTotal posts scraped: {len(all_data)}")
    return all_data


def save_to_csv(data, output_file):
    """Save data to CSV file"""
    if not data:
        print("No data to save")
        return

    # Define field order according to schema
    fieldnames = ['country', 'source', 'title', 'date_iso', 'summary', 'url', 'category', ]

    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

        print(f"Data saved to: {output_file}")

        # Print summary stats
        print("\n" + "="*60)
        print("SCRAPING SUMMARY")
        print("="*60)
        print(f"Total records: {len(data)}")

        # Category breakdown
        categories = {}
        for item in data:
            cat = item['category']
            categories[cat] = categories.get(cat, 0) + 1

        print("\nCategory breakdown:")
        for cat, count in sorted(categories.items()):
            print(f"  {cat}: {count}")

        # Status breakdown
        statuses = {}
        for item in data:
            status = item.get('category', 'unknown')
            statuses[status] = statuses.get(status, 0) + 1

        print("\nStatus breakdown:")
        for status, count in sorted(statuses.items()):
            print(f"  {status}: {count}")

        print("="*60)

    except Exception as e:
        print(f"Error saving to CSV: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Scrape Ghana MRH using WordPress API')
    parser.add_argument('--output', '-o', default='mrh_data.csv', help='Output CSV file')

    args = parser.parse_args()

    # Run scraper
    data = scrape_all_posts()

    # Save to CSV
    if data:
        save_to_csv(data, args.output)
    else:
        print("No data collected!")
