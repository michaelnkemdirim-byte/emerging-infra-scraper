#!/usr/bin/env python3
"""
Ministry of Transport (MOT) - HTML Scraper
Scrapes transport infrastructure news from mot.gov.gh
"""

import requests
import csv
import time
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from typing import List, Dict, Any

# Configuration
COUNTRY = "Ghana"
SOURCE_NAME = "Ministry of Transport"

# Date filtering - Last 30 days only
DATE_30_DAYS_AGO = datetime.now() - timedelta(days=30)

# Infrastructure keywords for filtering
INFRASTRUCTURE_KEYWORDS = [
    'port', 'harbour', 'harbor', 'maritime', 'shipping', 'jetty',
    'railway', 'rail', 'train', 'metro', 'transit',
    'highway', 'road', 'bridge', 'expressway', 'motorway', 'corridor',
    'airport', 'aviation', 'terminal', 'runway',
    'transport', 'infrastructure', 'project', 'construction', 'development',
    'expansion', 'rehabilitation', 'upgrade', 'modernization',
    'feasibility study', 'commissioned', 'inaugurated'
]

# Category keywords
CATEGORY_KEYWORDS = {
    'port': ['port', 'harbour', 'harbor', 'maritime', 'shipping', 'jetty', 'dock', 'wharf'],
    'rail': ['railway', 'rail', 'train', 'metro', 'transit', 'locomotive'],
    'highway': ['highway', 'road', 'bridge', 'expressway', 'motorway', 'corridor', 'interchange'],
    'Infrastructure': ['airport', 'aviation', 'terminal', 'transport']
}

# Status detection keywords
STATUS_KEYWORDS = {
    'planned': ['proposed', 'planned', 'will', 'upcoming', 'future', 'to be', 'feasibility'],
    'ongoing': ['ongoing', 'under construction', 'in progress', 'construction', 'rehabilitation', 'upgrading', 'modernization', 'developing'],
    'completed': ['completed', 'commissioned', 'inaugurated', 'opened', 'finished', 'launched']
}


def is_relevant(title, content):
    """Check if article is about infrastructure"""
    text = (title + " " + content).lower()

    # Must contain at least one infrastructure keyword
    for keyword in INFRASTRUCTURE_KEYWORDS:
        if keyword in text:
            return True

    return False


def extract_category(title, content):
    """Extract infrastructure category from content"""
    text = (title + " " + content).lower()

    # Check each category
    for category, keywords in CATEGORY_KEYWORDS.items():
        for keyword in keywords:
            if keyword in text:
                return category

    return 'Infrastructure'


def extract_status(title, content):
    """Extract project status from title and content"""
    text = (title + " " + content).lower()

    for status, keywords in STATUS_KEYWORDS.items():
        for keyword in keywords:
            if keyword in text:
                return status

    return ""


def extract_date(soup):
    """Extract date from page"""
    # Look for date patterns in the HTML
    date_patterns = [
        r'(\d{1,2}(?:st|nd|rd|th)?\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})',
        r'((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4})',
        r'(\d{4}-\d{2}-\d{2})',
        r'(\d{1,2}/\d{1,2}/\d{4})'
    ]

    # Check meta tags first
    meta_date = soup.find('meta', property='article:published_time') or soup.find('meta', {'name': 'date'})
    if meta_date and meta_date.get('content'):
        date_str = meta_date['content']
        if 'T' in date_str:
            return date_str.split('T')[0]

    # Search in page text
    page_text = soup.get_text()
    for pattern in date_patterns:
        match = re.search(pattern, page_text)
        if match:
            date_str = match.group(1)
            try:
                # Try parsing common formats
                for fmt in ['%d %B %Y', '%B %d, %Y', '%Y-%m-%d', '%m/%d/%Y']:
                    try:
                        date_obj = datetime.strptime(date_str.replace('st', '').replace('nd', '').replace('rd', '').replace('th', ''), fmt)
                        return date_obj.strftime('%Y-%m-%d')
                    except:
                        continue
            except:
                pass

    return ""


def scrape_article(url):
    """Scrape a single article"""
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract title
        title_tag = soup.find('h1') or soup.find('h2', class_='title') or soup.find('title')
        if not title_tag:
            return None

        title = title_tag.get_text(strip=True)

        # Skip 404 or error pages
        if '404' in title or 'not found' in title.lower():
            return None

        # Extract content
        article = soup.find('article') or soup.find('div', class_='content') or soup.find('div', class_='post-content')

        if article:
            # Remove script and style elements
            for script in article(["script", "style"]):
                script.decompose()

            content_text = article.get_text(separator=' ', strip=True)
        else:
            # Fallback to body
            body = soup.find('body')
            if body:
                for script in body(["script", "style", "nav", "header", "footer"]):
                    script.decompose()
                content_text = body.get_text(separator=' ', strip=True)
            else:
                return None

        # Clean up content
        content_text = ' '.join(content_text.split())

        # Check if relevant
        if not is_relevant(title, content_text):
            return None

        # Extract date
        date_iso = extract_date(soup)

        # Create summary (first 300 chars)
        summary = content_text[:300] if len(content_text) > 300 else content_text

        # Replace commas to avoid CSV issues
        title = title.replace(',', ' -')
        summary = summary.replace(',', ' -')

        # Determine category
        category = extract_category(title, content_text)

        # Extract status
        status = extract_status(title, content_text)

        return {
            'country': COUNTRY,
            'source': SOURCE_NAME,
            'title': title,
            'date_iso': date_iso,
            'summary': summary + '...',
            'url': url,
            'category': category,
            'status': status
        }

    except Exception as e:
        print(f"  Error scraping {url[:60]}: {e}")
        return None


def scrape_from_url_file(url_file):
    """Scrape articles from URL file"""
    print(f"Starting scraper for {SOURCE_NAME}")
    print("="*60)

    # Read URLs from file
    with open(url_file, 'r', encoding='utf-8') as f:
        urls = [line.strip() for line in f if line.strip() and line.startswith('http')]

    # Filter for news article URLs (pattern: /10/16/1/number/)
    news_urls = [url for url in urls if re.search(r'/10/16/1/\d+/', url)]

    print(f"\nFound {len(urls)} total URLs")
    print(f"Filtered to {len(news_urls)} news article URLs")
    print(f"Starting to scrape...\n")

    all_data = []
    scraped = 0
    skipped = 0
    errors = 0

    for i, url in enumerate(news_urls, 1):
        if i % 20 == 0:
            print(f"Progress: {i}/{len(news_urls)} ({len(all_data)} relevant articles found)")

        article_data = scrape_article(url)

        if article_data:
            # Filter by date - only include articles from last 30 days
            if article_data.get('date_iso'):
                try:
                    article_date = datetime.strptime(article_data['date_iso'], '%Y-%m-%d')
                    if article_date >= DATE_30_DAYS_AGO:
                        all_data.append(article_data)
                        scraped += 1
                    else:
                        skipped += 1
                except:
                    # If date parsing fails, include for manual review
                    all_data.append(article_data)
                    scraped += 1
            else:
                # If no date, include for manual review
                all_data.append(article_data)
                scraped += 1
        else:
            skipped += 1

        # Rate limiting
        time.sleep(0.5)

    print(f"\nScraping completed!")
    print(f"  Total URLs processed: {len(news_urls)}")
    print(f"  Relevant articles: {scraped}")
    print(f"  Skipped (not relevant/404): {skipped}")

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

        print(f"\nData saved to: {output_file}")

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
            print(f"  Articles with dates: {len(dates)}/{len(data)}")

        print("="*60)

    except Exception as e:
        print(f"Error saving to CSV: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Scrape Ministry of Transport')
    parser.add_argument('--urls', '-u', default='mot_all_urls.txt', help='Input file with URLs')
    parser.add_argument('--output', '-o', default='mot_data.csv', help='Output CSV file')

    args = parser.parse_args()

    data = scrape_from_url_file(args.urls)

    if data:
        save_to_csv(data, args.output)
    else:
        print("No relevant infrastructure data collected!")
