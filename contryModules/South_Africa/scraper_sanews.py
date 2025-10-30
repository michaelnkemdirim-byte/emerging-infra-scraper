#!/usr/bin/env python3
"""
SA News - Scraper
Scrapes government news from sanews.gov.za using RSS feeds
Collects from both main news feed and features feed
"""

import requests
import csv
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from typing import List, Dict, Any
import xml.etree.ElementTree as ET
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
BASE_URL = "https://www.sanews.gov.za"
RSS_FEEDS = [
    f"{BASE_URL}/south-africa-news-stories.xml",
    f"{BASE_URL}/features.xml"
]
COUNTRY = "South Africa"
SOURCE_NAME = "SA News"

# Date filtering - Last 7 days only
DATE_7_DAYS_AGO = datetime.now() - timedelta(days=7)

# Comprehensive infrastructure keywords for filtering
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
    'industrial zone',
    'free trade zone',
    'logistics hub',
    'container terminal',
    'freight corridor',
    'dry port',

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
    'mobile money',
    'remittances',
    'venture capital',
    'private equity',
    'forex',
    'foreign exchange',
    'inflation',
    'gdp',

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
    'biofuel',
    'bioenergy',
    'geothermal',
    'battery storage',
    'grid infrastructure',
    'ppa',
    'power purchase agreement',
    'lng',
    'liquefied natural gas',
    'gas-to-power',
    'coal-to-power',

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
    'data center',
    'iot',
    'internet of things',
    'cloud computing',
    'mobile banking',
    'satellite internet',
    'starlink',
    'e-commerce',
    'digital payments',
    'biometric',
    'digital id',
    'drone',
    'api'
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}


def parse_date(date_str: str) -> str:
    """Parse RSS date to ISO format"""
    try:
        # RSS date format examples:
        # Wed, 23 Oct 2025 09:30:00 +0200
        # Try multiple formats
        for fmt in [
            '%a, %d %b %Y %H:%M:%S %z',
            '%Y-%m-%dT%H:%M:%S%z',
            '%Y-%m-%d'
        ]:
            try:
                date_obj = datetime.strptime(date_str.strip(), fmt)
                return date_obj.strftime('%Y-%m-%d')
            except:
                continue
        return ""
    except:
        return ""


def is_relevant_article(title, summary):
    """Check if article contains relevant keywords"""
    text = (title + ' ' + summary).lower()
    return any(keyword.lower() in text for keyword in SEARCH_KEYWORDS)


def fetch_rss_feed(feed_url: str) -> List[Dict]:
    """Fetch and parse RSS feed"""
    try:
        print(f"  Fetching: {feed_url}")
        response = requests.get(feed_url, headers=HEADERS, timeout=30, verify=False)
        response.raise_for_status()

        # Parse XML
        root = ET.fromstring(response.content)

        items = []
        # RSS 2.0 format
        for item in root.findall('.//item'):
            title_elem = item.find('title')
            link_elem = item.find('link')
            desc_elem = item.find('description')
            date_elem = item.find('pubDate')

            if title_elem is not None and link_elem is not None:
                items.append({
                    'title': title_elem.text or '',
                    'link': link_elem.text or '',
                    'description': desc_elem.text or '' if desc_elem is not None else '',
                    'pubDate': date_elem.text or '' if date_elem is not None else ''
                })

        print(f"    Found {len(items)} items")
        return items
    except Exception as e:
        print(f"    Error fetching RSS feed: {e}")
        return []


def fetch_article_content(url: str) -> str:
    """Fetch full article content from URL"""
    try:
        response = requests.get(url, headers=HEADERS, timeout=30, verify=False)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        # Try to find article content
        # Look for common article content containers
        article = None
        for selector in [
            'article',
            '.article-content',
            '.post-content',
            '.entry-content',
            '.content',
            'div[class*="body"]',
            'div[class*="text"]'
        ]:
            article = soup.select_one(selector)
            if article:
                break

        if article:
            # Remove script and style tags
            for tag in article.find_all(['script', 'style']):
                tag.decompose()

            text = article.get_text(separator=' ', strip=True)
            return text

        return ""
    except Exception as e:
        return ""


def process_rss_item(item: Dict, seen_urls: set) -> Dict[str, Any]:
    """Process a single RSS item"""
    try:
        title = item['title'].strip()
        url = item['link'].strip()
        rss_description = item['description'].strip()
        pub_date = item['pubDate'].strip()

        # Skip duplicates
        if url in seen_urls or title in seen_titles:
            return None
        seen_urls.add(url)

        seen_titles.add(title)

        # Parse date
        date_iso = parse_date(pub_date)

        # Clean HTML from description
        desc_soup = BeautifulSoup(rss_description, 'html.parser')
        description = desc_soup.get_text(separator=' ', strip=True)

        # Try to fetch full article content
        full_content = fetch_article_content(url)

        # Use full content if available, otherwise use RSS description
        content = full_content if full_content else description

        # Create summary (first 500 chars)
        summary = content[:497] + "..." if len(content) > 500 else content

        # Skip if no meaningful content
        if len(title) < 10 or len(summary) < 50:
            return None

        # Check relevance with keywords
        if not is_relevant_article(title, summary):
            return None

        return {
            'country': COUNTRY,
            'source': SOURCE_NAME,
            'title': title.replace(',', ' '),
            'date_iso': date_iso,
            'summary': summary.replace(',', ' '),
            'url': url,
            'category': '',  # Will be filled by AI
            'status': ''     # Will be filled by AI
        }
    except Exception as e:
        return None


def scrape_all_feeds():
    """Scrape all RSS feeds"""
    print(f"Starting scraper for {SOURCE_NAME}")
    print("="*60)
    print("Collecting articles from RSS feeds")
    print("="*60)

    all_data = []
    seen_urls = set()

    for feed_url in RSS_FEEDS:
        items = fetch_rss_feed(feed_url)

        if not items:
            continue

        print(f"\n  Processing {len(items)} items from feed...")
        for idx, item in enumerate(items, 1):
            result = process_rss_item(item, seen_urls)
            if result:
                # Filter by date - only include articles from last 7 days
                if result.get('date_iso'):
                    try:
                        article_date = datetime.strptime(result['date_iso'], '%Y-%m-%d')
                        if article_date >= DATE_7_DAYS_AGO:
                            all_data.append(result)
                    except:
                        # If date parsing fails, include for manual review
                        all_data.append(result)
                else:
                    # If no date, include for manual review
                    all_data.append(result)

            print(f"    Progress: {idx}/{len(items)} ({len(all_data)} kept)", end='\r')
            time.sleep(0.2)  # Be polite

        print(f"\n    Completed processing feed")
        time.sleep(1)

    print(f"\n\nTotal articles collected: {len(all_data)}")
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

        # Date coverage
        dates = [d['date_iso'] for d in data if d['date_iso']]
        if dates:
            dates.sort()
            print(f"\nDate range:")
            print(f"  Oldest: {dates[0]}")
            print(f"  Newest: {dates[-1]}")

        print("\nNote: Category and status fields are empty - will be filled by AI processing")
        print("Note: This scraper collects from RSS feeds which contain mixed content")
        print("      AI categorization will filter for infrastructure-related articles")
        print("="*60)

    except Exception as e:
        print(f"Error saving to CSV: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Scrape SA News RSS feeds')
    parser.add_argument('--output', '-o', default='sanews_data.csv', help='Output CSV file')

    args = parser.parse_args()

    # Run scraper
    data = scrape_all_feeds()

    # Save to CSV
    if data:
        save_to_csv(data, args.output)
    else:
        print("No articles collected!")
