#!/usr/bin/env python3
"""
Ghana Ports & Harbours Authority Scraper
Scrapes port news and infrastructure updates
"""

import requests
import csv
import time
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
BASE_URL = "https://www.ghanaports.gov.gh"
MEDIA_URL = f"{BASE_URL}/media"
COUNTRY = "Ghana"
SOURCE_NAME = "Ghana Ports & Harbours Authority"

# Date filtering - Last 7 days to match other scrapers
DATE_7_DAYS_AGO = datetime.now() - timedelta(days=7)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}


def fetch_page(url: str, retries: int = 2) -> str:
    """Fetch a page with retries"""
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=20)
            response.raise_for_status()
            return response.text
        except Exception as e:
            if attempt < retries - 1:
                print(f"  Retry {attempt+1}/{retries} for {url[:50]}...")
                time.sleep(1)  # Reduced from 2 to 1 second
    return ""


def extract_article_urls_from_page(page_num: int) -> List[str]:
    """Extract article URLs from a media pagination page"""
    url = f"{MEDIA_URL}?page={page_num}" if page_num > 1 else MEDIA_URL

    html = fetch_page(url)
    if not html:
        return []

    soup = BeautifulSoup(html, 'html.parser')
    article_urls = []

    # Find all news-details links
    for link in soup.find_all('a', href=True):
        href = link['href']
        if 'news-details' in href:
            if href.startswith('http'):
                full_url = href
            elif href.startswith('/'):
                full_url = f"{BASE_URL}{href}"
            else:
                full_url = f"{BASE_URL}/{href}"

            article_urls.append(full_url)

    # Remove duplicates while preserving order
    seen = set()
    unique_urls = []
    for url in article_urls:
        if url not in seen:
            seen.add(url)
            unique_urls.append(url)

    return unique_urls


def parse_date(text: str) -> str:
    """Parse date from article content and convert to ISO format"""
    if not text:
        return ""

    # Look for date patterns like "22nd September 2025", "September 22, 2025", or "Aug 22 2025"
    patterns = [
        r'(\d{1,2}(?:st|nd|rd|th)?\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})',
        r'((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4})',
        r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+\d{4})',  # Short month format
        r'(\d{4}-\d{2}-\d{2})',
        r'(\d{1,2}/\d{1,2}/\d{4})'
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            date_str = match.group(1)

            # Try to parse and convert to ISO format
            try:
                # Remove ordinal suffixes (st, nd, rd, th)
                date_str = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_str)

                # Try different date formats
                for fmt in ['%d %B %Y', '%B %d %Y', '%b %d %Y', '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']:
                    try:
                        date_obj = datetime.strptime(date_str, fmt)
                        return date_obj.strftime('%Y-%m-%d')
                    except:
                        continue
            except:
                pass

    return ""


def scrape_article(url: str) -> Dict[str, Any]:
    """Scrape a single article"""
    html = fetch_page(url)
    if not html:
        return None

    soup = BeautifulSoup(html, 'html.parser')

    # Extract title
    title = ""
    title_tag = soup.find('title')
    if title_tag:
        title_text = title_tag.get_text()
        # Remove site name prefix
        title = title_text.replace('Ghana Ports & Harbours Authority :: ', '').strip()

    # Extract content
    content = ""
    # Look for main content area
    content_areas = soup.find_all(['div', 'article', 'section'])
    for area in content_areas:
        paragraphs = area.find_all('p')
        if len(paragraphs) > 2:  # Likely the main content
            content = ' '.join([p.get_text(strip=True) for p in paragraphs])
            break

    if not content:
        # Fallback: get all paragraphs
        paragraphs = soup.find_all('p')
        content = ' '.join([p.get_text(strip=True) for p in paragraphs[:10]])

    # Create summary (first 300 chars)
    summary = content[:297] + "..." if len(content) > 300 else content

    # Extract date from styled div first (format: "Aug 22 2025")
    date_iso = ""
    date_div = soup.find('div', style=lambda x: x and 'font-style: italic' in x and 'color: #808080' in x)
    if date_div:
        date_text = date_div.get_text(strip=True)
        date_iso = parse_date(date_text)

    # Fallback: Extract date from content if not found in div
    if not date_iso:
        date_iso = parse_date(content)

    # Extract status keywords
    status = ""
    content_lower = content.lower()
    if any(word in content_lower for word in ['planned', 'proposed', 'upcoming', 'future']):
        status = "planned"
    elif any(word in content_lower for word in ['ongoing', 'under construction', 'in progress', 'construction']):
        status = "ongoing"
    elif any(word in content_lower for word in ['completed', 'commissioned', 'inaugurated', 'opened']):
        status = "completed"

    return {
        'country': COUNTRY,
        'source': SOURCE_NAME,
        'title': title,
        'date_iso': date_iso,
        'summary': summary,
        'url': url,
        'category': 'port',  # Ghana Ports is all about ports
        'status': status
    }


def scrape_all_articles():
    """Scrape all articles from Ghana Ports website"""
    print(f"Starting scraper for {SOURCE_NAME}")
    print("="*60)

    # Step 1: Collect all article URLs from pagination
    print("\nStep 1: Collecting article URLs from pagination pages...")
    all_article_urls = []
    empty_pages = 0

    # Try pages 1-15 (limit to speed up scraping)
    for page_num in range(1, 16):
        print(f"  Scanning page {page_num}...", end='\r')
        urls = extract_article_urls_from_page(page_num)

        if not urls:
            empty_pages += 1
            # Break if 3 consecutive empty pages (likely reached end)
            if empty_pages >= 3:
                print(f"\n  Stopping: {empty_pages} consecutive empty pages")
                break
        else:
            empty_pages = 0  # Reset counter
            all_article_urls.extend(urls)

        time.sleep(0.3)  # Reduced delay

    # Remove duplicates
    all_article_urls = list(dict.fromkeys(all_article_urls))

    print(f"\n  Found {len(all_article_urls)} unique article URLs")

    # Step 2: Scrape each article in parallel
    print("\nStep 2: Scraping articles in parallel (8 workers)...")
    all_data = []
    seen_urls = set()  # Track URLs to avoid duplicates
    seen_titles = set()  # Track titles to avoid duplicates

    with ThreadPoolExecutor(max_workers=8) as executor:
        # Submit all scraping tasks
        future_to_url = {executor.submit(scrape_article, url): url for url in all_article_urls}

        completed = 0
        for future in as_completed(future_to_url):
            completed += 1
            url = future_to_url[future]

            try:
                article_data = future.result()
                if article_data and article_data['title']:
                    # Deduplicate by URL and title
                    if article_data['url'] not in seen_urls and article_data['title'] not in seen_titles:
                        # Filter by date - only include articles from last 7 days
                        if article_data.get('date_iso'):
                            try:
                                article_date = datetime.strptime(article_data['date_iso'], '%Y-%m-%d')
                                if article_date >= DATE_7_DAYS_AGO:
                                    seen_urls.add(article_data['url'])
                                    seen_titles.add(article_data['title'])
                                    all_data.append(article_data)
                            except:
                                # If date parsing fails, include for manual review
                                seen_urls.add(article_data['url'])
                                seen_titles.add(article_data['title'])
                                all_data.append(article_data)
                        else:
                            # If no date, include for manual review
                            seen_urls.add(article_data['url'])
                            seen_titles.add(article_data['title'])
                            all_data.append(article_data)

                print(f"  Progress: {completed}/{len(all_article_urls)} articles processed", end='\r')
            except Exception as e:
                print(f"\n  Error scraping {url[:60]}: {e}")

    print(f"\n  Successfully scraped {len(all_data)} articles")

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

    parser = argparse.ArgumentParser(description='Scrape Ghana Ports & Harbours Authority')
    parser.add_argument('--output', '-o', default='ghanaports_data.csv', help='Output CSV file')

    args = parser.parse_args()

    # Run scraper
    data = scrape_all_articles()

    # Save to CSV
    if data:
        save_to_csv(data, args.output)
    else:
        print("No data collected!")
