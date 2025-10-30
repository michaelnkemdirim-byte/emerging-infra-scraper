#!/usr/bin/env python3
"""
Kenya Electricity Generating Company (KenGen) - News Scraper
Scrapes infrastructure project news and announcements
"""

import requests
import csv
import time
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
BASE_URL = "https://www.kengen.co.ke"
NEWS_URL = f"{BASE_URL}/index.php/information-center/news-and-events.html"
COUNTRY = "Kenya"
SOURCE_NAME = "Kenya Electricity Generating Company (KenGen)"

# Date filtering - Last 7 days to match other scrapers
DATE_7_DAYS_AGO = datetime.now() - timedelta(days=7)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}

# Infrastructure keywords - ONLY infrastructure-related content
INFRASTRUCTURE_KEYWORDS = [
    # Energy (primary focus)
    'power plant', 'power station', 'hydropower', 'hydro power', 'geothermal',
    'solar', 'wind', 'renewable energy', 'electricity generation', 'dam',
    'transmission', 'grid', 'substation', 'power line', 'electricity infrastructure',
    'mw', 'megawatt', 'capacity', 'generation', 'plant', 'facility',
    'energy infrastructure', 'green energy', 'nuclear', 'thermal', 'biomass',
    'wind farm', 'battery storage', 'energy storage',
    # Infrastructure
    'construction', 'development', 'expansion', 'project', 'commissioning',
    'infrastructure', 'data center',
    # Economic
    'finance', 'investment', 'fintech', 'economic', 'banking', 'trade',
    # Technology
    'digital', 'technology', 'ICT', 'AI', 'cybersecurity', 'broadband',
    'telecommunications', 'e-government', 'fiber optic'
]

# Exclude non-infrastructure content
EXCLUDE_KEYWORDS = [
    'vacancy', 'recruitment', 'job', 'career', 'agm', 'annual general meeting',
    'shares', 'dividend', 'financial results', 'profit', 'stock', 'investor',
    'csr', 'charity', 'donation', 'festive', 'christmas', 'scholarship',
    'staff', 'employee', 'retirement', 'award ceremony', 'sports day'
]


def fetch_page(url: str, retries: int = 3) -> str:
    """Fetch a page with retries"""
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=30, verify=False)
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"  Error fetching {url} (attempt {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(2)
    return ""


def is_infrastructure_related(title: str, summary: str) -> bool:
    """Check if content is infrastructure-related"""
    text = (title + " " + summary).lower()

    # Check for infrastructure keywords
    has_infra = any(keyword in text for keyword in INFRASTRUCTURE_KEYWORDS)

    # Check for excluded keywords
    has_excluded = any(keyword in text for keyword in EXCLUDE_KEYWORDS)

    return has_infra and not has_excluded


def extract_article_urls_from_page(page_num: int = 0) -> List[str]:
    """Extract article URLs from a news listing page"""
    if page_num == 0:
        url = NEWS_URL
    else:
        url = f"{NEWS_URL}?start={page_num * 10}"

    html = fetch_page(url)
    if not html:
        return []

    soup = BeautifulSoup(html, 'html.parser')
    article_urls = []

    # Find all links that contain news-and-events
    for link in soup.find_all('a', href=True):
        href = link['href']

        # Skip pagination, print, and layout links
        if any(x in href for x in ['start=', 'print=', 'layout=', 'format=', '.feed']):
            continue

        if 'news-and-events' in href:
            # Build full URL
            if href.startswith('http'):
                full_url = href
            elif href.startswith('/'):
                full_url = f"{BASE_URL}{href}"
            else:
                full_url = f"{BASE_URL}/index.php/{href}"

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

    # Look for date patterns
    patterns = [
        r'((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4})',
        r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})',
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            date_str = match.group(1)

            # Try to parse and convert to ISO format
            try:
                # Remove comma
                date_str = date_str.replace(',', '')

                # Try different date formats
                for fmt in ['%B %d %Y', '%d %B %Y']:
                    try:
                        date_obj = datetime.strptime(date_str, fmt)
                        # Only accept dates from 2020-2025
                        if 2020 <= date_obj.year <= 2025:
                            return date_obj.strftime('%Y-%m-%d')
                    except:
                        continue
            except:
                pass

    return ""


def extract_status(title: str, content: str) -> str:
    """Extract project status from title and content"""
    text = (title + " " + content).lower()

    # Status keywords
    if any(kw in text for kw in ['commissioned', 'inaugurated', 'opened', 'completed', 'operational', 'online']):
        return "completed"
    elif any(kw in text for kw in ['ongoing', 'under construction', 'construction', 'drilling', 'developing', 'building']):
        return "ongoing"
    elif any(kw in text for kw in ['planned', 'proposed', 'upcoming', 'to be', 'will', 'cabinet approves', 'green light']):
        return "planned"

    return ""


def scrape_article(url: str) -> Dict[str, Any]:
    """Scrape a single article"""
    html = fetch_page(url)
    if not html:
        return None

    soup = BeautifulSoup(html, 'html.parser')

    # Extract title - try multiple methods
    title = ""

    # Try h1 first
    title_tag = soup.find('h1')
    if title_tag:
        title = title_tag.get_text(strip=True)

    # If h1 is bad or missing, try <title> tag
    if not title or title in ['Ends/', 'No title', '']:
        title_tag = soup.find('title')
        if title_tag:
            title = title_tag.get_text(strip=True)
            # Remove site name from title
            title = title.replace('| Kenya Electricity Generating Company', '').replace('| KenGen', '').strip()

    # If still no title, skip
    if not title or len(title) < 10:
        return None

    # Extract content
    paragraphs = soup.find_all('p')
    content = ' '.join([p.get_text(strip=True) for p in paragraphs[:10]])

    # Create summary (first 300 chars)
    summary = content[:297] + "..." if len(content) > 300 else content

    # Skip if no content
    if len(content) < 50:
        return None

    # Check if infrastructure-related
    if not is_infrastructure_related(title, summary):
        return None

    # Extract date
    date_iso = parse_date(html)

    # Extract status
    status = extract_status(title, summary)

    return {
        'country': COUNTRY,
        'source': SOURCE_NAME,
        'title': title.replace(',', ' '),  # Remove commas for CSV
        'date_iso': date_iso,
        'summary': summary.replace(',', ' '),  # Remove commas for CSV
        'url': url,
        'category': 'infrastructure',  # Energy infrastructure
        'status': status
    }


def discover_all_articles_via_pagination() -> List[str]:
    """Discover all article URLs by crawling pagination pages"""
    print("  Discovering articles via pagination...")

    all_article_urls = []
    page_num = 0
    max_empty_pages = 3  # Stop after 3 consecutive empty pages
    empty_count = 0

    while page_num < 20:  # Limit to 20 pages for speed
        print(f"  Scanning page {page_num + 1}...", end='\r')

        # Build page URL
        if page_num == 0:
            page_url = NEWS_URL
        else:
            # Pagination uses start=10, start=20, etc (10 per increment)
            page_url = f"{NEWS_URL}?start={page_num * 10}"

        html = fetch_page(page_url)
        if not html:
            empty_count += 1
            if empty_count >= max_empty_pages:
                break
            page_num += 1
            continue

        soup = BeautifulSoup(html, 'html.parser')

        # Find article links on this page
        page_articles = []
        for link in soup.find_all('a', href=True):
            href = link['href']

            # Look for news article links
            if 'news-and-events' in href:
                # Skip pagination, print, layout links
                if not any(x in href for x in ['start=', 'print=1', 'layout=', 'format=', '.feed', 'tmpl=component']):
                    # Build full URL
                    if href.startswith('http'):
                        full_url = href
                    elif href.startswith('/'):
                        full_url = f"{BASE_URL}{href}"
                    else:
                        full_url = f"{BASE_URL}/{href}"

                    page_articles.append(full_url)

        # Remove duplicates on this page
        page_articles = list(dict.fromkeys(page_articles))

        if page_articles:
            all_article_urls.extend(page_articles)
            empty_count = 0  # Reset counter
        else:
            empty_count += 1
            if empty_count >= max_empty_pages:
                print(f"\n  Stopped at page {page_num + 1} (no more articles)")
                break

        page_num += 1
        time.sleep(0.3)  # Be polite

    # Remove duplicates from all pages
    all_article_urls = list(dict.fromkeys(all_article_urls))

    print(f"\n  Discovered {len(all_article_urls)} unique articles across {page_num} pages")
    return all_article_urls


def scrape_all_articles():
    """Scrape all articles from KenGen news portal"""
    print(f"Starting scraper for {SOURCE_NAME}")
    print("="*60)

    # Step 1: Discover article URLs via pagination
    print("\nStep 1: Discovering article URLs from website...")
    all_article_urls = discover_all_articles_via_pagination()

    if not all_article_urls:
        print("  ERROR: No articles discovered!")
        return []

    print(f"  Found {len(all_article_urls)} unique article URLs")

    # Step 2: Scrape each article in parallel
    print("\nStep 2: Scraping articles in parallel (8 workers)...")
    all_data = []
    seen_urls = set()  # Track URLs to avoid duplicates
    seen_titles = set()  # Track titles to avoid duplicates

    with ThreadPoolExecutor(max_workers=8) as executor:
        # Submit all scraping tasks
        future_to_url = {executor.submit(scrape_article, url): url for url in all_article_urls}

        completed = 0
        skipped = 0
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
                else:
                    skipped += 1

                print(f"  Progress: {completed}/{len(all_article_urls)} processed ({len(all_data)} kept, {skipped} filtered)", end='\r')
            except Exception as e:
                print(f"\n  Error scraping {url[:60]}: {e}")
                skipped += 1

    print(f"\n  Successfully scraped {len(all_data)} infrastructure articles")
    print(f"  Filtered out {skipped} non-infrastructure articles")

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
        else:
            print(f"\nArticles without dates: {len(data)}")

        print("="*60)

    except Exception as e:
        print(f"Error saving to CSV: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Scrape KenGen news')
    parser.add_argument('--output', '-o', default='kengen_data.csv', help='Output CSV file')

    args = parser.parse_args()

    # Run scraper (fully standalone - discovers articles from website)
    data = scrape_all_articles()

    # Save to CSV
    if data:
        save_to_csv(data, args.output)
    else:
        print("No infrastructure articles collected!")
