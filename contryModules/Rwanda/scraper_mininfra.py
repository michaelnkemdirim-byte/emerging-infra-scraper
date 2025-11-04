#!/usr/bin/env python3
"""
Rwanda Ministry of Infrastructure - News Scraper
Scrapes infrastructure news articles from mininfra.gov.rw
"""

import requests
import csv
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
BASE_URL = "https://www.mininfra.gov.rw"
NEWS_LIST_URL = f"{BASE_URL}/updates/news"
COUNTRY = "Rwanda"
SOURCE_NAME = "Ministry of Infrastructure"

# Date filtering - Last 7 days only
DATE_7_DAYS_AGO = datetime.now() - timedelta(days=7)

# Get current year and month
NOW = datetime.now()
CURRENT_YEAR = NOW.year
CURRENT_MONTH = NOW.month

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}


def is_current_month(date_str: str) -> bool:
    """Check if date is in current month"""
    if not date_str:
        return False

    try:
        date_parts = date_str.split('-')
        year = int(date_parts[0])
        month = int(date_parts[1])

        return year == CURRENT_YEAR and month == CURRENT_MONTH
    except:
        return False


def fetch_news_list_page(page_num: int = 1) -> tuple:
    """Fetch a news list page and extract article URLs with dates"""
    try:
        if page_num == 1:
            url = NEWS_LIST_URL
        else:
            url = f"{NEWS_LIST_URL}/page?tx_news_pi1%5BcurrentPage%5D={page_num}"

        response = requests.get(url, headers=HEADERS, timeout=30, verify=False)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        articles = []
        has_current_month_articles = False

        # Find all article links with news-details pattern
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if '/updates/news-details/' in href and href != '/updates/news-details/news':
                # Get full URL
                if href.startswith('http'):
                    article_url = href
                else:
                    article_url = BASE_URL + href if href.startswith('/') else BASE_URL + '/' + href

                # Try to find the date for this article
                # Look for <time> tag in the same article container
                article_container = link.find_parent('div', class_='row')
                date_str = ""

                if article_container:
                    time_tag = article_container.find('time', itemprop='datePublished')
                    if time_tag and time_tag.get('datetime'):
                        date_str = time_tag.get('datetime')

                # Only keep current month articles
                if not is_current_month(date_str):
                    continue

                has_current_month_articles = True

                # Get title from the link
                title_elem = link.find('h3')
                title = title_elem.get_text(strip=True) if title_elem else link.get('title', '')

                if article_url and title:
                    articles.append({
                        'url': article_url,
                        'title': title,
                        'date_iso': date_str
                    })

        # Remove duplicates and filter by date (last 7 days only)
        seen_urls = set()
        seen_titles = set()
        unique_articles = []
        for article in articles:
            if article['url'] not in seen_urls and article['title'] not in seen_titles:
                seen_urls.add(article['url'])

                seen_titles.add(article['title'])
                # Filter by date - only include articles from last 7 days
                if article.get('date_iso'):
                    try:
                        article_date = datetime.strptime(article['date_iso'], '%Y-%m-%d')
                        if article_date >= DATE_7_DAYS_AGO:
                            unique_articles.append(article)
                    except:
                        # If date parsing fails, include for manual review
                        unique_articles.append(article)
                else:
                    # If no date, include for manual review
                    unique_articles.append(article)

        return unique_articles, has_current_month_articles

    except Exception as e:
        print(f"  Error fetching page {page_num}: {e}")
        return [], False


def scrape_article_content(article_info: Dict) -> Dict[str, Any]:
    """Scrape full content from an article page"""
    try:
        url = article_info['url']

        response = requests.get(url, headers=HEADERS, timeout=30, verify=False)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract title (if not already present)
        title = article_info.get('title', '')
        if not title:
            title_elem = soup.find('h1') or soup.find('h2', class_='fnt_black')
            if title_elem:
                title = title_elem.get_text(strip=True)

        # Extract date (if not already present)
        date_iso = article_info.get('date_iso', '')
        if not date_iso:
            time_tag = soup.find('time', itemprop='datePublished')
            if time_tag and time_tag.get('datetime'):
                date_iso = time_tag.get('datetime')

        # Extract main content
        # TYPO3 news extension typically uses article or div with specific classes
        content_elem = soup.find('article') or soup.find('div', class_='news-text-wrap') or soup.find('div', class_='txt_content')

        if not content_elem:
            # Fallback: look for main content area
            content_elem = soup.find('div', class_='container')

        if content_elem:
            # Remove script, style, and navigation elements
            for tag in content_elem.find_all(['script', 'style', 'nav', 'header', 'footer']):
                tag.decompose()

            # Get text content
            content_text = content_elem.get_text(separator=' ', strip=True)
        else:
            content_text = ""

        # Create summary (first 500 chars for AI processing)
        summary = content_text[:497] + "..." if len(content_text) > 500 else content_text

        # Skip if no meaningful content
        if len(title) < 10 or len(summary) < 50:
            return None

        return {
            'country': COUNTRY,
            'source': SOURCE_NAME,
            'title': title.replace(',', ' '),
            'date_iso': date_iso,
            'summary': summary.replace(',', ' '),
            'url': url,
            'category': '',  # Will be filled by AI
        }

    except Exception as e:
        print(f"  Error scraping {article_info.get('url', 'unknown')}: {e}")
        return None


def scrape_all_news():
    """Scrape all news articles from mininfra.gov.rw (current month only)"""
    print(f"Starting scraper for {SOURCE_NAME}")
    print("="*60)
    print(f"Filtering for: {CURRENT_YEAR}-{CURRENT_MONTH:02d} (current month)")
    print("="*60)

    # Step 1: Fetch all article URLs from paginated list
    print("\nStep 1: Fetching article URLs from news list pages...")
    all_articles = []
    seen_urls = set()  # Track URLs to avoid duplicates
    seen_titles = set()
    seen_titles = set()  # Track titles to avoid duplicates
    page_num = 1
    max_pages = 25  # Safety limit (we know there are ~20 pages)

    while page_num <= max_pages:
        print(f"  Fetching page {page_num}...", end='\r')
        articles, has_current_month = fetch_news_list_page(page_num)

        if not has_current_month:
            print(f"\n  No more current month articles found at page {page_num}")
            break

        if articles:
            # Deduplicate by URL
            for article in articles:
                url = article.get('url', '')
                if url and url not in seen_urls and title not in seen_titles:
                    seen_urls.add(url)

                    seen_titles.add(title)
                    all_articles.append(article)
            print(f"  Page {page_num}: Found {len(articles)} articles ({len(all_articles)} unique so far)", end='\r')

        page_num += 1
        time.sleep(0.5)  # Rate limiting

    print(f"\n  Total unique article URLs collected: {len(all_articles)}")

    if not all_articles:
        print(f"  ERROR: No articles from {CURRENT_YEAR}-{CURRENT_MONTH:02d} found!")
        return []

    # Step 2: Scrape full content from each article
    print(f"\nStep 2: Scraping full content from {len(all_articles)} articles...")
    all_data = []
    skipped = 0

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_article = {executor.submit(scrape_article_content, article): article for article in all_articles}

        completed = 0
        for future in as_completed(future_to_article):
            completed += 1

            try:
                result = future.result()
                if result:
                    all_data.append(result)
                else:
                    skipped += 1

                print(f"  Progress: {completed}/{len(all_articles)} processed ({len(all_data)} kept, {skipped} skipped)", end='\r')
            except Exception as e:
                skipped += 1
                print(f"\n  Error: {e}")

    print(f"\n  Successfully scraped {len(all_data)} articles")
    print(f"  Skipped: {skipped} (insufficient content)")

    return all_data


def save_to_csv(data: List[Dict], output_file: str):
    """Save data to CSV file"""
    if not data:
        print("No data to save")
        return

    fieldnames = ['country', 'source', 'title', 'date_iso', 'summary', 'url', 'category']

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
        print(f"Month filter: {CURRENT_YEAR}-{CURRENT_MONTH:02d}")

        # Date coverage
        dates = [d['date_iso'] for d in data if d['date_iso']]
        if dates:
            dates.sort()
            print(f"\nDate range:")
            print(f"  Oldest: {dates[0]}")
            print(f"  Newest: {dates[-1]}")

        print("\nNote: Category fields are empty - will be filled by AI processing")
        print("="*60)

    except Exception as e:
        print(f"Error saving to CSV: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Scrape Rwanda Ministry of Infrastructure news')
    parser.add_argument('--output', '-o', default='mininfra_data.csv', help='Output CSV file')

    args = parser.parse_args()

    # Run scraper
    data = scrape_all_news()

    # Save to CSV
    if data:
        save_to_csv(data, args.output)
    else:
        print("No articles collected!")
