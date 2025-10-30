#!/usr/bin/env python3
"""
The Citizen - Playwright Search Scraper
Scrapes infrastructure, energy, technology, and economic news from thecitizen.co.tz
Uses search endpoint with keyword-based queries
"""

from patchright.sync_api import sync_playwright
import csv
from datetime import datetime, timedelta
from typing import List, Dict, Any
import time

# Configuration
BASE_URL = "https://www.thecitizen.co.tz"
SEARCH_URL = f"{BASE_URL}/service/search/tanzania/2718734"
COUNTRY = "Tanzania"
SOURCE_NAME = "The Citizen"

# Date filtering - Last 7 days only
DAYS_BACK = 7
DATE_FILTER = datetime.now() - timedelta(days=DAYS_BACK)

# Maximum pages to scrape per keyword (to avoid too much data)
MAX_PAGES_PER_KEYWORD = 5

# Search keywords - comprehensive coverage
SEARCH_KEYWORDS = [
    'infrastructure',
    'construction',
    'railway',
    'port',
    'airport',
    'highway',
    'energy',
    'power',
    'electricity',
    'solar',
    'renewable',
    'technology',
    'digital',
    'telecom',
    'internet',
    '5g',
    'finance',
    'investment',
    'economy',
    'banking',
    'trade',
]


def parse_relative_date(date_str: str) -> str:
    """
    Parse relative dates like 'YESTERDAY', 'OCT 23', '11 HOURS AGO' to ISO format
    """
    date_str = date_str.upper().strip()

    try:
        # Remove time info like "- 3 min read"
        if '-' in date_str:
            date_str = date_str.split('-')[0].strip()

        # Handle relative dates
        if 'YESTERDAY' in date_str:
            date = datetime.now() - timedelta(days=1)
            return date.strftime('%Y-%m-%d')

        if 'AGO' in date_str or 'HOURS' in date_str:
            # Today
            return datetime.now().strftime('%Y-%m-%d')

        # Handle month abbreviations like "OCT 23"
        month_map = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
        }

        for month_abbr, month_num in month_map.items():
            if month_abbr in date_str:
                # Extract day number
                parts = date_str.split()
                for part in parts:
                    if part.isdigit():
                        day = int(part)
                        year = datetime.now().year
                        return f"{year}-{month_num:02d}-{day:02d}"

        # If we can't parse, return today's date
        return datetime.now().strftime('%Y-%m-%d')

    except Exception:
        return datetime.now().strftime('%Y-%m-%d')


def is_within_date_range(date_str: str) -> bool:
    """Check if date is within last 7 days"""
    if not date_str:
        return False
    try:
        article_date = datetime.strptime(date_str, '%Y-%m-%d')
        return article_date >= DATE_FILTER
    except:
        return False


def scrape_search_page(page, keyword: str, page_num: int) -> List[Dict[str, Any]]:
    """Scrape a single search results page"""
    url = f"{SEARCH_URL}?pageNum={page_num}&query={keyword}&sortByDate=true"

    try:
        page.goto(url, wait_until='domcontentloaded', timeout=30000)
        time.sleep(2)

        articles = page.query_selector_all('article')
        results = []

        for article in articles:
            try:
                # Get parent link (article is wrapped in <a> tag)
                parent = article.evaluate_handle('el => el.parentElement')
                parent_elem = parent.as_element()

                article_url = None
                if parent_elem:
                    href = parent_elem.get_attribute('href')
                    if href:
                        if href.startswith('/'):
                            article_url = BASE_URL + href
                        else:
                            article_url = href

                if not article_url:
                    continue

                # Get title
                title_elem = article.query_selector('h1, h2, h3, h4')
                title = title_elem.inner_text().strip() if title_elem else ""

                if not title:
                    continue

                # Get date
                date_elem = article.query_selector('.date, time')
                date_text = date_elem.inner_text().strip() if date_elem else ""
                date_iso = parse_relative_date(date_text)

                # Get summary
                summary_elem = article.query_selector('p, .excerpt, .summary')
                summary = summary_elem.inner_text().strip() if summary_elem else title

                # Limit summary length
                if len(summary) > 500:
                    summary = summary[:497] + "..."

                results.append({
                    'title': title,
                    'url': article_url,
                    'date_iso': date_iso,
                    'summary': summary,
                })

            except Exception as e:
                continue

        return results

    except Exception as e:
        print(f"    Error scraping page {page_num} for '{keyword}': {e}")
        return []


def scrape_thecitizen():
    """Main scraper function"""
    print(f"Starting The Citizen search scraper")
    print("=" * 60)
    print(f"Collecting articles from last {DAYS_BACK} days")
    print(f"Date filter: {DATE_FILTER.strftime('%Y-%m-%d')} to present")
    print(f"Max pages per keyword: {MAX_PAGES_PER_KEYWORD}")
    print("=" * 60)

    all_data = []
    seen_urls = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        for keyword in SEARCH_KEYWORDS:
            print(f"\n  Searching for: '{keyword}'")

            keyword_articles = 0

            for page_num in range(1, MAX_PAGES_PER_KEYWORD + 1):
                print(f"    Page {page_num}...", end=' ')

                results = scrape_search_page(page, keyword, page_num)

                if not results:
                    print("no results")
                    break

                # OPTIMIZATION: Check first article's date (results sorted by date)
                # If first article is too old, all subsequent ones will be too
                if results and not is_within_date_range(results[0]['date_iso']):
                    print(f"first article too old ({results[0]['date_iso']}), skipping to next keyword")
                    break

                page_count = 0
                for result in results:
                    # Skip duplicates
                    if result['url'] in seen_urls:
                        continue

                    # Filter by date
                    if not is_within_date_range(result['date_iso']):
                        continue

                    seen_urls.add(result['url'])

                    all_data.append({
                        'country': COUNTRY,
                        'source': SOURCE_NAME,
                        'title': result['title'].replace(',', ' '),
                        'date_iso': result['date_iso'],
                        'summary': result['summary'].replace(',', ' ').replace('\n', ' '),
                        'url': result['url'],
                        'category': '',  # Will be filled by AI
                        'status': ''     # Will be filled by AI
                    })

                    page_count += 1
                    keyword_articles += 1

                print(f"kept {page_count} articles")

                # If we got fewer than 10 results, probably no more pages
                if len(results) < 10:
                    break

            print(f"    Total for '{keyword}': {keyword_articles} articles")

        browser.close()

    # Remove articles older than 7 days (in case date parsing was inaccurate)
    filtered_data = [
        article for article in all_data
        if is_within_date_range(article['date_iso'])
    ]

    print(f"\n\nTotal unique articles collected: {len(filtered_data)}")
    return filtered_data


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
        print("\n" + "=" * 60)
        print("SCRAPING SUMMARY")
        print("=" * 60)
        print(f"Total records: {len(data)}")

        # Date coverage
        dates = [d['date_iso'] for d in data if d['date_iso']]
        if dates:
            dates.sort()
            print(f"\nDate range:")
            print(f"  Oldest: {dates[0]}")
            print(f"  Newest: {dates[-1]}")

        print("\nNote: Category and status fields are empty - will be filled by AI processing")
        print("=" * 60)

    except Exception as e:
        print(f"Error saving to CSV: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Scrape The Citizen via search endpoint')
    parser.add_argument('--output', '-o', default='thecitizen_data.csv', help='Output CSV file')

    args = parser.parse_args()

    # Run scraper
    data = scrape_thecitizen()

    # Save to CSV
    if data:
        save_to_csv(data, args.output)
    else:
        print("No articles collected!")
