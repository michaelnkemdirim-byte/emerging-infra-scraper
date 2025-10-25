#!/usr/bin/env python3
"""
Engineering News - Patchright Scraper
Scrapes infrastructure news from engineeringnews.co.za
Uses Patchright for antibot bypass
"""

import asyncio
import csv
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any
from patchright.async_api import async_playwright

# Configuration
BASE_URL = "https://www.engineeringnews.co.za"
COUNTRY = "South Africa"
SOURCE_NAME = "Engineering News"
DAYS_BACK = 30

# Calculate date 30 days ago
NOW = datetime.now()
DATE_30_DAYS_AGO = NOW - timedelta(days=DAYS_BACK)

# Categories to scrape with mapping to our project categories
CATEGORIES = {
    'infrastructure': 'Infrastructure',
    'construction': 'Infrastructure',
    'ports': 'port',
    'rail': 'rail',
    'roads': 'highway',
    'transport': 'Infrastructure'
}


def parse_date_from_url(url: str) -> str:
    """Extract date from URL pattern: /article/title-2025-10-20"""
    try:
        match = re.search(r'-(\d{4}-\d{2}-\d{2})$', url)
        if match:
            return match.group(1)
    except:
        pass
    return ""


def is_within_30_days(date_str: str) -> bool:
    """Check if date is within last 30 days"""
    if not date_str:
        return False
    try:
        article_date = datetime.strptime(date_str, '%Y-%m-%d')
        return article_date >= DATE_30_DAYS_AGO
    except:
        return False


async def scrape_category(page, category: str, our_category: str) -> List[Dict[str, Any]]:
    """Scrape articles from a specific category"""
    print(f"\n  Scraping category: {category} → {our_category}")
    url = f"{BASE_URL}/page/{category}"

    try:
        await page.goto(url, wait_until='networkidle', timeout=60000)
        await asyncio.sleep(2)  # Wait for dynamic content

        # Find all article links
        article_links = await page.query_selector_all('a[href*="/article/"]')
        print(f"    Found {len(article_links)} article links")

        articles_data = []
        seen_urls = set()

        for link in article_links:
            try:
                href = await link.get_attribute('href')
                title_text = await link.inner_text()

                if not href or not title_text or title_text.strip() == '':
                    continue

                # Make absolute URL
                if href.startswith('/'):
                    full_url = BASE_URL + href
                else:
                    full_url = href

                # Skip duplicates
                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                # Extract date from URL
                date_iso = parse_date_from_url(href)

                # Filter: only last 30 days
                if not is_within_30_days(date_iso):
                    continue

                # Get summary from parent element
                parent = await link.evaluate_handle('el => el.closest("div.entry, article, .article, div[class*=item]")')
                summary = ""
                if parent:
                    try:
                        summary_elem = await parent.query_selector('p, .summary, [class*="summary"], [class*="excerpt"]')
                        if summary_elem:
                            summary = await summary_elem.inner_text()
                    except:
                        pass

                # If no summary, use title
                if not summary or len(summary) < 20:
                    summary = title_text

                # Create summary (max 500 chars)
                summary = summary[:497] + "..." if len(summary) > 500 else summary

                articles_data.append({
                    'country': COUNTRY,
                    'source': SOURCE_NAME,
                    'title': title_text.strip().replace(',', ' '),
                    'date_iso': date_iso,
                    'summary': summary.strip().replace(',', ' ').replace('\n', ' '),
                    'url': full_url,
                    'category': our_category,  # Pre-filled from Engineering News category
                    'status': ''     # Will be filled by AI
                })

            except Exception as e:
                continue

        print(f"    Collected {len(articles_data)} articles from last 30 days")
        return articles_data

    except Exception as e:
        print(f"    Error scraping {category}: {e}")
        return []


async def scrape_all_categories():
    """Scrape all infrastructure categories"""
    print(f"Starting Patchright scraper for {SOURCE_NAME}")
    print("="*60)
    print(f"Collecting articles from last {DAYS_BACK} days")
    print(f"Date filter: {DATE_30_DAYS_AGO.strftime('%Y-%m-%d')} to present")
    print("="*60)

    async with async_playwright() as p:
        # Launch browser with antibot bypass
        print("\nLaunching browser...")
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        )
        page = await context.new_page()

        all_data = []
        seen_urls = set()
        seen_titles = set()  # Track titles to avoid duplicates

        for category, our_category in CATEGORIES.items():
            articles = await scrape_category(page, category, our_category)

            # Deduplicate across categories
            for article in articles:
                if article['url'] not in seen_urls and article['title'] not in seen_titles:
                    seen_urls.add(article['url'])
                    seen_titles.add(article['title'])
                    all_data.append(article)

            await asyncio.sleep(1)  # Be polite between categories

        await browser.close()

        print(f"\n\nTotal unique articles collected: {len(all_data)}")
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

        # Count by category
        from collections import Counter
        cat_counts = Counter([d['category'] for d in data])
        print("\nArticles by category:")
        for cat, count in cat_counts.items():
            print(f"  {cat}: {count}")

        print("\nNote: Category pre-filled from Engineering News, status will be filled by AI processing")
        print("="*60)

    except Exception as e:
        print(f"Error saving to CSV: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Scrape Engineering News (Patchright)')
    parser.add_argument('--output', '-o', default='engineeringnews_data.csv', help='Output CSV file')

    args = parser.parse_args()

    # Run scraper
    data = asyncio.run(scrape_all_categories())

    # Save to CSV
    if data:
        save_to_csv(data, args.output)
    else:
        print("No articles collected!")
