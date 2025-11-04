#!/usr/bin/env python3
"""
Categorize infrastructure news articles using Anthropic API
Categories: NonInfra, port, rail, highway, SEZ, smart city, Infrastructure
"""

import os
import csv
import json
import time
from pathlib import Path
import anthropic

# Configuration
SECRETS_FILE = Path(__file__).parent / "secrets.toml"

def load_anthropic_key():
    """Load Anthropic API key from secrets.toml"""
    try:
        import toml
        with open(SECRETS_FILE, 'r') as f:
            secrets = toml.load(f)
            return secrets.get('anthropicAPI')
    except Exception as e:
        print(f"Error loading API key from secrets.toml: {e}")
        return os.getenv("ANTHROPIC_API_KEY")

ANTHROPIC_API_KEY = load_anthropic_key()
ANTHROPIC_MODEL = "claude-sonnet-4-5-20250929"  # Sonnet 4.5 - most capable model
BATCH_SIZE = 10  # Categorize 10 articles per API call (smaller batches for reliability)
# No threading - sequential processing to respect rate limits

# Categories
CATEGORIES = ["NonInfra", "port", "rail", "highway", "SEZ", "smart city", "economic", "energy", "technology", "Infrastructure"]


def categorize_batch(client, articles_batch):
    """
    Categorize a batch of articles using Anthropic API

    Args:
        client: Anthropic API client
        articles_batch: List of dicts with 'title' and 'summary'

    Returns:
        List of category strings
    """
    # Build optimized prompt aligned with project goals
    prompt = f"""You classify African infrastructure and development projects. Focus on actual projects, not general corporate news.

CATEGORIES (choose ONE per article):
• port: ONLY if mentions ports, airports, maritime, shipping, cargo terminals, harbours, vessels
• rail: ONLY if mentions railways, trains, metro, rail tracks, stations, locomotives
• highway: ONLY if mentions roads, highways, bridges, expressways, motorways
• SEZ: ONLY if mentions Special Economic Zones, industrial parks, free trade zones
• smart city: Smart city initiatives, digital infrastructure, e-government, urban tech
• economic: Finance, trade, economy, crypto, investment, fintech, stock exchange, banking, commerce, export/import
• energy: Solar, wind, hydropower, nuclear, thermal, renewable energy, power plants, electricity, grid, dams
• technology: Technology projects, digital transformation, broadband, 5G, data centers, ICT, fiber optic, AI, cybersecurity, telecommunications
• Infrastructure: Water projects, housing, buildings, general construction, recycling, waste
• NonInfra: NOT a project (HR news, awards, earnings, general announcements) from above categories and NOT infrastructure-related and out of context

CRITICAL RULES:
- port/rail/highway/SEZ: Must have explicit keywords. If no clear keywords, use "Infrastructure"
- economic: Financial/trade projects, markets, banking, investment initiatives
- energy: Power generation, renewable energy, electricity infrastructure
- technology: Digital/ICT projects, telecom infrastructure, tech hubs
- smart city: Urban development with digital/tech focus
- Infrastructure: Default for any development project that doesn't clearly fit above categories
- NonInfra: Only for non-project content (appointments, awards, earnings, generic news)
- When in doubt: "Infrastructure" is safer than guessing specific categories

ARTICLES:
"""

    # Add each article with index
    for i, article in enumerate(articles_batch):
        title = article.get('title', '')
        summary = article.get('summary', '')
        # Use more summary text for better context
        prompt += f"\n[{i}] Title: {title}\nSummary: {summary[:500]}\n"

    prompt += f"""
Return JSON array with {len(articles_batch)} categories. No explanations. Example: ["port", "economic", "energy", "technology", "Infrastructure"]

["""

    # Retry for rate limiting and incomplete responses
    max_retries = 3
    response = None

    for attempt in range(max_retries):
        try:
            # Call Anthropic API with optimized settings
            response = client.messages.create(
                model=ANTHROPIC_MODEL,
                max_tokens=2000,  # Increased to ensure complete responses
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2  # Slightly higher for better reasoning
            )

            # Check if we got a complete response
            try:
                content = response.content[0].text.strip()
                # Better validation - check for JSON array markers
                has_opening_bracket = '[' in content
                has_closing_bracket = ']' in content

                if not (has_opening_bracket and has_closing_bracket):
                    if attempt < max_retries - 1:
                        print(f"  ⚠️  Incomplete response, retrying {attempt + 2}/{max_retries}...")
                        continue
                break  # Response looks good, exit retry loop
            except:
                if attempt < max_retries - 1:
                    continue
                break

        except Exception as api_error:
            error_str = str(api_error)

            # Handle 429 rate limit errors
            if '429' in error_str or 'rate' in error_str.lower() or 'exceeded' in error_str.lower():
                if attempt < max_retries - 1:
                    print(f"  ⚠️  Rate limit hit, retrying immediately {attempt + 2}/{max_retries}...")
                    continue
                else:
                    print(f"  ❌ Rate limit persists after {max_retries} retries")
                    return ["Infrastructure" for _ in articles_batch]  # Changed from NonInfra
            else:
                # Non-rate-limit error
                print(f"  ❌ API error: {error_str[:100]}")
                return ["Infrastructure" for _ in articles_batch]  # Changed from NonInfra

    if response is None:
        return ["Infrastructure" for _ in articles_batch]  # Changed from NonInfra

    try:
        # Extract response
        content = response.content[0].text.strip()

        # Remove markdown code blocks if present
        if content.startswith('```'):
            content = content.split('```')[1]
            if content.startswith('json'):
                content = content[4:]
            content = content.strip()

        # Parse JSON
        results = json.loads(content)

        # Validate results
        if not isinstance(results, list):
            print(f"  ❌ Error: Invalid response format (not a list)")
            return ["NonInfra" for _ in articles_batch]

        # Handle partial results - pad with "Infrastructure" instead of losing data
        if len(results) != len(articles_batch):
            print(f"  ⚠️  Warning: Expected {len(articles_batch)} results, got {len(results)}")
            # Pad with "Infrastructure" (safer than NonInfra which discards data)
            while len(results) < len(articles_batch):
                results.append("Infrastructure")
            # Truncate if too many
            results = results[:len(articles_batch)]

        # Validate each result
        validated_results = []
        for result in results:
            if isinstance(result, str):
                category = result
            elif isinstance(result, dict):
                category = result.get('category', 'NonInfra')
            else:
                category = "NonInfra"

            if category not in CATEGORIES:
                category = "NonInfra"

            validated_results.append(category)

        return validated_results

    except json.JSONDecodeError as e:
        print(f"  ❌ JSON parsing error: {e}")
        print(f"  Response was: {content[:200]}")
        return ["NonInfra" for _ in articles_batch]
    except Exception as e:
        print(f"  ❌ API error: {e}")
        return ["NonInfra" for _ in articles_batch]


def categorize_csv_file(csv_file, api_key):
    """
    Categorize all articles in a CSV file

    Args:
        csv_file: Path to CSV file
        api_key: Anthropic API key
    """
    csv_path = Path(csv_file)

    if not csv_path.exists():
        print(f"❌ File not found: {csv_file}")
        return

    print(f"\n📁 Processing: {csv_path.name}")

    # Initialize Anthropic client
    client = anthropic.Anthropic(api_key=api_key)

    # Read CSV
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    total_rows = len(rows)
    print(f"  Total articles: {total_rows}")

    # Sources with UNRELIABLE pre-filled categories (need re-categorization)
    FORCE_RECATEGORIZE_SOURCES = [
        'L\'Economiste du Faso',  # Keyword matching is too broad, creates false positives
    ]

    # Only categorize rows that DON'T already have a category OR are from unreliable sources
    rows_to_categorize = [
        (i, row) for i, row in enumerate(rows)
        if not row.get('category', '').strip() or  # Empty category = needs categorization
           row.get('source') in FORCE_RECATEGORIZE_SOURCES  # Unreliable pre-fills
    ]

    preserved_count = total_rows - len(rows_to_categorize)
    if preserved_count > 0:
        print(f"  ⏭️  Preserved {preserved_count} articles from specialized infrastructure sources")

    print(f"  Articles to categorize: {len(rows_to_categorize)} (General news sources only)")

    # Process in batches
    categorized_count = 0

    for batch_start in range(0, len(rows_to_categorize), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(rows_to_categorize))
        batch = rows_to_categorize[batch_start:batch_end]

        # Extract articles for categorization
        articles_batch = [row for _, row in batch]

        # Categorize batch
        print(f"  Categorizing batch {batch_start//BATCH_SIZE + 1}/{(len(rows_to_categorize)-1)//BATCH_SIZE + 1}...", end=' ')
        results = categorize_batch(client, articles_batch)

        # Update rows with results (category only)
        for (row_idx, row), category in zip(batch, results):
            rows[row_idx]['category'] = category
            categorized_count += 1

        print(f"✓ ({categorized_count}/{len(rows_to_categorize)})")

    # Filter out NonInfra rows before saving
    original_count = len(rows)
    rows = [row for row in rows if row.get('category') != 'NonInfra']
    removed_count = original_count - len(rows)

    if removed_count > 0:
        print(f"  🗑️  Removed {removed_count} NonInfra articles")

    # Write updated CSV (preserve all existing fields)
    with open(csv_path, 'w', encoding='utf-8', newline='') as f:
        fieldnames = rows[0].keys() if rows else ['country', 'source', 'title', 'date_iso', 'summary', 'url', 'category']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  ✅ Categorized {categorized_count} articles → Kept {len(rows)} infrastructure articles")

    # Print category breakdown
    category_counts = {}
    for row in rows:
        cat = row.get('category', 'unknown')
        category_counts[cat] = category_counts.get(cat, 0) + 1

    print(f"  📊 Category breakdown:")
    for cat in CATEGORIES:
        count = category_counts.get(cat, 0)
        if count > 0:
            print(f"     - {cat}: {count}")


def categorize_all_csvs(root_dir, api_key):
    """
    Categorize combined_data.csv ONLY (not individual country CSVs)

    Args:
        root_dir: Root directory containing combined_data.csv
        api_key: Anthropic API key
    """
    root_path = Path(root_dir)

    print("="*80)
    print("INFRASTRUCTURE NEWS CATEGORIZATION")
    print("="*80)

    # ONLY process combined_data.csv (already merged and translated by master_scraper)
    combined_csv = root_path / 'combined_data.csv'

    if not combined_csv.exists():
        print("\n❌ Error: combined_data.csv not found!")
        print("   Run master_scraper.py first to create the combined file.")
        return

    print(f"\nProcessing: {combined_csv.name}")

    # Categorize the combined file
    categorize_csv_file(combined_csv, api_key)

    print("\n" + "="*80)
    print("✅ CATEGORIZATION COMPLETE")
    print("="*80)


def main():
    """Main function"""
    # Check API key
    if not ANTHROPIC_API_KEY:
        print("❌ Error: ANTHROPIC_API_KEY not found in credentials.txt")
        print("   Add it to credentials.txt: anthropicAPI=your-api-key")
        return

    # Get root directory
    script_dir = Path(__file__).parent

    # Categorize all CSVs
    categorize_all_csvs(script_dir, ANTHROPIC_API_KEY)


if __name__ == "__main__":
    main()
