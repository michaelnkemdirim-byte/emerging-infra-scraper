#!/usr/bin/env python3
"""
Generic keyword filter to detect false matches across all categories
Checks if keywords appear as standalone words vs embedded in other words
"""
import csv
import re

# Define which keywords need filtering (single words that commonly embed in other words)
KEYWORDS_TO_FILTER = {
    'port': ['sport', 'transport', 'rapport', 'apporte', 'aéroport', 'importante', 'porter', 'portée', 'portes'],
    'train': ['entraînement', 'terrain', 'entraîner'],
    'pont': ['pontage', 'spontané'],
    'route': ['déroute', 'autoroute'],  # autoroute is actually okay, but route can be in other words
    'transport': ['transporter', 'transporté', 'transporteur'],
    'construction': ['reconstruction', 'déconstruction'],
    'numérique': ['alphanumérique', 'numérique'],
    'corridor': [],  # Less common to be embedded
    'ferroviaire': [],  # Less common to be embedded
    'industrialisation': []  # Less common to be embedded
}

def has_standalone_keyword(text, keyword):
    """
    Check if keyword appears as a standalone word (not embedded in other words)

    Args:
        text: The text to search in
        keyword: The keyword to search for

    Returns:
        True if standalone keyword is found
        False if keyword only appears embedded in words
        None if keyword doesn't appear at all (ambiguous - pass for manual review)
    """
    if not text or not keyword:
        return None

    text_lower = text.lower()
    keyword_lower = keyword.lower()

    # If keyword doesn't appear at all, return None (ambiguous - let it pass for manual review)
    if keyword_lower not in text_lower:
        return None

    # Check for standalone keyword with word boundaries
    # \b matches word boundary (space, punctuation, start/end of string)
    standalone_pattern = r'\b' + re.escape(keyword_lower) + r'\b'

    if re.search(standalone_pattern, text_lower):
        return True  # Found standalone keyword

    # If we get here, keyword only appears embedded in other words
    return False

def has_standalone_port(text):
    """
    Legacy function for backwards compatibility
    Check if 'port' appears as a standalone word (not embedded in other words)

    Returns True if standalone 'port' is found
    Returns False if 'port' only appears embedded in words like sport, transport, rapport, etc.
    """
    return has_standalone_keyword(text, 'port')

def get_keyword_from_category(category):
    """Map category back to the search keyword that triggered it"""
    # This maps categories to the most specific keyword used
    category_to_keyword = {
        'port': 'port',
        'rail': 'train',  # Could also be 'chemin de fer' or 'ferroviaire'
        'highway': 'route',  # Could also be 'autoroute' or 'pont'
        'industrial zone': 'industrialisation',
        'smart city': 'numérique',
        'Infrastructure': 'infrastructure'
    }
    return category_to_keyword.get(category, None)

def test_filter(filename):
    """Test the keyword filter on a CSV file for all categories"""
    print(f"\n{'='*80}")
    print(f"TESTING KEYWORD FILTER ON: {filename}")
    print('='*80)

    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        data = list(reader)

    print(f"\nTotal articles: {len(data)}")

    # Get unique categories
    categories = set(row['category'] for row in data if row['category'])

    all_results = {}
    total_false_matches = 0
    total_legitimate = 0
    total_ambiguous = 0

    for category in sorted(categories):
        category_articles = [row for row in data if row['category'] == category]

        if not category_articles:
            continue

        # Get the keyword for this category
        keyword = get_keyword_from_category(category)

        # Only test categories with filterable keywords
        if keyword not in KEYWORDS_TO_FILTER:
            continue

        print(f"\n{'─'*80}")
        print(f"CATEGORY: {category.upper()} (keyword: '{keyword}')")
        print(f"Articles in category: {len(category_articles)}")

        # Analyze articles
        standalone = []
        embedded_only = []
        no_keyword = []

        for article in category_articles:
            # Combine title and summary for analysis
            full_text = f"{article['title']} {article['summary']}"

            result = has_standalone_keyword(full_text, keyword)

            if result is True:
                standalone.append(article)
            elif result is False:
                embedded_only.append(article)
            else:  # None
                no_keyword.append(article)

        if len(category_articles) > 0:
            print(f"\nANALYSIS RESULTS:")
            print(f"  ✅ Legitimate (standalone '{keyword}'):     {len(standalone):3d} ({len(standalone)/len(category_articles)*100:.1f}%)")
            print(f"  ❌ FALSE MATCH (embedded only):            {len(embedded_only):3d} ({len(embedded_only)/len(category_articles)*100:.1f}%)")
            print(f"  ⚠️  AMBIGUOUS (no '{keyword}' at all):     {len(no_keyword):3d} ({len(no_keyword)/len(category_articles)*100:.1f}%)")

            # Show examples of false matches
            if embedded_only:
                print(f"\n❌ EXAMPLES OF FALSE MATCHES (embedded '{keyword}' only):")
                for i, article in enumerate(embedded_only[:3], 1):
                    title = article['title'][:70]
                    # Find which word contains the keyword
                    text_lower = f"{article['title']} {article['summary']}".lower()
                    words_with_keyword = [word for word in text_lower.split() if keyword in word and word != keyword]
                    examples = ', '.join(list(set(words_with_keyword))[:3])
                    print(f"  {i}. {title}...")
                    print(f"     '{keyword}' found in: {examples}")

        all_results[category] = {
            'total': len(category_articles),
            'legitimate': len(standalone),
            'false_matches': len(embedded_only),
            'ambiguous': len(no_keyword),
            'keyword': keyword
        }

        total_false_matches += len(embedded_only)
        total_legitimate += len(standalone)
        total_ambiguous += len(no_keyword)

    print(f"\n{'='*80}")
    print(f"OVERALL SUMMARY FOR {filename}")
    print('='*80)

    total_tested = total_false_matches + total_legitimate + total_ambiguous
    if total_tested > 0:
        print(f"\nTotal articles tested: {total_tested}")
        print(f"  ✅ Legitimate:     {total_legitimate:3d} ({total_legitimate/total_tested*100:.1f}%)")
        print(f"  ❌ False matches:  {total_false_matches:3d} ({total_false_matches/total_tested*100:.1f}%)")
        print(f"  ⚠️  Ambiguous:      {total_ambiguous:3d} ({total_ambiguous/total_tested*100:.1f}%)")

    print(f"\nRECOMMENDATION:")
    print(f"  - Remove {total_false_matches} false matches")
    print(f"  - Keep {total_legitimate + total_ambiguous} articles")
    print('='*80)

    return all_results

if __name__ == "__main__":
    print("\n" + "="*80)
    print("KEYWORD FILTER TEST - BURKINA FASO SCRAPERS")
    print("Testing all categories: port, rail, highway, industrial zone, smart city, Infrastructure")
    print("="*80)

    files = [
        'faso7_data.csv',
        'leconomiste_data.csv',
        'gouvernement_data.csv'
    ]

    all_files_results = {}
    for filename in files:
        try:
            all_files_results[filename] = test_filter(filename)
        except Exception as e:
            print(f"\n❌ Error processing {filename}: {e}")

    # Overall summary across all files
    print(f"\n\n{'='*80}")
    print("GRAND TOTAL - ALL FILES & ALL CATEGORIES")
    print('='*80)

    grand_total_false = 0
    grand_total_legitimate = 0
    grand_total_ambiguous = 0
    grand_total_articles = 0

    for filename, categories in all_files_results.items():
        for category, stats in categories.items():
            grand_total_false += stats['false_matches']
            grand_total_legitimate += stats['legitimate']
            grand_total_ambiguous += stats['ambiguous']
            grand_total_articles += stats['total']

    if grand_total_articles > 0:
        print(f"\nTotal articles tested across all files: {grand_total_articles}")
        print(f"  ✅ Legitimate:     {grand_total_legitimate:3d} ({grand_total_legitimate/grand_total_articles*100:.1f}%)")
        print(f"  ❌ False matches:  {grand_total_false:3d} ({grand_total_false/grand_total_articles*100:.1f}%)")
        print(f"  ⚠️  Ambiguous:      {grand_total_ambiguous:3d} ({grand_total_ambiguous/grand_total_articles*100:.1f}%)")

        print(f"\n🎯 FINAL RECOMMENDATION:")
        print(f"  Filtering will remove {grand_total_false} false matches")
        print(f"  and keep {grand_total_legitimate + grand_total_ambiguous} articles")
        print(f"  Cleaning {grand_total_false}/{grand_total_articles} ({grand_total_false/grand_total_articles*100:.1f}%) of tested articles")

    print("="*80)
