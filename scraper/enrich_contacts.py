#!/usr/bin/env python3
"""
DiRoNA Contact Enrichment Tool v1.0
Enriches restaurant CSV files with:
- Email addresses
- Social media links (Instagram, Facebook, Twitter/X)
- Owner/Chef names

Usage:
  python3 scraper/enrich_contacts.py output/dirona_leads_Delaware_DE_20260303.csv
"""

import csv
import re
import time
import sys
import os
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

# ============================================================
# CONFIGURATION
# ============================================================

# Request settings
REQUEST_TIMEOUT = 10
REQUEST_DELAY = 2  # Delay between requests to be polite

# User agent (pretend to be a browser)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# New CSV columns to add
NEW_COLUMNS = [
    "Email",
    "Instagram",
    "Facebook",
    "Twitter",
    "Owner/Chef Name",
    "Enrichment Status"
]

# ============================================================
# WEB SCRAPING FUNCTIONS
# ============================================================

def fetch_website(url):
    """
    Fetch website HTML content.
    Returns BeautifulSoup object or None if failed.
    """
    if not url or url.strip() == "":
        return None
    
    try:
        # Add https:// if missing
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        
        if response.status_code == 200:
            return BeautifulSoup(response.text, 'html.parser')
        else:
            return None
            
    except Exception as e:
        print(f"      Error fetching {url}: {str(e)[:50]}")
        return None

def find_emails(soup, base_url):
    """
    Find email addresses on the page.
    Returns list of unique emails.
    """
    emails = set()
    
    if not soup:
        return list(emails)
    
    # Method 1: Find mailto: links
    for link in soup.find_all('a', href=re.compile(r'^mailto:', re.I)):
        email = link.get('href').replace('mailto:', '').split('?')[0].strip()
        if '@' in email:
            emails.add(email.lower())
    
    # Method 2: Find email patterns in text
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    page_text = soup.get_text()
    found_emails = re.findall(email_pattern, page_text)
    
    for email in found_emails:
        # Filter out common false positives
        if not any(x in email.lower() for x in ['example.com', 'yourdomain.com', 'domain.com']):
            emails.add(email.lower())
    
    return list(emails)[:3]  # Return up to 3 emails

def find_social_media(soup, base_url):
    """
    Find social media links.
    Returns dict with Instagram, Facebook, Twitter URLs.
    """
    social = {
        'instagram': '',
        'facebook': '',
        'twitter': ''
    }
    
    if not soup:
        return social
    
    # Find all links
    for link in soup.find_all('a', href=True):
        href = link.get('href', '').lower()
        
        # Instagram
        if 'instagram.com' in href and not social['instagram']:
            social['instagram'] = link.get('href')
        
        # Facebook
        elif 'facebook.com' in href and not social['facebook']:
            social['facebook'] = link.get('href')
        
        # Twitter/X
        elif ('twitter.com' in href or 'x.com' in href) and not social['twitter']:
            social['twitter'] = link.get('href')
    
    return social

def find_owner_chef(soup):
    """
    Attempt to find owner or chef name.
    Returns name string or empty string.
    """
    if not soup:
        return ''
    
    # Common patterns to look for
    patterns = [
        r'chef[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)',
        r'owner[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)',
        r'executive chef[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)',
        r'chef/owner[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)',
        r'founded by[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)',
    ]
    
    page_text = soup.get_text()
    
    for pattern in patterns:
        matches = re.findall(pattern, page_text, re.IGNORECASE)
        if matches:
            return matches[0].strip()
    
    return ''

def enrich_restaurant(row):
    """
    Enrich a single restaurant row with contact info.
    Returns updated row dict.
    """
    website = row.get('Website', '').strip()
    name = row.get('Restaurant Name', 'Unknown')
    
    print(f"\n  Enriching: {name}")
    print(f"    Website: {website}")
    
    # Initialize new fields
    row['Email'] = ''
    row['Instagram'] = ''
    row['Facebook'] = ''
    row['Twitter'] = ''
    row['Owner/Chef Name'] = ''
    row['Enrichment Status'] = 'No Website'
    
    if not website:
        print(f"    ⚠️  No website available")
        return row
    
    # Fetch website
    soup = fetch_website(website)
    
    if not soup:
        row['Enrichment Status'] = 'Failed to Load'
        print(f"    ❌ Failed to load website")
        return row
    
    # Find emails
    emails = find_emails(soup, website)
    if emails:
        row['Email'] = ', '.join(emails)
        print(f"    ✅ Email: {row['Email']}")
    
    # Find social media
    social = find_social_media(soup, website)
    if social['instagram']:
        row['Instagram'] = social['instagram']
        print(f"    ✅ Instagram: {social['instagram']}")
    if social['facebook']:
        row['Facebook'] = social['facebook']
        print(f"    ✅ Facebook: {social['facebook']}")
    if social['twitter']:
        row['Twitter'] = social['twitter']
        print(f"    ✅ Twitter: {social['twitter']}")
    
    # Find owner/chef
    owner = find_owner_chef(soup)
    if owner:
        row['Owner/Chef Name'] = owner
        print(f"    ✅ Owner/Chef: {owner}")
    
    # Set status
    if any([row['Email'], row['Instagram'], row['Facebook'], row['Twitter'], row['Owner/Chef Name']]):
        row['Enrichment Status'] = 'Success'
        print(f"    ✅ Enrichment successful")
    else:
        row['Enrichment Status'] = 'No Data Found'
        print(f"    ⚠️  No additional data found")
    
    return row

# ============================================================
# MAIN ENRICHMENT PROCESS
# ============================================================

def enrich_csv(input_file):
    """
    Read CSV, enrich each restaurant, write enriched CSV.
    """
    if not os.path.exists(input_file):
        print(f"\n❌ ERROR: File not found: {input_file}")
        return
    
    # Generate output filename
    base_name = os.path.splitext(input_file)[0]
    output_file = f"{base_name}_enriched.csv"
    
    print(f"\n{'='*70}")
    print(f"  DiRoNA Contact Enrichment Tool v1.0")
    print(f"{'='*70}")
    print(f"  Input File  : {input_file}")
    print(f"  Output File : {output_file}")
    print(f"{'='*70}\n")
    
    # Read input CSV
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        original_columns = reader.fieldnames
    
    print(f"  Found {len(rows)} restaurants to enrich\n")
    
    # Add new columns
    enriched_columns = list(original_columns) + NEW_COLUMNS
    
    # Enrich each row
    enriched_rows = []
    for idx, row in enumerate(rows, 1):
        print(f"\n[{idx}/{len(rows)}] ", end="")
        enriched_row = enrich_restaurant(row)
        enriched_rows.append(enriched_row)
        
        # Be polite with delays
        if idx < len(rows):
            time.sleep(REQUEST_DELAY)
    
    # Write enriched CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=enriched_columns)
        writer.writeheader()
        writer.writerows(enriched_rows)
    
    # Stats
    success_count = sum(1 for r in enriched_rows if r['Enrichment Status'] == 'Success')
    email_count = sum(1 for r in enriched_rows if r['Email'])
    instagram_count = sum(1 for r in enriched_rows if r['Instagram'])
    facebook_count = sum(1 for r in enriched_rows if r['Facebook'])
    twitter_count = sum(1 for r in enriched_rows if r['Twitter'])
    owner_count = sum(1 for r in enriched_rows if r['Owner/Chef Name'])
    
    print(f"\n\n{'='*70}")
    print(f"  ✅ ENRICHMENT COMPLETE!")
    print(f"{'='*70}")
    print(f"  Total Restaurants     : {len(rows)}")
    print(f"  Successfully Enriched : {success_count}")
    print(f"  Emails Found          : {email_count}")
    print(f"  Instagram Links       : {instagram_count}")
    print(f"  Facebook Links        : {facebook_count}")
    print(f"  Twitter Links         : {twitter_count}")
    print(f"  Owner/Chef Names      : {owner_count}")
    print(f"  Output File           : {output_file}")
    print(f"{'='*70}\n")

# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("\n❌ ERROR: Please provide a CSV file to enrich")
        print("\nUsage:")
        print("  python3 scraper/enrich_contacts.py output/dirona_leads_Delaware_DE_20260303.csv")
        print("\nThis will create: output/dirona_leads_Delaware_DE_20260303_enriched.csv\n")
        sys.exit(1)
    
    input_file = sys.argv[1]
    enrich_csv(input_file)
