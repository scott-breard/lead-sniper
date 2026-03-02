#!/usr/bin/env python3
"""
DiRoNA Lead Scraper v2 - Enhanced Bot Evasion
Searches Google Maps ZIP code by ZIP code with advanced anti-detection.
Processes states in strict alphabetical order (Alabama → Wyoming).
"""

import csv
import time
import random
import re
import os
import json
from datetime import datetime
from playwright.sync_api import sync_playwright

# ============================================================
# CONFIGURATION - Enhanced for Bot Evasion
# ============================================================

SEARCH_CATEGORIES = [
    "fine dining restaurant",
    "steakhouse",
    "french restaurant",
    "italian restaurant",
    "seafood restaurant",
]

EXCLUDE_KEYWORDS = [
    "buffet", "chinese", "brazilian", "all you can eat", "all-you-can-eat",
    "pizzeria", "pizza", "gastropub", "thai", "indian",
    "fast food", "fast casual", "mcdonalds", "subway", "chipotle",
]

MIN_RATING      = 4.0
MIN_PRICE_LEVEL = 3
MIN_REVIEWS     = 50
DELAY_MIN       = 6.0    # Increased from 2.5
DELAY_MAX       = 12.0   # Increased from 5.0
RETRY_ATTEMPTS  = 2      # Retry failed searches

OUTPUT_DIR    = "output"
PROGRESS_FILE = "scraper/progress.json"

# Rotating user agents
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
]

# Random viewport sizes (desktop)
VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1680, "height": 1050},
]

CSV_COLUMNS = [
    "Restaurant Name", "Contact Name", "Street Address", "City", "ZIP",
    "State", "Country", "Price Level", "Description", "Phone", "Email",
    "Website", "Cuisine Type", "Has Bar", "Review Count", "Rating",
    "Facebook", "Twitter", "Instagram", "Google Maps URL",
    "Search Category", "ZIP Searched", "Date Scraped",
]

STATES_ALPHABETICAL = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California",
    "Colorado", "Connecticut", "Delaware", "Florida", "Georgia",
    "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa",
    "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland",
    "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri",
    "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey",
    "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
    "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina",
    "South Dakota", "Tennessee", "Texas", "Utah", "Vermont",
    "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming",
]

STATE_ZIPS = {
    "Alabama":        list(range(35004, 36926)),
    "Alaska":         list(range(99501, 99951)),
    "Arizona":        list(range(85001, 86557)),
    "Arkansas":       list(range(71601, 72960)),
    "California":     list(range(90001, 96163)),
    "Colorado":       list(range(80001, 81659)),
    "Connecticut":    list(range(6001,  6929)),
    "Delaware":       list(range(19701, 19981)),
    "Florida":        list(range(32004, 34998)),
    "Georgia":        list(range(30001, 31999)),
    "Hawaii":         list(range(96701, 96899)),
    "Idaho":          list(range(83201, 83877)),
    "Illinois":       list(range(60001, 62999)),
    "Indiana":        list(range(46001, 47998)),
    "Iowa":           list(range(50001, 52809)),
    "Kansas":         list(range(66002, 67955)),
    "Kentucky":       list(range(40003, 42789)),
    "Louisiana":      list(range(70001, 71498)),
    "Maine":          list(range(3901,  4993)),
    "Maryland":       list(range(20601, 21931)),
    "Massachusetts":  list(range(1001,  2792)),
    "Michigan":       list(range(48001, 49972)),
    "Minnesota":      list(range(55001, 56764)),
    "Mississippi":    list(range(38601, 39777)),
    "Missouri":       list(range(63001, 65900)),
    "Montana":        list(range(59001, 59938)),
    "Nebraska":       list(range(68001, 69368)),
    "Nevada":         list(range(88901, 89884)),
    "New Hampshire":  list(range(3031,  3898)),
    "New Jersey":     list(range(7001,  8990)),
    "New Mexico":     list(range(87001, 88442)),
    "New York":       list(range(10001, 14976)),
    "North Carolina": list(range(27006, 28910)),
    "North Dakota":   list(range(58001, 58857)),
    "Ohio":           list(range(43001, 45999)),
    "Oklahoma":       list(range(73001, 74967)),
    "Oregon":         list(range(97001, 97921)),
    "Pennsylvania":   list(range(15001, 19641)),
    "Rhode Island":   list(range(2801,  2941)),
    "South Carolina": list(range(29001, 29949)),
    "South Dakota":   list(range(57001, 57799)),
    "Tennessee":      list(range(37010, 38590)),
    "Texas":          list(range(73301, 79999)),
    "Utah":           list(range(84001, 84785)),
    "Vermont":        list(range(5001,  5908)),
    "Virginia":       list(range(20101, 24659)),
    "Washington":     list(range(98001, 99404)),
    "West Virginia":  list(range(24701, 26887)),
    "Wisconsin":      list(range(53001, 54991)),
    "Wyoming":        list(range(82001, 83129)),
}

# ============================================================
# HELPERS
# ============================================================

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"completed_zips": [], "completed_states": []}

def save_progress(progress):
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)

def is_excluded(name, cuisine):
    text = (name + " " + cuisine).lower()
    return any(kw in text for kw in EXCLUDE_KEYWORDS)

def parse_price_level(price_str):
    return price_str.count("$") if price_str else 0

def random_delay(min_delay=None, max_delay=None):
    """Human-like random delay."""
    delay = random.uniform(min_delay or DELAY_MIN, max_delay or DELAY_MAX)
    print(f"    Pausing {delay:.1f}s...")
    time.sleep(delay)

def get_output_filename(state):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    date_str   = datetime.now().strftime("%Y%m%d")
    safe_state = state.replace(" ", "_")
    return os.path.join(OUTPUT_DIR, f"dirona_leads_{safe_state}_{date_str}.csv")

def write_csv(filepath, rows):
    file_exists = os.path.exists(filepath)
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

def print_state_header(state, state_num, total_states):
    print(f"\n{'='*60}")
    print(f"  STATE {state_num}/{total_states}: {state.upper()}")
    print(f"{'='*60}")

def human_like_scroll(page):
    """Simulate human scrolling behavior."""
    for _ in range(random.randint(2, 4)):
        page.keyboard.press("End")
        time.sleep(random.uniform(0.8, 1.8))

# ============================================================
# ENHANCED SCRAPER WITH RETRY LOGIC
# ============================================================

def scrape_zip_category(page, category, zip_code, attempt=1):
    """
    Search Google Maps for one category + ZIP with retry logic.
    Returns list of qualifying restaurant dicts.
    """
    results = []
    url     = f"https://www.google.com/maps/search/{category.replace(' ', '+')}+{zip_code}"

    try:
        # Navigate with longer timeout
        page.goto(url, timeout=45000, wait_until="domcontentloaded")
        time.sleep(random.uniform(3, 5))  # Let page settle

        # Human-like scrolling
        human_like_scroll(page)

        # Collect listings
        listings   = page.query_selector_all('a[href*="/maps/place/"]')
        seen_hrefs = set()

        if not listings:
            print(f"      No listings found")
            return []

        for listing in listings[:15]:  # Reduced from 20 to be safer
            href = listing.get_attribute("href")
            if not href or href in seen_hrefs:
                continue
            seen_hrefs.add(href)

            try:
                # Wait before clicking
                time.sleep(random.uniform(0.5, 1.2))
                listing.scroll_into_view_if_needed()
                time.sleep(random.uniform(0.3, 0.7))
                listing.click(timeout=5000)
                time.sleep(random.uniform(2.5, 4.0))  # Longer wait after click

                # Extract data
                name = ""
                name_el = page.query_selector("h1.DUwDvf")
                if name_el:
                    name = name_el.inner_text().strip()
                if not name:
                    page.go_back(timeout=10000)
                    time.sleep(random.uniform(1, 2))
                    continue

                # Rating
                rating = 0.0
                rating_el = page.query_selector("span.ceNzKf")
                if rating_el:
                    m = re.search(r"([\d\.]+)", rating_el.get_attribute("aria-label") or "")
                    if m:
                        rating = float(m.group(1))
                if rating < MIN_RATING:
                    page.go_back(timeout=10000)
                    time.sleep(random.uniform(1, 2))
                    continue

                # Reviews
                reviews = 0
                review_el = page.query_selector('span[aria-label*="reviews"]')
                if review_el:
                    m = re.search(r"([\d,]+)", review_el.get_attribute("aria-label") or "")
                    if m:
                        reviews = int(m.group(1).replace(",", ""))
                if reviews < MIN_REVIEWS:
                    page.go_back(timeout=10000)
                    time.sleep(random.uniform(1, 2))
                    continue

                # Price
                price_str = ""
                price_el  = page.query_selector('span[aria-label*="Price"]')
                if price_el:
                    price_str = re.sub(r"[^$]", "", price_el.get_attribute("aria-label") or "")
                if parse_price_level(price_str) < MIN_PRICE_LEVEL:
                    page.go_back(timeout=10000)
                    time.sleep(random.uniform(1, 2))
                    continue

                # Cuisine
                cuisine = ""
                cat_el  = page.query_selector('button[jsaction*="category"]')
                if cat_el:
                    cuisine = cat_el.inner_text().strip()
                if is_excluded(name, cuisine):
                    page.go_back(timeout=10000)
                    time.sleep(random.uniform(1, 2))
                    continue

                # Address, phone, website, description
                address = ""
                addr_el = page.query_selector('button[data-item-id="address"]')
                if addr_el:
                    address = addr_el.inner_text().strip()

                phone = ""
                phone_el = page.query_selector('button[data-item-id^="phone"]')
                if phone_el:
                    phone = phone_el.inner_text().strip()

                website = ""
                web_el = page.query_selector('a[data-item-id="authority"]')
                if web_el:
                    website = web_el.get_attribute("href") or ""

                description = ""
                desc_el = page.query_selector("div.PYvSYb")
                if desc_el:
                    description = desc_el.inner_text().strip()

                results.append({
                    "Restaurant Name": name,
                    "Contact Name":    "",
                    "Street Address":  address,
                    "City":            "",
                    "ZIP":             zip_code,
                    "State":           "",
                    "Country":         "United States",
                    "Price Level":     price_str,
                    "Description":     description,
                    "Phone":           phone,
                    "Email":           "",
                    "Website":         website,
                    "Cuisine Type":    cuisine,
                    "Has Bar":         "",
                    "Review Count":    reviews,
                    "Rating":          rating,
                    "Facebook":        "",
                    "Twitter":         "",
                    "Instagram":       "",
                    "Google Maps URL": page.url,
                    "Search Category": category,
                    "ZIP Searched":    zip_code,
                    "Date Scraped":    datetime.now().strftime("%Y-%m-%d"),
                })

                page.go_back(timeout=10000)
                time.sleep(random.uniform(1.5, 2.5))

            except Exception as e:
                print(f"      Listing error: {e}")
                try:
                    page.go_back(timeout=5000)
                    time.sleep(random.uniform(1, 2))
                except:
                    pass

    except Exception as e:
        error_msg = str(e)
        print(f"    Search error [{category} / {zip_code}]: {error_msg[:100]}")
        
        # Retry logic for context destroyed errors
        if "context" in error_msg.lower() and attempt < RETRY_ATTEMPTS:
            print(f"    Retrying (attempt {attempt + 1}/{RETRY_ATTEMPTS})...")
            time.sleep(random.uniform(8, 15))  # Longer wait before retry
            return scrape_zip_category(page, category, zip_code, attempt + 1)

    return results


# ============================================================
# MAIN RUNNER
# ============================================================

def run_scraper(state="Alabama"):
    """
    Run ZIP-by-ZIP scraper with enhanced bot evasion.
    """
    if state == "ALL":
        states_to_run = STATES_ALPHABETICAL
    else:
        if state not in STATE_ZIPS:
            print(f"ERROR: '{state}' not found.")
            return
        states_to_run = [state]

    progress         = load_progress()
    completed_zips   = set(progress.get("completed_zips", []))
    completed_states = set(progress.get("completed_states", []))
    total_states     = len(states_to_run)

    print(f"\nDiRoNA Lead Scraper v2 — National Run")
    print(f"States: {', '.join(states_to_run[:5])}{'...' if len(states_to_run) > 5 else ''}")

    for state_num, current_state in enumerate(states_to_run, 1):

        if current_state in completed_states:
            print(f"\n  Skipping {current_state} (complete)")
            continue

        print_state_header(current_state, state_num, total_states)

        zips        = STATE_ZIPS[current_state]
        output_file = get_output_filename(current_state)
        seen_keys   = set()
        state_total = 0

        # New browser context per state with random config
        with sync_playwright() as p:
            viewport = random.choice(VIEWPORTS)
            user_agent = random.choice(USER_AGENTS)
            
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=user_agent,
                viewport=viewport,
                locale="en-US",
                timezone_id="America/New_York",
            )
            page = context.new_page()

            total_zips = len(zips)
            for idx, zip_code in enumerate(zips, 1):
                zip_str = str(zip_code).zfill(5)

                if zip_str in completed_zips:
                    continue

                print(f"\n  ZIP {zip_str}  ({idx}/{total_zips})")
                zip_results = []

                for category in SEARCH_CATEGORIES:
                    print(f"    Searching: {category}...")
                    random_delay()

                    found = scrape_zip_category(page, category, zip_str)

                    for r in found:
                        key = (r["Restaurant Name"].lower(), zip_str)
                        if key not in seen_keys:
                            seen_keys.add(key)
                            zip_results.append(r)
                            print(f"      ✅ FOUND: {r['Restaurant Name']} "
                                  f"| {r['Rating']}⭐ | {r['Price Level']}")

                if zip_results:
                    write_csv(output_file, zip_results)
                    state_total += len(zip_results)
                    print(f"    💾 {len(zip_results)} written (state: {state_total})")

                completed_zips.add(zip_str)
                progress["completed_zips"] = list(completed_zips)
                save_progress(progress)

            browser.close()

        completed_states.add(current_state)
        progress["completed_states"] = list(completed_states)
        save_progress(progress)

        print(f"\n  ✅ {current_state} COMPLETE: {state_total} restaurants")
        print(f"     CSV: {output_file}")
        print(f"     Progress: {state_num}/{total_states} states")


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    import sys
    state = sys.argv[1] if len(sys.argv) > 1 else "ALL"

    print("=" * 60)
    print("  DiRoNA Lead Scraper v2 - Enhanced Bot Evasion")
    print("=" * 60)
    print(f"  Target       : {state}")
    print(f"  Delay        : {DELAY_MIN}–{DELAY_MAX}s (random)")
    print(f"  User Agents  : {len(USER_AGENTS)} rotating")
    print(f"  Viewports    : {len(VIEWPORTS)} random sizes")
    print(f"  Retry Logic  : {RETRY_ATTEMPTS} attempts per search")
    print(f"  Min Rating   : {MIN_RATING}+ | Price: {'$'*MIN_PRICE_LEVEL}+ | Reviews: {MIN_REVIEWS}+")
    print("=" * 60)
    print()
    run_scraper(state)
