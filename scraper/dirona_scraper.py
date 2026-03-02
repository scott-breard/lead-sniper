#!/usr/bin/env python3
"""
DiRoNA Lead Scraper
Searches Google Maps ZIP code by ZIP code across 5 categories.
Processes states in strict alphabetical order (Alabama → Wyoming).
Outputs results to CSV files in the output/ folder.
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
# CONFIGURATION
# ============================================================

SEARCH_CATEGORIES = [
    "fine dining",
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
MIN_PRICE_LEVEL = 3      # $$$ = 3, $$$$ = 4
MIN_REVIEWS     = 50
DELAY_MIN       = 2.5   # seconds between every request
DELAY_MAX       = 5.0

OUTPUT_DIR    = "output"
PROGRESS_FILE = "scraper/progress.json"

# ============================================================
# CSV COLUMNS  (matches LeadsScrapper format)
# ============================================================

CSV_COLUMNS = [
    "Restaurant Name", "Contact Name", "Street Address", "City", "ZIP",
    "State", "Country", "Price Level", "Description", "Phone", "Email",
    "Website", "Cuisine Type", "Has Bar", "Review Count", "Rating",
    "Facebook", "Twitter", "Instagram", "Google Maps URL",
    "Search Category", "ZIP Searched", "Date Scraped",
]

# ============================================================
# ALL 50 U.S. STATES IN STRICT ALPHABETICAL ORDER
# ============================================================

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

def random_delay():
    delay = random.uniform(DELAY_MIN, DELAY_MAX)
    print(f"    Pausing {delay:.1f}s...")
    time.sleep(delay)

def get_output_filename(state):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    date_str   = datetime.now().strftime("%Y%m%d")
    safe_state = state.replace(" ", "_")
    return os.path.join(OUTPUT_DIR, f"dirona_leads_{safe_state}_{date_str}.csv")

def write_csv(filepath, rows):
    """Append rows to CSV. Writes header if file is new."""
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

# ============================================================
# GOOGLE MAPS SCRAPER
# ============================================================

def scrape_zip_category(page, category, zip_code):
    """
    Search Google Maps for one category + ZIP code.
    Returns list of qualifying restaurant dicts.
    """
    results = []
    url     = f"https://www.google.com/maps/search/{category.replace(' ', '+')}+{zip_code}"

    try:
        page.goto(url, timeout=30000)
        page.wait_for_timeout(3000)

        for _ in range(4):
            page.keyboard.press("End")
            page.wait_for_timeout(1200)

        listings   = page.query_selector_all('a[href*="/maps/place/"]')
        seen_hrefs = set()

        for listing in listings[:20]:
            href = listing.get_attribute("href")
            if not href or href in seen_hrefs:
                continue
            seen_hrefs.add(href)

            try:
                listing.click()
                page.wait_for_timeout(2500)

                # Name
                name    = ""
                name_el = page.query_selector("h1.DUwDvf")
                if name_el:
                    name = name_el.inner_text().strip()
                if not name:
                    page.go_back(); page.wait_for_timeout(1200)
                    continue

                # Rating
                rating    = 0.0
                rating_el = page.query_selector("span.ceNzKf")
                if rating_el:
                    m = re.search(r"([\d\.]+)", rating_el.get_attribute("aria-label") or "")
                    if m:
                        rating = float(m.group(1))
                if rating < MIN_RATING:
                    page.go_back(); page.wait_for_timeout(1200)
                    continue

                # Reviews
                reviews   = 0
                review_el = page.query_selector('span[aria-label*="reviews"]')
                if review_el:
                    m = re.search(r"([\d,]+)", review_el.get_attribute("aria-label") or "")
                    if m:
                        reviews = int(m.group(1).replace(",", ""))
                if reviews < MIN_REVIEWS:
                    page.go_back(); page.wait_for_timeout(1200)
                    continue

                # Price level
                price_str = ""
                price_el  = page.query_selector('span[aria-label*="Price"]')
                if price_el:
                    price_str = re.sub(r"[^$]", "", price_el.get_attribute("aria-label") or "")
                if parse_price_level(price_str) < MIN_PRICE_LEVEL:
                    page.go_back(); page.wait_for_timeout(1200)
                    continue

                # Cuisine
                cuisine = ""
                cat_el  = page.query_selector('button[jsaction*="category"]')
                if cat_el:
                    cuisine = cat_el.inner_text().strip()
                if is_excluded(name, cuisine):
                    page.go_back(); page.wait_for_timeout(1200)
                    continue

                # Address
                address = ""
                addr_el = page.query_selector('button[data-item-id="address"]')
                if addr_el:
                    address = addr_el.inner_text().strip()

                # Phone
                phone    = ""
                phone_el = page.query_selector('button[data-item-id^="phone"]')
                if phone_el:
                    phone = phone_el.inner_text().strip()

                # Website
                website = ""
                web_el  = page.query_selector('a[data-item-id="authority"]')
                if web_el:
                    website = web_el.get_attribute("href") or ""

                # Description
                description = ""
                desc_el     = page.query_selector("div.PYvSYb")
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

                page.go_back()
                page.wait_for_timeout(1200)

            except Exception as e:
                print(f"      Listing error: {e}")
                try:
                    page.go_back(); page.wait_for_timeout(1000)
                except:
                    pass

    except Exception as e:
        print(f"    Search error [{category} / {zip_code}]: {e}")

    return results


# ============================================================
# MAIN RUNNER
# ============================================================

def run_scraper(state="Alabama"):
    """
    Run ZIP-by-ZIP scraper.
    - Single state: python dirona_scraper.py "California"
    - All 50 states in alphabetical order: python dirona_scraper.py ALL
    Results written to CSV after every ZIP. Auto-resumes if interrupted.
    """
    if state == "ALL":
        states_to_run = STATES_ALPHABETICAL   # strict A-Z order
    else:
        if state not in STATE_ZIPS:
            print(f"ERROR: '{state}' not found. Check spelling.")
            return
        states_to_run = [state]

    progress           = load_progress()
    completed_zips     = set(progress.get("completed_zips", []))
    completed_states   = set(progress.get("completed_states", []))
    total_states       = len(states_to_run)

    print(f"\nDiRoNA Lead Scraper — National Run")
    print(f"States to process ({total_states}): {', '.join(states_to_run)}")

    for state_num, current_state in enumerate(states_to_run, 1):

        if current_state in completed_states:
            print(f"\n  Skipping {current_state} (already complete)")
            continue

        print_state_header(current_state, state_num, total_states)

        zips        = STATE_ZIPS[current_state]
        output_file = get_output_filename(current_state)
        seen_keys   = set()
        state_total = 0

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                )
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
                            print(f"      FOUND: {r['Restaurant Name']} "
                                  f"| {r['Rating']}⭐ | {r['Price Level']}")

                if zip_results:
                    write_csv(output_file, zip_results)
                    state_total += len(zip_results)
                    print(f"    ✅ {len(zip_results)} written to CSV  "
                          f"(state total: {state_total})")

                completed_zips.add(zip_str)
                progress["completed_zips"]   = list(completed_zips)
                save_progress(progress)

            browser.close()

        # Mark state complete
        completed_states.add(current_state)
        progress["completed_states"] = list(completed_states)
        save_progress(progress)

        print(f"\n  ✅ COMPLETE: {current_state}")
        print(f"     Restaurants found : {state_total}")
        print(f"     CSV               : {output_file}")
        print(f"     States done       : {state_num}/{total_states}")
        remaining = states_to_run[state_num:]
        if remaining:
            print(f"     Up next           : {remaining[0]}")


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    import sys
    state = sys.argv[1] if len(sys.argv) > 1 else "ALL"

    print("=" * 60)
    print("  DiRoNA Lead Scraper")
    print("=" * 60)
    print(f"  Target     : {state}")
    print(f"  Order      : Alphabetical (Alabama → Wyoming)")
    print(f"  Categories : {', '.join(SEARCH_CATEGORIES)}")
    print(f"  Min Rating : {MIN_RATING}+")
    print(f"  Min Price  : {'$' * MIN_PRICE_LEVEL}+")
    print(f"  Min Reviews: {MIN_REVIEWS}+")
    print(f"  Delay      : {DELAY_MIN}–{DELAY_MAX}s between every request")
    print(f"  Output     : {OUTPUT_DIR}/ (one CSV per state)")
    print("=" * 60)
    print()
    run_scraper(state)
