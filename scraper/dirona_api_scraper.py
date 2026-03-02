#!/usr/bin/env python3
"""
DiRoNA Lead Scraper v3 - Google Places API Edition
Searches all 42,000 US ZIP codes for fine dining restaurants.
Fast, reliable, and cost-effective ($1,144 total or $744 over 3 months).
"""

import csv
import time
import json
import os
import requests
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================

# **PASTE YOUR GOOGLE PLACES API KEY HERE**
GOOGLE_API_KEY = "YOUR_API_KEY_HERE"  # Replace with your actual API key

# Search settings
SEARCH_QUERY = "fine dining restaurant"
MIN_RATING = 4.0
MIN_PRICE_LEVEL = 3  # 3 = $$$, 4 = $$$$
MIN_REVIEWS = 50

# Exclusion keywords
EXCLUDE_KEYWORDS = [
    "buffet", "chinese", "brazilian", "all you can eat",
    "pizzeria", "pizza", "gastropub", "thai", "indian",
    "fast food", "fast casual", "mcdonalds", "subway", "chipotle",
]

# API settings
API_DELAY = 0.1  # Small delay between requests (Google allows 60/sec)
MAX_RESULTS_PER_ZIP = 20  # Google returns max 20 results per search

# File paths
OUTPUT_DIR = "output"
PROGRESS_FILE = "scraper/api_progress.json"
MASTER_CSV = os.path.join(OUTPUT_DIR, "dirona_leads_nationwide_api.csv")

# CSV columns
CSV_COLUMNS = [
    "Restaurant Name", "Street Address", "City", "State", "ZIP",
    "Country", "Rating", "Review Count", "Price Level",
    "Cuisine Type", "Phone", "Website", "Google Maps URL",
    "Place ID", "Latitude", "Longitude", "ZIP Searched", "Date Scraped"
]

# All US states with ZIP ranges
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
    """Load scraping progress from file."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"completed_zips": [], "total_found": 0, "total_qualified": 0, "api_calls": 0}

def save_progress(progress):
    """Save scraping progress to file."""
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)

def is_excluded(name, cuisine_type):
    """Check if restaurant should be excluded based on keywords."""
    text = (name + " " + (cuisine_type or "")).lower()
    return any(kw in text for kw in EXCLUDE_KEYWORDS)

def write_csv(filepath, rows):
    """Append rows to CSV file."""
    file_exists = os.path.exists(filepath)
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

# ============================================================
# GOOGLE PLACES API FUNCTIONS
# ============================================================

def search_places_api(zip_code):
    """
    Search Google Places API for fine dining restaurants in a ZIP code.
    Returns list of places with basic info.
    """
    url = "https://places.googleapis.com/v1/places:searchText"
    
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_API_KEY,
        "X-Goog-FieldMask": (
            "places.id,places.displayName,places.formattedAddress,"
            "places.rating,places.userRatingCount,places.priceLevel,"
            "places.types,places.nationalPhoneNumber,places.websiteUri,"
            "places.googleMapsUri,places.location"
        )
    }
    
    payload = {
        "textQuery": f"{SEARCH_QUERY} {zip_code}",
        "maxResultCount": MAX_RESULTS_PER_ZIP,
        "locationBias": {
            "circle": {
                "center": {"latitude": 0, "longitude": 0},  # Will be overridden by query
                "radius": 25000.0  # 25km radius
            }
        }
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            return data.get("places", [])
        else:
            print(f"    API Error {response.status_code}: {response.text[:200]}")
            return []
            
    except Exception as e:
        print(f"    Request failed: {e}")
        return []

def parse_place(place, zip_searched):
    """
    Parse a place from API response into our CSV format.
    Returns dict or None if doesn't meet criteria.
    """
    # Extract basic fields
    name = place.get("displayName", {}).get("text", "")
    if not name:
        return None
    
    # Rating check
    rating = place.get("rating", 0)
    if rating < MIN_RATING:
        return None
    
    # Review count check
    review_count = place.get("userRatingCount", 0)
    if review_count < MIN_REVIEWS:
        return None
    
    # Price level check (PRICE_LEVEL_FREE=0, INEXPENSIVE=1, MODERATE=2, EXPENSIVE=3, VERY_EXPENSIVE=4)
    price_level = place.get("priceLevel", "")
    price_map = {
        "PRICE_LEVEL_FREE": 0,
        "PRICE_LEVEL_INEXPENSIVE": 1,
        "PRICE_LEVEL_MODERATE": 2,
        "PRICE_LEVEL_EXPENSIVE": 3,
        "PRICE_LEVEL_VERY_EXPENSIVE": 4
    }
    price_num = price_map.get(price_level, 0)
    
    if price_num < MIN_PRICE_LEVEL:
        return None
    
    # Convert to $ symbols
    price_display = "$" * price_num if price_num > 0 else ""
    
    # Get cuisine type from types array
    types = place.get("types", [])
    cuisine_type = ", ".join([t.replace("_", " ").title() for t in types if "restaurant" in t.lower()][:3])
    
    # Exclusion check
    if is_excluded(name, cuisine_type):
        return None
    
    # Parse address
    address = place.get("formattedAddress", "")
    address_parts = address.split(", ") if address else []
    
    street = address_parts[0] if len(address_parts) > 0 else ""
    city = address_parts[1] if len(address_parts) > 1 else ""
    state = address_parts[2].split()[0] if len(address_parts) > 2 else ""
    
    # Get coordinates
    location = place.get("location", {})
    lat = location.get("latitude", "")
    lng = location.get("longitude", "")
    
    return {
        "Restaurant Name": name,
        "Street Address": street,
        "City": city,
        "State": state,
        "ZIP": zip_searched,
        "Country": "United States",
        "Rating": rating,
        "Review Count": review_count,
        "Price Level": price_display,
        "Cuisine Type": cuisine_type,
        "Phone": place.get("nationalPhoneNumber", ""),
        "Website": place.get("websiteUri", ""),
        "Google Maps URL": place.get("googleMapsUri", ""),
        "Place ID": place.get("id", ""),
        "Latitude": lat,
        "Longitude": lng,
        "ZIP Searched": zip_searched,
        "Date Scraped": datetime.now().strftime("%Y-%m-%d")
    }

# ============================================================
# MAIN SCRAPER
# ============================================================

def run_api_scraper(state="ALL"):
    """
    Run the API scraper for specified state(s).
    """
    if GOOGLE_API_KEY == "YOUR_API_KEY_HERE":
        print("\n❌ ERROR: Please add your Google Places API key to the script!")
        print("   Edit line 16: GOOGLE_API_KEY = \"YOUR_ACTUAL_KEY_HERE\"\n")
        return
    
    # Determine states to process
    if state == "ALL":
        states_to_run = STATES_ALPHABETICAL
    else:
        if state not in STATE_ZIPS:
            print(f"ERROR: '{state}' not found in state list.")
            return
        states_to_run = [state]
    
    # Load progress
    progress = load_progress()
    completed_zips = set(progress.get("completed_zips", []))
    seen_place_ids = set()
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print(f"\n{'='*70}")
    print(f"  DiRoNA Lead Scraper v3 - Google Places API Edition")
    print(f"{'='*70}")
    print(f"  Target States : {len(states_to_run)} ({states_to_run[0]} → {states_to_run[-1]})")
    print(f"  Search Query  : {SEARCH_QUERY}")
    print(f"  Filters       : Rating ≥{MIN_RATING} | Price ≥{'$'*MIN_PRICE_LEVEL} | Reviews ≥{MIN_REVIEWS}")
    print(f"  Output File   : {MASTER_CSV}")
    print(f"  Progress      : {len(completed_zips):,} ZIPs complete | {progress.get('total_qualified', 0):,} qualified")
    print(f"  API Calls     : {progress.get('api_calls', 0):,} made")
    print(f"{'='*70}\n")
    
    total_states = len(states_to_run)
    
    for state_idx, current_state in enumerate(states_to_run, 1):
        print(f"\n{'='*70}")
        print(f"  STATE {state_idx}/{total_states}: {current_state.upper()}")
        print(f"{'='*70}")
        
        zips = STATE_ZIPS[current_state]
        state_qualified = 0
        
        for zip_idx, zip_code in enumerate(zips, 1):
            zip_str = str(zip_code).zfill(5)
            
            if zip_str in completed_zips:
                continue
            
            print(f"\n  ZIP {zip_str} ({zip_idx}/{len(zips)})")
            
            # Search API
            places = search_places_api(zip_str)
            progress["api_calls"] = progress.get("api_calls", 0) + 1
            
            if not places:
                print(f"    No results")
                completed_zips.add(zip_str)
                progress["completed_zips"] = list(completed_zips)
                save_progress(progress)
                time.sleep(API_DELAY)
                continue
            
            print(f"    Found {len(places)} results, filtering...")
            
            # Parse and filter
            qualified = []
            for place in places:
                place_id = place.get("id", "")
                if place_id in seen_place_ids:
                    continue
                
                parsed = parse_place(place, zip_str)
                if parsed:
                    seen_place_ids.add(place_id)
                    qualified.append(parsed)
                    print(f"      ✅ {parsed['Restaurant Name']} | {parsed['Rating']}⭐ | {parsed['Price Level']}")
            
            # Save results
            if qualified:
                write_csv(MASTER_CSV, qualified)
                state_qualified += len(qualified)
                progress["total_qualified"] = progress.get("total_qualified", 0) + len(qualified)
                print(f"    💾 Saved {len(qualified)} restaurants (state total: {state_qualified})")
            
            # Update progress
            completed_zips.add(zip_str)
            progress["completed_zips"] = list(completed_zips)
            progress["total_found"] = progress.get("total_found", 0) + len(places)
            save_progress(progress)
            
            time.sleep(API_DELAY)
        
        print(f"\n  ✅ {current_state} COMPLETE: {state_qualified} restaurants")
        print(f"     Progress: {len(completed_zips):,}/{42000} ZIPs | {progress['total_qualified']:,} qualified total")
    
    print(f"\n{'='*70}")
    print(f"  🎉 SCRAPING COMPLETE!")
    print(f"{'='*70}")
    print(f"  Total ZIPs Searched    : {len(completed_zips):,}")
    print(f"  Total API Calls        : {progress['api_calls']:,}")
    print(f"  Total Results Found    : {progress['total_found']:,}")
    print(f"  Qualified Restaurants  : {progress['total_qualified']:,}")
    print(f"  Output File            : {MASTER_CSV}")
    print(f"  Estimated API Cost     : ${(progress['api_calls'] * 0.032):.2f}")
    print(f"{'='*70}\n")

# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    import sys
    state = sys.argv[1] if len(sys.argv) > 1 else "ALL"
    run_api_scraper(state)
