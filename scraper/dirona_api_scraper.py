#!/usr/bin/env python3
"""
DiRoNA Lead Scraper v4.0 - Text Search + Place Details (Full Info)
Searches all 42,000 US ZIP codes for fine dining restaurants.
Makes Place Details calls for complete data including hours and photos.
Cost: ~$1,314 total ($1,114 after $200 free credit)
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

# **GOOGLE PLACES API KEY**
GOOGLE_API_KEY = "AIzaSyBBpTCIYUKGxs8oWwe5YtFpeU9RJ8jkK7s"

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

# CSV columns (expanded for Place Details)
CSV_COLUMNS = [
    "Restaurant Name", "Street Address", "City", "State", "ZIP",
    "Country", "Rating", "Review Count", "Price Level",
    "Cuisine Type", "Phone", "Website", "Google Maps URL",
    "Place ID", "Latitude", "Longitude",
    "Hours Monday", "Hours Tuesday", "Hours Wednesday", "Hours Thursday",
    "Hours Friday", "Hours Saturday", "Hours Sunday",
    "Business Status", "Serves Dinner", "Serves Wine", "Serves Beer",
    "Reservable", "Dine In", "Takeout", "Delivery",
    "Photo URL 1", "Photo URL 2", "Photo URL 3",
    "ZIP Searched", "Date Scraped"
]

# All US states with ZIP ranges and abbreviations
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

STATE_ABBREV = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
    "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
    "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
    "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
    "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY"
}

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

def get_state_csv_filename(state_name):
    """Generate CSV filename for a specific state."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    state_abbrev = STATE_ABBREV.get(state_name, "")
    safe_name = state_name.replace(" ", "_")
    return os.path.join(OUTPUT_DIR, f"dirona_leads_{safe_name}_{state_abbrev}_{date_str}.csv")

def load_progress():
    """Load scraping progress from file."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {
        "completed_zips": [],
        "total_found": 0,
        "total_qualified": 0,
        "search_api_calls": 0,
        "details_api_calls": 0
    }

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

def search_places_api(zip_code, state_name):
    """
    Search Google Places API for fine dining restaurants in a ZIP code.
    Returns list of places with basic info.
    Cost: $0.032 per search
    """
    url = "https://places.googleapis.com/v1/places:searchText"
    
    state_abbrev = STATE_ABBREV.get(state_name, "")
    query = f"{SEARCH_QUERY} {zip_code} {state_abbrev}"
    
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_API_KEY,
        "X-Goog-FieldMask": (
            "places.id,places.displayName,places.formattedAddress,"
            "places.rating,places.userRatingCount,places.priceLevel,"
            "places.types,places.location"
        )
    }
    
    payload = {
        "textQuery": query,
        "maxResultCount": MAX_RESULTS_PER_ZIP,
        "locationBias": {
            "circle": {
                "center": {"latitude": 39.8283, "longitude": -98.5795},
                "radius": 50000.0
            }
        }
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        if response.status_code == 200:
            return response.json().get("places", [])
        else:
            print(f"    API Error {response.status_code}: {response.text[:200]}")
            return []
    except Exception as e:
        print(f"    Request failed: {e}")
        return []

def get_place_details(place_id):
    """
    Get full place details including hours, photos, and amenities.
    Cost: $0.017 per details call
    """
    url = f"https://places.googleapis.com/v1/places/{place_id}"
    
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_API_KEY,
        "X-Goog-FieldMask": (
            "id,displayName,formattedAddress,internationalPhoneNumber,"
            "nationalPhoneNumber,websiteUri,googleMapsUri,location,"
            "rating,userRatingCount,priceLevel,types,"
            "regularOpeningHours,businessStatus,"
            "servesDinner,servesWine,servesWine,servesBeer,"
            "reservable,dineIn,takeout,delivery,"
            "photos.name"
        )
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"      Details API Error {response.status_code}")
            return None
    except Exception as e:
        print(f"      Details request failed: {e}")
        return None

def parse_hours(opening_hours):
    """Parse opening hours into individual day columns."""
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    hours_dict = {f"Hours {day}": "" for day in days}
    
    if not opening_hours or "weekdayDescriptions" not in opening_hours:
        return hours_dict
    
    for desc in opening_hours.get("weekdayDescriptions", []):
        # Format: "Monday: 5:00 PM – 10:00 PM"
        if ": " in desc:
            day, hours = desc.split(": ", 1)
            hours_dict[f"Hours {day}"] = hours
    
    return hours_dict

def get_photo_urls(photos):
    """Extract up to 3 photo URLs from photos array."""
    photo_urls = {"Photo URL 1": "", "Photo URL 2": "", "Photo URL 3": ""}
    
    if not photos:
        return photo_urls
    
    for i, photo in enumerate(photos[:3], 1):
        photo_name = photo.get("name", "")
        if photo_name:
            # Construct photo URL
            photo_url = f"https://places.googleapis.com/v1/{photo_name}/media?key={GOOGLE_API_KEY}&maxHeightPx=1600&maxWidthPx=1600"
            photo_urls[f"Photo URL {i}"] = photo_url
    
    return photo_urls

def parse_place_with_details(place, details, zip_searched, target_state):
    """
    Parse place with full details into CSV format.
    Returns dict or None if doesn't meet criteria or wrong state.
    """
    # Use details if available, fallback to search result
    data = details if details else place
    
    name = data.get("displayName", {}).get("text", "")
    if not name:
        return None
    
    # Parse address and check state
    address = data.get("formattedAddress", "")
    address_parts = address.split(", ") if address else []
    
    street = address_parts[0] if len(address_parts) > 0 else ""
    city = address_parts[1] if len(address_parts) > 1 else ""
    state_zip = address_parts[2] if len(address_parts) > 2 else ""
    state = state_zip.split()[0] if state_zip else ""
    
    # State filter
    target_abbrev = STATE_ABBREV.get(target_state, "")
    if state != target_abbrev:
        return None
    
    # Rating check
    rating = data.get("rating", 0)
    if rating < MIN_RATING:
        return None
    
    # Review count check
    review_count = data.get("userRatingCount", 0)
    if review_count < MIN_REVIEWS:
        return None
    
    # Price level check
    price_level = data.get("priceLevel", "")
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
    
    price_display = "$" * price_num if price_num > 0 else ""
    
    # Cuisine type
    types = data.get("types", [])
    cuisine_type = ", ".join([t.replace("_", " ").title() for t in types if "restaurant" in t.lower()][:3])
    
    # Exclusion check
    if is_excluded(name, cuisine_type):
        return None
    
    # Coordinates
    location = data.get("location", {})
    lat = location.get("latitude", "")
    lng = location.get("longitude", "")
    
    # Parse hours from details
    hours = parse_hours(data.get("regularOpeningHours")) if details else {
        f"Hours {day}": "" for day in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    }
    
    # Parse photos from details
    photos = get_photo_urls(data.get("photos", [])) if details else {
        "Photo URL 1": "", "Photo URL 2": "", "Photo URL 3": ""
    }
    
    # Build result
    result = {
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
        "Phone": data.get("nationalPhoneNumber", data.get("internationalPhoneNumber", "")),
        "Website": data.get("websiteUri", ""),
        "Google Maps URL": data.get("googleMapsUri", ""),
        "Place ID": data.get("id", ""),
        "Latitude": lat,
        "Longitude": lng,
        "Business Status": data.get("businessStatus", ""),
        "Serves Dinner": data.get("servesDinner", ""),
        "Serves Wine": data.get("servesWine", ""),
        "Serves Beer": data.get("servesBeer", ""),
        "Reservable": data.get("reservable", ""),
        "Dine In": data.get("dineIn", ""),
        "Takeout": data.get("takeout", ""),
        "Delivery": data.get("delivery", ""),
        "ZIP Searched": zip_searched,
        "Date Scraped": datetime.now().strftime("%Y-%m-%d")
    }
    
    # Add hours and photos
    result.update(hours)
    result.update(photos)
    
    return result

# ============================================================
# MAIN SCRAPER
# ============================================================

def run_api_scraper(state="ALL"):
    """
    Run the API scraper with Place Details calls.
    """
    if GOOGLE_API_KEY == "YOUR_API_KEY_HERE":
        print("\n❌ ERROR: Please add your Google Places API key!")
        return
    
    # Determine states
    if state == "ALL":
        states_to_run = STATES_ALPHABETICAL
    else:
        if state not in STATE_ZIPS:
            print(f"ERROR: '{state}' not found.")
            return
        states_to_run = [state]
    
    # Load progress
    progress = load_progress()
    completed_zips = set(progress.get("completed_zips", []))
    seen_place_ids = set()
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print(f"\n{'='*70}")
    print(f"  DiRoNA Lead Scraper v4.0 - WITH PLACE DETAILS")
    print(f"{'='*70}")
    print(f"  Target States : {len(states_to_run)}")
    print(f"  Search Query  : {SEARCH_QUERY}")
    print(f"  Filters       : Rating ≥{MIN_RATING} | Price ≥{'$'*MIN_PRICE_LEVEL} | Reviews ≥{MIN_REVIEWS}")
    print(f"  Details       : Hours, Photos, Amenities included")
    print(f"  Costs         : Search $0.032 + Details $0.017 = $0.049/restaurant")
    print(f"  Progress      : {len(completed_zips):,} ZIPs | {progress.get('total_qualified', 0):,} qualified")
    print(f"  API Calls     : {progress.get('search_api_calls', 0):,} search | {progress.get('details_api_calls', 0):,} details")
    print(f"{'='*70}\n")
    
    for state_idx, current_state in enumerate(states_to_run, 1):
        print(f"\n{'='*70}")
        print(f"  STATE {state_idx}/{len(states_to_run)}: {current_state.upper()} ({STATE_ABBREV[current_state]})")
        print(f"{'='*70}")
        
        state_csv = get_state_csv_filename(current_state)
        print(f"  Output: {state_csv}\n")
        
        zips = STATE_ZIPS[current_state]
        state_qualified = 0
        
        for zip_idx, zip_code in enumerate(zips, 1):
            zip_str = str(zip_code).zfill(5)
            
            if zip_str in completed_zips:
                continue
            
            print(f"\n  ZIP {zip_str} ({zip_idx}/{len(zips)})")
            
            # Search
            places = search_places_api(zip_str, current_state)
            progress["search_api_calls"] = progress.get("search_api_calls", 0) + 1
            
            if not places:
                print(f"    No results")
                completed_zips.add(zip_str)
                progress["completed_zips"] = list(completed_zips)
                save_progress(progress)
                time.sleep(API_DELAY)
                continue
            
            print(f"    Found {len(places)} results, fetching details...")
            
            # Process each place
            qualified = []
            for place in places:
                place_id = place.get("id", "")
                if place_id in seen_place_ids:
                    continue
                
                # Quick check before details call
                quick_check = parse_place_with_details(place, None, zip_str, current_state)
                if not quick_check:
                    continue
                
                # Get full details
                details = get_place_details(place_id)
                progress["details_api_calls"] = progress.get("details_api_calls", 0) + 1
                time.sleep(API_DELAY)
                
                # Parse with details
                parsed = parse_place_with_details(place, details, zip_str, current_state)
                if parsed:
                    seen_place_ids.add(place_id)
                    qualified.append(parsed)
                    print(f"      ✅ {parsed['Restaurant Name']} | {parsed['City']}, {parsed['State']} | {parsed['Rating']}⭐")
            
            # Save
            if qualified:
                write_csv(state_csv, qualified)
                state_qualified += len(qualified)
                progress["total_qualified"] = progress.get("total_qualified", 0) + len(qualified)
                print(f"    💾 Saved {len(qualified)} restaurants (state: {state_qualified})")
            
            # Update progress
            completed_zips.add(zip_str)
            progress["completed_zips"] = list(completed_zips)
            progress["total_found"] = progress.get("total_found", 0) + len(places)
            save_progress(progress)
            
            time.sleep(API_DELAY)
        
        search_cost = progress.get('search_api_calls', 0) * 0.032
        details_cost = progress.get('details_api_calls', 0) * 0.017
        total_cost = search_cost + details_cost
        
        print(f"\n  ✅ {current_state} COMPLETE: {state_qualified} restaurants")
        print(f"     Progress: {len(completed_zips):,}/42,000 ZIPs | {progress['total_qualified']:,} total")
        print(f"     Cost: ${total_cost:.2f} (${search_cost:.2f} search + ${details_cost:.2f} details)")
    
    print(f"\n{'='*70}")
    print(f"  🎉 COMPLETE!")
    print(f"{'='*70}")
    print(f"  ZIPs Searched         : {len(completed_zips):,}")
    print(f"  Search API Calls      : {progress['search_api_calls']:,} (${progress['search_api_calls'] * 0.032:.2f})")
    print(f"  Details API Calls     : {progress['details_api_calls']:,} (${progress['details_api_calls'] * 0.017:.2f})")
    print(f"  Qualified Restaurants : {progress['total_qualified']:,}")
    print(f"  Total Cost            : ${(progress['search_api_calls'] * 0.032 + progress['details_api_calls'] * 0.017):.2f}")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    import sys
    state = sys.argv[1] if len(sys.argv) > 1 else "ALL"
    run_api_scraper(state)
