#!/usr/bin/env python3
"""
Canadian Restaurant Scraper - Nova Scotia Edition
Searches major cities and towns in Nova Scotia for fine dining restaurants.
Uses Google Places API with Place Details for complete data.
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

# **GOOGLE PLACES API KEY** - Load from environment variable
GOOGLE_API_KEY = os.environ.get('GOOGLE_PLACES_API_KEY', '')

if not GOOGLE_API_KEY:
    print("\n❌ ERROR: GOOGLE_PLACES_API_KEY environment variable not set!")
    print("\nRun: export GOOGLE_PLACES_API_KEY='your-key-here'\n")
    exit(1)

# Search settings
SEARCH_QUERY = "fine dining restaurant"
MIN_RATING = 4.0
MIN_PRICE_LEVEL = 3  # 3 = $$$, 4 = $$$$
MIN_REVIEWS = 25  # Lower for Canada (smaller population)

# Exclusion keywords
EXCLUDE_KEYWORDS = [
    "buffet", "chinese", "brazilian", "all you can eat",
    "pizzeria", "pizza", "gastropub", "thai", "indian",
    "fast food", "fast casual", "mcdonalds", "subway", "chipotle",
    "tim hortons", "tims"
]

# Geographic bounds for Nova Scotia (to filter out wrong locations)
# Nova Scotia latitude: ~43.4°N to 47.0°N
# Nova Scotia longitude: ~-66.4°W to -59.7°W
NS_LAT_MIN = 43.4
NS_LAT_MAX = 47.0
NS_LNG_MIN = -66.4
NS_LNG_MAX = -59.7

# API settings
API_DELAY = 0.1
MAX_RESULTS_PER_SEARCH = 20

# File paths
OUTPUT_DIR = "output"
PROGRESS_FILE = "scraper/canada_progress.json"

# CSV columns
CSV_COLUMNS = [
    "Restaurant Name", "Street Address", "City", "Province", "Postal Code",
    "Country", "Rating", "Review Count", "Price Level",
    "Cuisine Type", "Phone", "Website", "Google Maps URL",
    "Place ID", "Latitude", "Longitude",
    "Hours Monday", "Hours Tuesday", "Hours Wednesday", "Hours Thursday",
    "Hours Friday", "Hours Saturday", "Hours Sunday",
    "Business Status", "Serves Dinner", "Serves Wine", "Serves Beer",
    "Reservable", "Dine In", "Takeout", "Delivery",
    "Photo URL 1", "Photo URL 2", "Photo URL 3",
    "Search Location", "Date Scraped"
]

# Nova Scotia cities and towns to search (using full "Nova Scotia, Canada" format)
NOVA_SCOTIA_LOCATIONS = [
    # Major cities
    "Halifax, Nova Scotia, Canada",
    "Dartmouth, Nova Scotia, Canada",
    "Sydney, Nova Scotia, Canada",
    "Truro, Nova Scotia, Canada",
    "New Glasgow, Nova Scotia, Canada",
    "Glace Bay, Nova Scotia, Canada",
    "Kentville, Nova Scotia, Canada",
    
    # Mid-size towns
    "Amherst, Nova Scotia, Canada",
    "Bridgewater, Nova Scotia, Canada",
    "Yarmouth, Nova Scotia, Canada",
    "Antigonish, Nova Scotia, Canada",
    "Port Hawkesbury, Nova Scotia, Canada",
    "Stellarton, Nova Scotia, Canada",
    "Westville, Nova Scotia, Canada",
    "Pictou, Nova Scotia, Canada",
    "Windsor, Nova Scotia, Canada",
    "Wolfville, Nova Scotia, Canada",
    "Chester, Nova Scotia, Canada",
    
    # Coastal/tourist areas
    "Lunenburg, Nova Scotia, Canada",
    "Mahone Bay, Nova Scotia, Canada",
    "Baddeck, Nova Scotia, Canada",
    "Ingonish, Nova Scotia, Canada",
    "Digby, Nova Scotia, Canada",
    "Annapolis Royal, Nova Scotia, Canada",
    "Liverpool, Nova Scotia, Canada",
    "Shelburne, Nova Scotia, Canada",
    "Pugwash, Nova Scotia, Canada",
    "Parrsboro, Nova Scotia, Canada"
]

PROVINCE_ABBREV = {"Nova Scotia": "NS"}

# ============================================================
# HELPERS
# ============================================================

def get_output_filename():
    """Generate output filename."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    return os.path.join(OUTPUT_DIR, f"dirona_leads_Nova_Scotia_NS_{date_str}.csv")

def load_progress():
    """Load scraping progress."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {
        "completed_locations": [],
        "total_found": 0,
        "total_qualified": 0,
        "search_api_calls": 0,
        "details_api_calls": 0,
        "filtered_out_of_bounds": 0
    }

def save_progress(progress):
    """Save scraping progress."""
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)

def is_excluded(name, cuisine_type):
    """Check if restaurant should be excluded."""
    text = (name + " " + (cuisine_type or "")).lower()
    return any(kw in text for kw in EXCLUDE_KEYWORDS)

def is_in_nova_scotia(lat, lng):
    """Validate that coordinates are within Nova Scotia bounds."""
    if not lat or not lng:
        return False
    
    try:
        lat_float = float(lat)
        lng_float = float(lng)
        
        # Check if within Nova Scotia bounding box
        return (NS_LAT_MIN <= lat_float <= NS_LAT_MAX and 
                NS_LNG_MIN <= lng_float <= NS_LNG_MAX)
    except (ValueError, TypeError):
        return False

def write_csv(filepath, rows):
    """Append rows to CSV."""
    file_exists = os.path.exists(filepath)
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

# ============================================================
# GOOGLE PLACES API
# ============================================================

def search_places_api(location):
    """Search Google Places API for restaurants in a location."""
    url = "https://places.googleapis.com/v1/places:searchText"
    
    query = f"{SEARCH_QUERY} {location}"
    
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
        "maxResultCount": MAX_RESULTS_PER_SEARCH
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        if response.status_code == 200:
            return response.json().get("places", [])
        else:
            print(f"    API Error {response.status_code}")
            return []
    except Exception as e:
        print(f"    Request failed: {e}")
        return []

def get_place_details(place_id):
    """Get full place details."""
    url = f"https://places.googleapis.com/v1/places/{place_id}"
    
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_API_KEY,
        "X-Goog-FieldMask": (
            "id,displayName,formattedAddress,internationalPhoneNumber,"
            "nationalPhoneNumber,websiteUri,googleMapsUri,location,"
            "rating,userRatingCount,priceLevel,types,"
            "regularOpeningHours,businessStatus,"
            "servesDinner,servesWine,servesBeer,"
            "reservable,dineIn,takeout,delivery,"
            "photos.name"
        )
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except Exception as e:
        return None

def parse_hours(opening_hours):
    """Parse opening hours."""
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    hours_dict = {f"Hours {day}": "" for day in days}
    
    if not opening_hours or "weekdayDescriptions" not in opening_hours:
        return hours_dict
    
    for desc in opening_hours.get("weekdayDescriptions", []):
        if ": " in desc:
            day, hours = desc.split(": ", 1)
            hours_dict[f"Hours {day}"] = hours
    
    return hours_dict

def get_photo_urls(photos):
    """Extract photo URLs."""
    photo_urls = {"Photo URL 1": "", "Photo URL 2": "", "Photo URL 3": ""}
    
    if not photos:
        return photo_urls
    
    for i, photo in enumerate(photos[:3], 1):
        photo_name = photo.get("name", "")
        if photo_name:
            photo_url = f"https://places.googleapis.com/v1/{photo_name}/media?key={GOOGLE_API_KEY}&maxHeightPx=1600&maxWidthPx=1600"
            photo_urls[f"Photo URL {i}"] = photo_url
    
    return photo_urls

def parse_place_with_details(place, details, search_location):
    """Parse place into CSV format."""
    data = details if details else place
    
    name = data.get("displayName", {}).get("text", "")
    if not name:
        return None
    
    # Get coordinates FIRST for validation
    location = data.get("location", {})
    lat = location.get("latitude", "")
    lng = location.get("longitude", "")
    
    # **CRITICAL: Filter out results not in Nova Scotia**
    if not is_in_nova_scotia(lat, lng):
        return None
    
    # Parse address
    address = data.get("formattedAddress", "")
    address_parts = address.split(", ") if address else []
    
    street = address_parts[0] if len(address_parts) > 0 else ""
    city = address_parts[1] if len(address_parts) > 1 else ""
    
    # Extract province and postal code from last part
    province = ""
    postal_code = ""
    if len(address_parts) >= 3:
        last_part = address_parts[-2]  # Second to last usually has province + postal
        if "NS" in last_part or "Nova Scotia" in last_part:
            province = "NS"
            # Try to extract postal code
            parts = last_part.split()
            for i, part in enumerate(parts):
                if len(part) == 3 and part[0].isalpha() and part[1].isdigit() and part[2].isalpha():
                    if i + 1 < len(parts):
                        postal_code = f"{part} {parts[i+1]}"
                    break
    
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
    
    # Parse hours
    hours = parse_hours(data.get("regularOpeningHours")) if details else {
        f"Hours {day}": "" for day in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    }
    
    # Parse photos
    photos = get_photo_urls(data.get("photos", [])) if details else {
        "Photo URL 1": "", "Photo URL 2": "", "Photo URL 3": ""
    }
    
    # Build result
    result = {
        "Restaurant Name": name,
        "Street Address": street,
        "City": city,
        "Province": province,
        "Postal Code": postal_code,
        "Country": "Canada",
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
        "Search Location": search_location,
        "Date Scraped": datetime.now().strftime("%Y-%m-%d")
    }
    
    result.update(hours)
    result.update(photos)
    
    return result

# ============================================================
# MAIN SCRAPER
# ============================================================

def run_scraper():
    """Run the Nova Scotia scraper."""
    progress = load_progress()
    completed_locations = set(progress.get("completed_locations", []))
    seen_place_ids = set()
    
    output_file = get_output_filename()
    
    print(f"\n{'='*70}")
    print(f"  Nova Scotia Fine Dining Scraper")
    print(f"{'='*70}")
    print(f"  Locations     : {len(NOVA_SCOTIA_LOCATIONS)} cities/towns")
    print(f"  Search Query  : {SEARCH_QUERY}")
    print(f"  Filters       : Rating ≥{MIN_RATING} | Price ≥{'$'*MIN_PRICE_LEVEL} | Reviews ≥{MIN_REVIEWS}")
    print(f"  Geo Bounds    : Lat {NS_LAT_MIN}-{NS_LAT_MAX}°N | Lng {NS_LNG_MIN}-{NS_LNG_MAX}°W")
    print(f"  Output File   : {output_file}")
    print(f"  Progress      : {len(completed_locations)}/{len(NOVA_SCOTIA_LOCATIONS)} locations | {progress.get('total_qualified', 0)} qualified")
    print(f"{'='*70}\n")
    
    total_qualified = 0
    
    for idx, location in enumerate(NOVA_SCOTIA_LOCATIONS, 1):
        if location in completed_locations:
            continue
        
        print(f"\n[{idx}/{len(NOVA_SCOTIA_LOCATIONS)}] {location}")
        
        # Search
        places = search_places_api(location)
        progress["search_api_calls"] = progress.get("search_api_calls", 0) + 1
        
        if not places:
            print(f"    No results")
            completed_locations.add(location)
            progress["completed_locations"] = list(completed_locations)
            save_progress(progress)
            time.sleep(API_DELAY)
            continue
        
        print(f"    Found {len(places)} results, validating coordinates...")
        
        # Filter by coordinates first (before getting details)
        valid_places = []
        for place in places:
            location_data = place.get("location", {})
            lat = location_data.get("latitude", "")
            lng = location_data.get("longitude", "")
            
            if is_in_nova_scotia(lat, lng):
                valid_places.append(place)
            else:
                progress["filtered_out_of_bounds"] = progress.get("filtered_out_of_bounds", 0) + 1
        
        if not valid_places:
            print(f"    ⚠️  All results filtered (out of Nova Scotia bounds)")
            completed_locations.add(location)
            progress["completed_locations"] = list(completed_locations)
            save_progress(progress)
            time.sleep(API_DELAY)
            continue
        
        print(f"    ✓ {len(valid_places)} within Nova Scotia, fetching details...")
        
        # Process each valid place
        qualified = []
        for place in valid_places:
            place_id = place.get("id", "")
            if place_id in seen_place_ids:
                continue
            
            # Quick check
            quick_check = parse_place_with_details(place, None, location)
            if not quick_check:
                continue
            
            # Get details
            details = get_place_details(place_id)
            progress["details_api_calls"] = progress.get("details_api_calls", 0) + 1
            time.sleep(API_DELAY)
            
            # Parse
            parsed = parse_place_with_details(place, details, location)
            if parsed:
                seen_place_ids.add(place_id)
                qualified.append(parsed)
                print(f"      ✅ {parsed['Restaurant Name']} | {parsed['City']} | {parsed['Rating']}⭐")
        
        # Save
        if qualified:
            write_csv(output_file, qualified)
            total_qualified += len(qualified)
            progress["total_qualified"] = progress.get("total_qualified", 0) + len(qualified)
            print(f"    💾 Saved {len(qualified)} restaurants (total: {progress['total_qualified']})")
        
        # Update progress
        completed_locations.add(location)
        progress["completed_locations"] = list(completed_locations)
        progress["total_found"] = progress.get("total_found", 0) + len(places)
        save_progress(progress)
        
        time.sleep(API_DELAY)
    
    search_cost = progress.get('search_api_calls', 0) * 0.032
    details_cost = progress.get('details_api_calls', 0) * 0.017
    total_cost = search_cost + details_cost
    
    print(f"\n\n{'='*70}")
    print(f"  🎉 NOVA SCOTIA COMPLETE!")
    print(f"{'='*70}")
    print(f"  Locations Searched       : {len(completed_locations)}")
    print(f"  Search API Calls         : {progress['search_api_calls']} (${search_cost:.2f})")
    print(f"  Details API Calls        : {progress['details_api_calls']} (${details_cost:.2f})")
    print(f"  Filtered (Out of Bounds) : {progress.get('filtered_out_of_bounds', 0)}")
    print(f"  Qualified Restaurants    : {progress['total_qualified']}")
    print(f"  Total Cost               : ${total_cost:.2f}")
    print(f"  Output File              : {output_file}")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    run_scraper()
