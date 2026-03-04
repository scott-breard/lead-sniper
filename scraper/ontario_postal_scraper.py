#!/usr/bin/env python3
"""
Canadian Restaurant Scraper - Ontario Edition (By Postal Code)
Searches ALL Ontario postal codes (FSAs) for fine dining restaurants.
Uses Google Places API with Place Details for complete data.
STRICT GEOGRAPHIC VALIDATION - Only returns restaurants within Ontario bounds.
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
MIN_REVIEWS = 25

# Exclusion keywords
EXCLUDE_KEYWORDS = [
    "buffet", "chinese", "brazilian", "all you can eat",
    "pizzeria", "pizza", "gastropub", "thai", "indian",
    "fast food", "fast casual", "mcdonalds", "subway", "chipotle",
    "tim hortons", "tims"
]

# Geographic bounds for Ontario (STRICT)
ON_LAT_MIN = 41.7
ON_LAT_MAX = 56.9
ON_LNG_MIN = -95.2
ON_LNG_MAX = -74.3

# API settings
API_DELAY = 0.1
MAX_RESULTS_PER_SEARCH = 20

# File paths
OUTPUT_DIR = "output"
PROGRESS_FILE = "scraper/ontario_postal_progress.json"

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

# ALL Ontario Forward Sortation Areas (FSAs) - postal code prefixes
# Ontario uses: K*, L*, M*, N*, P* series
ONTARIO_FSAs = [
    # K series - Eastern Ontario (Ottawa region)
    "K0A", "K0B", "K0C", "K0E", "K0G", "K0H", "K0J", "K0K", "K0L", "K0M",
    "K1A", "K1B", "K1C", "K1E", "K1G", "K1H", "K1J", "K1K", "K1L", "K1M",
    "K1N", "K1P", "K1R", "K1S", "K1T", "K1V", "K1W", "K1X", "K1Y", "K1Z",
    "K2A", "K2B", "K2C", "K2E", "K2G", "K2H", "K2J", "K2K", "K2L", "K2M",
    "K2P", "K2R", "K2S", "K2T", "K2V", "K2W",
    "K4A", "K4B", "K4C", "K4K", "K4M", "K4P", "K4R",
    "K6A", "K6H", "K6J", "K6K", "K6T", "K6V",
    "K7A", "K7C", "K7G", "K7H", "K7K", "K7L", "K7M", "K7N", "K7P", "K7R", "K7S", "K7V",
    "K8A", "K8B", "K8H", "K8N", "K8P", "K8R", "K8V",
    "K9A", "K9H", "K9J", "K9K", "K9L", "K9V",
    
    # L series - Southern/Central Ontario (GTA, Hamilton, Niagara)
    "L0A", "L0B", "L0C", "L0E", "L0G", "L0H", "L0J", "L0K", "L0L", "L0M", "L0N", "L0P", "L0R", "L0S",
    "L1A", "L1B", "L1C", "L1E", "L1G", "L1H", "L1J", "L1K", "L1L", "L1M", "L1N", "L1P", "L1R", "L1S", "L1T", "L1V", "L1W", "L1X", "L1Y", "L1Z",
    "L2A", "L2E", "L2G", "L2H", "L2J", "L2M", "L2N", "L2P", "L2R", "L2S", "L2T", "L2V", "L2W",
    "L3B", "L3C", "L3M", "L3P", "L3R", "L3S", "L3T", "L3V", "L3X", "L3Y", "L3Z",
    "L4A", "L4B", "L4C", "L4E", "L4G", "L4H", "L4J", "L4K", "L4L", "L4M", "L4N", "L4P", "L4R", "L4S", "L4T", "L4V", "L4W", "L4X", "L4Y", "L4Z",
    "L5A", "L5B", "L5C", "L5E", "L5G", "L5H", "L5J", "L5K", "L5L", "L5M", "L5N", "L5P", "L5R", "L5S", "L5T", "L5V", "L5W",
    "L6A", "L6B", "L6C", "L6E", "L6G", "L6H", "L6J", "L6K", "L6L", "L6M", "L6P", "L6R", "L6S", "L6T", "L6V", "L6W", "L6X", "L6Y", "L6Z",
    "L7A", "L7B", "L7C", "L7E", "L7G", "L7J", "L7K", "L7L", "L7M", "L7N", "L7P", "L7R", "L7S", "L7T",
    "L8E", "L8G", "L8H", "L8J", "L8K", "L8L", "L8M", "L8N", "L8P", "L8R", "L8S", "L8T", "L8V", "L8W",
    "L9A", "L9B", "L9C", "L9E", "L9G", "L9H", "L9J", "L9K", "L9L", "L9M", "L9N", "L9P", "L9R", "L9S", "L9T", "L9V", "L9W", "L9X", "L9Y",
    
    # M series - Toronto
    "M1B", "M1C", "M1E", "M1G", "M1H", "M1J", "M1K", "M1L", "M1M", "M1N", "M1P", "M1R", "M1S", "M1T", "M1V", "M1W", "M1X",
    "M2H", "M2J", "M2K", "M2L", "M2M", "M2N", "M2P", "M2R",
    "M3A", "M3B", "M3C", "M3H", "M3J", "M3K", "M3L", "M3M", "M3N",
    "M4A", "M4B", "M4C", "M4E", "M4G", "M4H", "M4J", "M4K", "M4L", "M4M", "M4N", "M4P", "M4R", "M4S", "M4T", "M4V", "M4W", "M4X", "M4Y",
    "M5A", "M5B", "M5C", "M5E", "M5G", "M5H", "M5J", "M5K", "M5L", "M5M", "M5N", "M5P", "M5R", "M5S", "M5T", "M5V", "M5W", "M5X",
    "M6A", "M6B", "M6C", "M6E", "M6G", "M6H", "M6J", "M6K", "M6L", "M6M", "M6N", "M6P", "M6R", "M6S",
    "M7A", "M7R", "M7Y",
    "M8V", "M8W", "M8X", "M8Y", "M8Z",
    "M9A", "M9B", "M9C", "M9L", "M9M", "M9N", "M9P", "M9R", "M9V", "M9W",
    
    # N series - Southwestern Ontario (London, Windsor, Kitchener)
    "N0A", "N0B", "N0C", "N0E", "N0G", "N0H", "N0J", "N0K", "N0L", "N0M", "N0N", "N0P", "N0R",
    "N1A", "N1C", "N1E", "N1G", "N1H", "N1K", "N1L", "N1M", "N1P", "N1R", "N1S", "N1T",
    "N2A", "N2B", "N2C", "N2E", "N2G", "N2H", "N2J", "N2K", "N2L", "N2M", "N2N", "N2P", "N2R", "N2T", "N2V", "N2Z",
    "N3A", "N3B", "N3C", "N3E", "N3H", "N3L", "N3P", "N3R", "N3S", "N3T", "N3V", "N3W", "N3Y",
    "N4B", "N4G", "N4K", "N4L", "N4N", "N4S", "N4T", "N4V", "N4W", "N4X", "N4Z",
    "N5A", "N5C", "N5H", "N5L", "N5P", "N5R", "N5V", "N5W", "N5X", "N5Y", "N5Z",
    "N6A", "N6B", "N6C", "N6E", "N6G", "N6H", "N6J", "N6K", "N6L", "N6M", "N6N", "N6P",
    "N7A", "N7G", "N7L", "N7M", "N7S", "N7T", "N7V", "N7W", "N7X",
    "N8A", "N8H", "N8M", "N8N", "N8P", "N8R", "N8S", "N8T", "N8V", "N8W", "N8X", "N8Y",
    "N9A", "N9B", "N9C", "N9E", "N9G", "N9H", "N9J", "N9K", "N9V", "N9Y",
    
    # P series - Northern Ontario (Sudbury, Thunder Bay, North Bay)
    "P0A", "P0B", "P0C", "P0E", "P0G", "P0H", "P0J", "P0K", "P0L", "P0M", "P0N", "P0P", "P0R", "P0S", "P0T", "P0V", "P0W", "P0X", "P0Y",
    "P1A", "P1B", "P1C", "P1H", "P1L", "P1P",
    "P2A", "P2B", "P2N",
    "P3A", "P3B", "P3C", "P3E", "P3G", "P3L", "P3N", "P3P", "P3Y",
    "P4N", "P4P", "P4R",
    "P5A", "P5E", "P5N",
    "P6A", "P6B", "P6C",
    "P7A", "P7B", "P7C", "P7E", "P7G", "P7J", "P7K", "P7L",
    "P8N", "P8T",
    "P9A", "P9N"
]

PROVINCE_ABBREV = {"Ontario": "ON"}

# ============================================================
# HELPERS
# ============================================================

def get_output_filename():
    """Generate output filename."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    return os.path.join(OUTPUT_DIR, f"dirona_leads_Ontario_ON_{date_str}.csv")

def load_progress():
    """Load scraping progress."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {
        "completed_fsas": [],
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

def is_in_ontario(lat, lng):
    """STRICT validation that coordinates are within Ontario bounds."""
    if not lat or not lng:
        return False
    
    try:
        lat_float = float(lat)
        lng_float = float(lng)
        
        in_bounds = (ON_LAT_MIN <= lat_float <= ON_LAT_MAX and 
                     ON_LNG_MIN <= lng_float <= ON_LNG_MAX)
        
        return in_bounds
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

def search_places_api(fsa_code):
    """Search Google Places API for restaurants in a postal code area."""
    url = "https://places.googleapis.com/v1/places:searchText"
    
    query = f"{SEARCH_QUERY} {fsa_code} Ontario Canada"
    
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
            return []
    except Exception as e:
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

def parse_place_with_details(place, details, search_fsa):
    """Parse place into CSV format with STRICT Ontario validation."""
    data = details if details else place
    
    name = data.get("displayName", {}).get("text", "")
    if not name:
        return None
    
    # Get coordinates FIRST for validation
    location = data.get("location", {})
    lat = location.get("latitude", "")
    lng = location.get("longitude", "")
    
    # **CRITICAL: STRICT geographic filter - ONLY Ontario**
    if not is_in_ontario(lat, lng):
        return None
    
    # Parse address
    address = data.get("formattedAddress", "")
    address_parts = address.split(", ") if address else []
    
    street = address_parts[0] if len(address_parts) > 0 else ""
    city = address_parts[1] if len(address_parts) > 1 else ""
    
    # Extract province and postal code
    province = ""
    postal_code = ""
    if len(address_parts) >= 3:
        last_part = address_parts[-2]
        if "ON" in last_part or "Ontario" in last_part:
            province = "ON"
            # Try to extract postal code
            parts = last_part.split()
            for i, part in enumerate(parts):
                if len(part) == 3 and part[0].isalpha() and part[1].isdigit() and part[2].isalpha():
                    if i + 1 < len(parts):
                        postal_code = f"{part} {parts[i+1]}"
                    break
    
    # Additional safety check: Reject if province isn't ON
    if province and province != "ON":
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
        "Search Location": search_fsa,
        "Date Scraped": datetime.now().strftime("%Y-%m-%d")
    }
    
    result.update(hours)
    result.update(photos)
    
    return result

# ============================================================
# MAIN SCRAPER
# ============================================================

def run_scraper():
    """Run the Ontario postal code scraper with STRICT geographic validation."""
    progress = load_progress()
    completed_fsas = set(progress.get("completed_fsas", []))
    seen_place_ids = set()
    
    output_file = get_output_filename()
    
    print(f"\n{'='*70}")
    print(f"  Ontario Fine Dining Scraper (By Postal Code)")
    print(f"  STRICT MODE: Only restaurants within ON bounds")
    print(f"{'='*70}")
    print(f"  FSAs (Postal Codes) : {len(ONTARIO_FSAs)} areas")
    print(f"  Search Query        : {SEARCH_QUERY}")
    print(f"  Filters             : Rating ≥{MIN_RATING} | Price ≥{'$'*MIN_PRICE_LEVEL} | Reviews ≥{MIN_REVIEWS}")
    print(f"  Geo Bounds          : Lat {ON_LAT_MIN}-{ON_LAT_MAX}°N | Lng {ON_LNG_MIN}-{ON_LNG_MAX}°W")
    print(f"  Output File         : {output_file}")
    print(f"  Progress            : {len(completed_fsas)}/{len(ONTARIO_FSAs)} FSAs | {progress.get('total_qualified', 0)} qualified")
    print(f"{'='*70}\n")
    
    total_qualified = 0
    
    for idx, fsa in enumerate(ONTARIO_FSAs, 1):
        if fsa in completed_fsas:
            continue
        
        print(f"\n[{idx}/{len(ONTARIO_FSAs)}] FSA: {fsa}")
        
        # Search
        places = search_places_api(fsa)
        progress["search_api_calls"] = progress.get("search_api_calls", 0) + 1
        
        if not places:
            completed_fsas.add(fsa)
            progress["completed_fsas"] = list(completed_fsas)
            save_progress(progress)
            time.sleep(API_DELAY)
            continue
        
        # STRICT filter by coordinates BEFORE getting details
        valid_places = []
        for place in places:
            location_data = place.get("location", {})
            lat = location_data.get("latitude", "")
            lng = location_data.get("longitude", "")
            
            if is_in_ontario(lat, lng):
                valid_places.append(place)
            else:
                progress["filtered_out_of_bounds"] = progress.get("filtered_out_of_bounds", 0) + 1
        
        if not valid_places:
            completed_fsas.add(fsa)
            progress["completed_fsas"] = list(completed_fsas)
            save_progress(progress)
            time.sleep(API_DELAY)
            continue
        
        print(f"    ✓ {len(valid_places)} within Ontario bounds")
        
        # Process each valid place
        qualified = []
        for place in valid_places:
            place_id = place.get("id", "")
            if place_id in seen_place_ids:
                continue
            
            # Quick check
            quick_check = parse_place_with_details(place, None, fsa)
            if not quick_check:
                continue
            
            # Get details
            details = get_place_details(place_id)
            progress["details_api_calls"] = progress.get("details_api_calls", 0) + 1
            time.sleep(API_DELAY)
            
            # Parse
            parsed = parse_place_with_details(place, details, fsa)
            if parsed:
                seen_place_ids.add(place_id)
                qualified.append(parsed)
                print(f"      ✅ {parsed['Restaurant Name']} | {parsed['City']}, ON")
        
        # Save
        if qualified:
            write_csv(output_file, qualified)
            total_qualified += len(qualified)
            progress["total_qualified"] = progress.get("total_qualified", 0) + len(qualified)
            print(f"    💾 Saved {len(qualified)} (total: {progress['total_qualified']})")
        
        # Update progress
        completed_fsas.add(fsa)
        progress["completed_fsas"] = list(completed_fsas)
        progress["total_found"] = progress.get("total_found", 0) + len(places)
        save_progress(progress)
        
        time.sleep(API_DELAY)
    
    search_cost = progress.get('search_api_calls', 0) * 0.032
    details_cost = progress.get('details_api_calls', 0) * 0.017
    total_cost = search_cost + details_cost
    
    print(f"\n\n{'='*70}")
    print(f"  🎉 ONTARIO COMPLETE!")
    print(f"{'='*70}")
    print(f"  FSAs Searched            : {len(completed_fsas)}")
    print(f"  Search API Calls         : {progress['search_api_calls']} (${search_cost:.2f})")
    print(f"  Details API Calls        : {progress['details_api_calls']} (${details_cost:.2f})")
    print(f"  Filtered (Out of Bounds) : {progress.get('filtered_out_of_bounds', 0)}")
    print(f"  Qualified Restaurants    : {progress['total_qualified']}")
    print(f"  Total Cost               : ${total_cost:.2f}")
    print(f"  Output File              : {output_file}")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    run_scraper()
