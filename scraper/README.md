# DiRoNA Lead Scraper

Scrapes Google Maps **ZIP code by ZIP code** for qualifying fine dining restaurants.
Outputs a **CSV file per state** to the `output/` folder, ready to import into Pipedrive.

## Setup (One Time)

```bash
# 1. Install Python 3.9+
# 2. Install dependencies
pip install -r requirements.txt

# 3. Install Playwright browser
playwright install chromium
```

## How to Run

### Single State
```bash
cd scraper
python dirona_scraper.py "Illinois"
python dirona_scraper.py "New York"
python dirona_scraper.py "Florida"
```

### Full National Run (all 50 states)
```bash
python dirona_scraper.py ALL
```

## How It Works

1. Loops through **every ZIP code** in the target state
2. For each ZIP, searches **5 categories**: fine dining, steakhouse, french restaurant, italian restaurant, seafood restaurant
3. **Pauses randomly 2.5–5 seconds** between every single request
4. Filters by DiRoNA criteria and **deduplicates** results
5. **Writes results to CSV after each ZIP** — nothing is lost if interrupted
6. **Auto-resumes** from the last completed ZIP if restarted

## Your CSV Output

Files are saved to the `output/` folder:
```
output/dirona_leads_Illinois_20260302.csv
output/dirona_leads_New_York_20260302.csv
```

Open in Excel or Google Sheets, review, then import directly into Pipedrive.

## CSV Columns

| Column | Description |
|--------|-------------|
| Restaurant Name | Full name |
| Contact Name | Owner/manager (if available) |
| Street Address | Full street address |
| City | City |
| ZIP | ZIP code searched |
| State | State |
| Country | United States |
| Price Level | $$$  or $$$$ |
| Description | Google short description |
| Phone | Phone number |
| Email | Email (if listed) |
| Website | Website URL |
| Cuisine Type | Category from Google |
| Has Bar | Yes/No |
| Review Count | Number of Google reviews |
| Rating | Google star rating |
| Facebook | Facebook URL |
| Twitter | Twitter/X URL |
| Instagram | Instagram URL |
| Google Maps URL | Direct Google Maps link |
| Search Category | Which of the 5 searches found it |
| ZIP Searched | ZIP code used |
| Date Scraped | Date run |

## Filters Applied

| Filter | Value |
|--------|-------|
| Minimum Rating | 4.0+ stars |
| Minimum Price | $$$ or $$$$ |
| Minimum Reviews | 50+ |
| Excluded Types | Buffet, Chinese, Brazilian, All-you-can-eat, Pizzeria, Gastropub, Thai, Indian |
