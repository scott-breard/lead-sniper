#!/usr/bin/env python3
"""
DiRōNA Restaurant Qualification System v2.0
============================================
Batch-processes a CSV of restaurant leads, visits each website,
extracts menus (HTML + PDF), and applies disqualification rules.

Requirements:
    pip3 install playwright pymupdf requests beautifulsoup4 pytesseract
    python3 -m playwright install chromium

Usage (CLI):
    python3 qualifier.py --input leads.csv --output qualified.csv

Usage (Web App):
    python3 qualifier.py --web
    Then open http://localhost:5050
"""

import argparse
import csv
import io
import json
import os
import re
import sys
import tempfile
import time
import traceback
from pathlib import Path
from urllib.parse import urljoin, urlparse

import fitz  # PyMuPDF
import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MIN_RATING = 4.0
MIN_PRICE_LEVEL = 3  # $$$ or higher

# Menu-link keywords (case-insensitive). If a link's text or href contains
# any of these, we follow it looking for menu content.
MENU_LINK_KEYWORDS = [
    "menu", "lunch", "dinner", "brunch", "breakfast", "food",
    "tasting", "prix fixe", "prix-fixe", "drinks", "wine",
    "cocktail", "dessert", "appetizer", "entree", "entrée",
    "specials", "seasonal", "supper", "carte", "dine",
    "dining", "eat", "cuisine", "à la carte", "a la carte",
    "bar menu", "happy hour", "raw bar", "chef",
]

# Max pages to follow per restaurant (prevents runaway crawling)
MAX_MENU_PAGES = 15
# Timeout for each HTTP request (seconds)
REQUEST_TIMEOUT = 15
# Max PDF size to download (10 MB)
MAX_PDF_SIZE = 10 * 1024 * 1024

# User-Agent to look like a real browser
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

HEADERS = {"User-Agent": USER_AGENT}

# Non-restaurant business types to auto-disqualify based on name/website text.
# These get caught by Google Places but are NOT restaurants.
NON_RESTAURANT_KEYWORDS = [
    r'\bnutrition\s*(?:center|centre|shop|store|bar|club|hub)\b',
    r'\bherbalife\b',
    r'\bjuice\s*(?:bar|shop|store|press)\b',
    r'\bsmoothie\s*(?:bar|shop|store|king)\b',
    r'\bcatering\s+(?:company|service|only)\b',
    r'\bfood\s+truck\b',
    r'\bghost\s+kitchen\b',
    r'\bcloud\s+kitchen\b',
    r'\bvending\b',
    r'\bmeal\s+prep\b',
    r'\bmeal\s+delivery\b',
    r'\bcafeteria\b',
    r'\bconvenience\s+store\b',
    r'\bgrocery\b',
    r'\bgas\s+station\b',
    r'\bsnack\s*(?:bar|shop|shack)\b',
    r'\benergy\s+(?:bar|drink|shop)\b',
    r'\bsupplement\b',
    r'\bprotein\s+(?:bar|shop|shake)\b',
    r'\bice\s+cream\s+(?:shop|parlor|parlour|stand|truck)\b',
    r'\bdonut\s*(?:shop|store)\b',
    r'\bdoughnut\s*(?:shop|store)\b',
    r'\bbakery\b',
    r'\bcoffee\s+shop\b',
    r'\bwedding\s+venue\b',
    r'\bevent\s+(?:center|centre|hall|space|venue)\b',
    r'\bbanquet\s+hall\b',
    r'\breception\s+hall\b',
    r'\bconference\s+center\b',
    r'\bconvention\s+center\b',
    r'\bcountry\s+club\b',
    r'\bgolf\s+club\b',
    r'\bcabin\s+rental\b',
    r'\bhotel\b',
    r'\bmotel\b',
    r'\bresort\b(?!.*\brestaurant)',
    r'\bbed\s+(?:&|and)\s+breakfast\b',
    r'\bcookware\b',
    r'\bkitchen\s+(?:supply|store|shop)\b',
    r'\bcooking\s+(?:class|school|academy)\b',
    r'\bculinary\s+(?:school|academy|institute)\b',
]

NON_RESTAURANT_COMPILED = [re.compile(p, re.IGNORECASE) for p in NON_RESTAURANT_KEYWORDS]


# ---------------------------------------------------------------------------
# Disqualification Rules
# ---------------------------------------------------------------------------
class DisqualificationEngine:
    """
    Applies DiRōNA disqualification rules against extracted menu text.

    Rules:
    - More than 1 pizza item → disqualified
    - More than 1 burger item → disqualified
    - Any nachos → disqualified
    - Any fish and chips → disqualified
    - Any BBQ ribs (not short rib, braised rib, bone-in ribeye etc.) → disqualified
    - Any buffet / all-you-can-eat → disqualified
    - Brazilian steakhouse / churrascaria → disqualified
    - Any hot pot → disqualified
    - Any teppanyaki → disqualified
    """

    def __init__(self):
        # --- Single-instance disqualifiers (any one match = disqualified) ---
        self.instant_disqualifiers = {
            "nachos": [
                r'\bnachos?\b',
            ],
            "fish and chips": [
                r'\bfish\s*(?:&|and|n|n\')\s*chips?\b',
                r'\bfish\s+chips?\b',
            ],
            "bbq ribs": [
                r'\bbbq\s+ribs?\b',
                r'\bbarbecue\s+ribs?\b',
                r'\bbar[\-\s]?b[\-\s]?que?\s+ribs?\b',
                r'\bsmoked\s+ribs?\b',
                r'\bbaby\s+back\s+ribs?\b',
                r'\bspare\s*ribs?\b',
                r'\bst\.?\s*louis\s+ribs?\b',
                r'\bfull\s+rack\b',
                r'\bhalf\s+rack\b',
                r'\brack\s+of\s+ribs?\b',
                r'\bpork\s+ribs?\b',
                r'\bbeef\s+ribs?\b',
            ],
            "buffet / all-you-can-eat": [
                r'\bbuffet\b',
                r'\ball[\-\s]you[\-\s]can[\-\s]eat\b',
                r'\bunlimited\s+(?:dining|food|plates?|servings?)\b',
                r'\beat\s+all\s+you\s+(?:can|want)\b',
            ],
            "brazilian steakhouse": [
                r'\bbrazilian\s+steak\s*house\b',
                r'\bchurrascaria\b',
                r'\bchurrasco\b',
                r'\bbrazilian\s+grill\b',
                r'\brodizio\b',
                r'\bbrazilian\s+bbq\b',
                r'\bbrazilian\s+barbecue\b',
            ],
            "hot pot": [
                r'\bhot\s*pot\b',
                r'\bhotpot\b',
                r'\bshabu[\-\s]?shabu\b',
            ],
            "teppanyaki": [
                r'\bteppanyaki\b',
                r'\bteppan\b',
                r'\bhibachi\s+grill\b',
            ],
        }

        # --- Counted disqualifiers (threshold-based) ---
        # Each match on a SEPARATE menu line/item counts as 1
        self.counted_disqualifiers = {
            "pizza": {
                "patterns": [
                    r'\bpizza\b',
                    r'\bpizzas\b',
                    r'\bmargherita\s+pizza\b',
                    r'\bpizza\s+margherita\b',
                    r'\bneapolitan\b(?=.*\bpizza\b)',
                    r'\bcalzone\b',
                ],
                "max_allowed": 1,
                "label": "more than 1 pizza",
            },
            "burger": {
                "patterns": [
                    r'\bburger\b',
                    r'\bburgers\b',
                    r'\bhamburger\b',
                    r'\bcheeseburger\b',
                    r'\bsliders?\b',  # sliders are mini burgers
                ],
                "max_allowed": 1,
                "label": "more than 1 burger",
            },
        }

        # --- Allowlist: patterns that should NOT count as BBQ ribs ---
        self.rib_allowlist = [
            r'\bshort\s+rib',
            r'\bbraised\s+(?:\w+\s+)*rib',  # braised beef rib, braised rib, etc.
            r'\bbone[\-\s]?in\s+rib\s*eye',
            r'\bribeye\b',
            r'\brib[\-\s]?eye\b',
            r'\bprime\s+rib\b',
            r'\bstanding\s+rib\b',
            r'\brib\s+roast\b',
            r'\brib\s+chop\b',
            r'\blamb\s+rib',
            r'\bveal\s+rib',
            r'\brib\s+cap\b',
            r'\bribbon\b',
            r'\brib\s+hash\b',
            r'\bglazed\s+(?:\w+\s+)*rib',
            r'\bslow[\-\s]cooked\s+(?:\w+\s+)*rib',
            r'\bwine[\-\s]braised\s+(?:\w+\s+)*rib',
        ]

        # Compile all patterns
        self._compile_patterns()

    def _compile_patterns(self):
        """Pre-compile all regex patterns for speed."""
        self._instant_compiled = {}
        for name, patterns in self.instant_disqualifiers.items():
            self._instant_compiled[name] = [
                re.compile(p, re.IGNORECASE) for p in patterns
            ]

        self._counted_compiled = {}
        for name, config in self.counted_disqualifiers.items():
            self._counted_compiled[name] = {
                "patterns": [re.compile(p, re.IGNORECASE) for p in config["patterns"]],
                "max_allowed": config["max_allowed"],
                "label": config["label"],
            }

        self._rib_allowlist_compiled = [
            re.compile(p, re.IGNORECASE) for p in self.rib_allowlist
        ]

    def _is_allowed_rib(self, line: str) -> bool:
        """Check if a rib mention is on the allowlist (fine dining rib cuts)."""
        for pattern in self._rib_allowlist_compiled:
            if pattern.search(line):
                return True
        return False

    def check_menu(self, menu_text: str) -> dict:
        """
        Analyze menu text and return qualification result.

        Returns:
            {
                "qualified": True/False,
                "disqualifiers": ["nachos", "more than 1 pizza", ...],
                "details": ["Found 'nachos' in menu line: ...", ...],
            }
        """
        if not menu_text or not menu_text.strip():
            return {
                "qualified": None,
                "disqualifiers": [],
                "details": ["No menu text to analyze"],
            }

        disqualifiers = []
        details = []

        # Normalize text
        text_lower = menu_text.lower()
        lines = [l.strip() for l in menu_text.split("\n") if l.strip()]

        # --- Check instant disqualifiers ---
        for name, patterns in self._instant_compiled.items():
            found = False
            for pattern in patterns:
                for line in lines:
                    line_lower = line.lower()
                    match = pattern.search(line_lower)
                    if match:
                        # Special handling for BBQ ribs: check allowlist
                        if name == "bbq ribs":
                            if self._is_allowed_rib(line_lower):
                                continue
                        found = True
                        matched_text = match.group()
                        details.append(
                            f"[{name.upper()}] Found '{matched_text}' → \"{line[:120]}\""
                        )
                        break
                if found:
                    break
            if found:
                disqualifiers.append(name)

        # --- Check counted disqualifiers (pizza, burger) ---
        for name, config in self._counted_compiled.items():
            count = 0
            matched_lines = []
            seen_lines = set()

            for pattern in config["patterns"]:
                for line in lines:
                    line_lower = line.lower()
                    if line_lower in seen_lines:
                        continue
                    if pattern.search(line_lower):
                        count += 1
                        seen_lines.add(line_lower)
                        matched_lines.append(line[:120])

            if count > config["max_allowed"]:
                disqualifiers.append(config["label"])
                details.append(
                    f"[{name.upper()}] Found {count} items (max {config['max_allowed']}): "
                    + " | ".join(matched_lines[:5])
                )

        # --- Check menu prices: if we can read prices and they're all under $30, disqualify ---
        prices = self._extract_prices(menu_text)
        if prices:
            # Use the 75th percentile of prices to judge — avoids outliers
            # like a $5 side or a $200 wine skewing the result
            sorted_prices = sorted(prices)
            p75_index = int(len(sorted_prices) * 0.75)
            top_entree_price = sorted_prices[p75_index]
            avg_price = sum(prices) / len(prices)

            if top_entree_price < 30:
                disqualifiers.append("low menu prices")
                details.append(
                    f"[LOW PRICES] {len(prices)} prices found — "
                    f"75th percentile: ${top_entree_price:.0f}, "
                    f"avg: ${avg_price:.0f}, "
                    f"max: ${sorted_prices[-1]:.0f}"
                )

        qualified = len(disqualifiers) == 0
        return {
            "qualified": qualified,
            "disqualifiers": disqualifiers,
            "details": details,
        }

    def _extract_prices(self, text: str) -> list:
        """
        Extract dollar prices from menu text.
        Handles: $25, $25.00, US$30, $20-30, $20–30, 25$, etc.
        Returns list of floats.
        """
        prices = []

        # Pattern 1: $XX or US$XX or CA$XX (most common)
        for m in re.finditer(r'(?:US|CA|C)?\$\s*(\d+(?:\.\d{2})?)', text):
            try:
                price = float(m.group(1))
                if 5 <= price <= 500:  # filter out noise like years ($2026)
                    prices.append(price)
            except ValueError:
                pass

        # Pattern 2: Price ranges like $20-30 or $20–30 (take the higher number)
        for m in re.finditer(r'(?:US|CA|C)?\$\s*\d+(?:\.\d{2})?\s*[\-–—]\s*(\d+(?:\.\d{2})?)', text):
            try:
                price = float(m.group(1))
                if 5 <= price <= 500:
                    prices.append(price)
            except ValueError:
                pass

        # Pattern 3: XX$ format (less common)
        for m in re.finditer(r'(\d+(?:\.\d{2})?)\s*\$', text):
            try:
                price = float(m.group(1))
                if 5 <= price <= 500:
                    prices.append(price)
            except ValueError:
                pass

        return prices


# ---------------------------------------------------------------------------
# Menu Extraction
# ---------------------------------------------------------------------------
class MenuExtractor:
    """
    Extracts menu text from a restaurant website.

    Strategy:
    1. Fetch homepage HTML (requests first, Playwright fallback for JS sites)
    2. Find all menu-related links (HTML pages + PDF links)
    3. Follow each link:
       - HTML → extract text with BeautifulSoup
       - PDF → download and extract text with PyMuPDF (+ OCR fallback)
    4. Combine all text for disqualification analysis
    """

    def __init__(self, use_playwright=True):
        self.use_playwright = use_playwright
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._playwright = None
        self._browser = None

    def _get_playwright_page(self):
        """Lazy-init Playwright browser."""
        if not self.use_playwright:
            return None

        if self._playwright is None:
            try:
                from playwright.sync_api import sync_playwright
                self._playwright = sync_playwright().start()
                self._browser = self._playwright.chromium.launch(
                    headless=True,
                    args=["--disable-blink-features=AutomationControlled"],
                )
            except Exception as e:
                print(f"  ⚠️  Playwright not available: {e}")
                print("  Falling back to requests-only mode.")
                self.use_playwright = False
                return None

        context = self._browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1920, "height": 1080},
        )
        page = context.new_page()
        page.set_default_timeout(REQUEST_TIMEOUT * 1000)
        return page

    def _fetch_html_requests(self, url: str) -> str:
        """Fetch page HTML with requests library."""
        try:
            resp = self.session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            resp.raise_for_status()
            return resp.text
        except Exception:
            return ""

    def _fetch_html_playwright(self, url: str) -> str:
        """Fetch page HTML with Playwright (handles JS-rendered content)."""
        page = None
        try:
            page = self._get_playwright_page()
            if page is None:
                return ""
            page.goto(url, wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT * 1000)
            # Wait a bit for JS to render menus
            page.wait_for_timeout(2000)
            html = page.content()
            return html
        except Exception:
            return ""
        finally:
            if page:
                try:
                    page.context.close()
                except Exception:
                    pass

    def _fetch_html(self, url: str) -> str:
        """Fetch HTML, trying requests first then Playwright fallback."""
        html = self._fetch_html_requests(url)

        # If the page is mostly empty or looks like a JS shell, try Playwright
        if html:
            soup = BeautifulSoup(html, "html.parser")
            text = soup.get_text(separator=" ", strip=True)
            if len(text) > 100:
                return html

        if self.use_playwright:
            pw_html = self._fetch_html_playwright(url)
            if pw_html:
                return pw_html

        return html

    def _extract_text_from_html(self, html: str) -> str:
        """Extract visible text from HTML."""
        if not html:
            return ""
        soup = BeautifulSoup(html, "html.parser")

        # Remove script, style, nav, header, footer elements
        for tag in soup(["script", "style", "noscript", "svg", "path"]):
            tag.decompose()

        text = soup.get_text(separator="\n", strip=True)
        # Clean up excessive whitespace
        lines = [l.strip() for l in text.split("\n") if l.strip()]
        return "\n".join(lines)

    def _find_menu_links(self, html: str, base_url: str) -> list:
        """
        Find all links on a page that likely lead to menus.
        Returns list of (url, is_pdf) tuples.
        """
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        menu_links = []
        seen_urls = set()

        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
                continue

            # Resolve relative URLs
            full_url = urljoin(base_url, href)

            # Skip external links (different domain)
            base_domain = urlparse(base_url).netloc.replace("www.", "")
            link_domain = urlparse(full_url).netloc.replace("www.", "")

            # Allow same domain + common menu hosting domains
            menu_hosts = ["squarespace", "wixsite", "toast", "popmenu", "bentobox",
                          "getbento", "chownow", "doordash", "grubhub"]
            same_domain = (base_domain == link_domain) or any(
                h in link_domain for h in menu_hosts
            )
            if not same_domain:
                # Also allow if it's a PDF on any domain
                if not full_url.lower().endswith(".pdf"):
                    continue

            # Normalize URL
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            # Check if the link text or URL contains menu keywords
            link_text = tag.get_text(strip=True).lower()
            href_lower = full_url.lower()

            is_menu_link = False
            is_pdf = href_lower.endswith(".pdf")

            for keyword in MENU_LINK_KEYWORDS:
                if keyword in link_text or keyword in href_lower:
                    is_menu_link = True
                    break

            # PDFs on the same domain are likely menus even without keyword match
            if is_pdf and same_domain:
                is_menu_link = True

            if is_menu_link:
                menu_links.append((full_url, is_pdf))

        return menu_links

    def _download_pdf(self, url: str) -> bytes:
        """Download a PDF file from a URL."""
        try:
            resp = self.session.get(
                url, timeout=REQUEST_TIMEOUT, allow_redirects=True,
                stream=True, headers={**HEADERS, "Accept": "application/pdf"}
            )
            resp.raise_for_status()

            # Check content length
            content_length = resp.headers.get("content-length")
            if content_length and int(content_length) > MAX_PDF_SIZE:
                return b""

            content = resp.content
            if len(content) > MAX_PDF_SIZE:
                return b""

            return content
        except Exception:
            return b""

    def _extract_text_from_pdf(self, pdf_bytes: bytes) -> str:
        """
        Extract text from PDF bytes using PyMuPDF.
        Falls back to OCR for image-based PDFs.
        """
        if not pdf_bytes:
            return ""

        text_parts = []
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            for page_num in range(len(doc)):
                page = doc[page_num]

                # Try normal text extraction first
                text = page.get_text("text")

                # If very little text found, the PDF might be image-based
                if len(text.strip()) < 20:
                    try:
                        # Try OCR via PyMuPDF's built-in Tesseract support
                        text = page.get_text("text", flags=fitz.TEXT_PRESERVE_WHITESPACE)
                        if len(text.strip()) < 20:
                            # Render page as image and try OCR
                            pix = page.get_pixmap(dpi=300)
                            # Save to temp file for OCR
                            try:
                                import pytesseract
                                from PIL import Image
                                img_bytes = pix.tobytes("png")
                                img = Image.open(io.BytesIO(img_bytes))
                                text = pytesseract.image_to_string(img)
                            except ImportError:
                                # pytesseract not installed, skip OCR
                                pass
                    except Exception:
                        pass

                if text.strip():
                    text_parts.append(text)

            doc.close()
        except Exception:
            pass

        return "\n".join(text_parts)

    def extract_menu(self, website_url: str) -> dict:
        """
        Extract all menu text from a restaurant website.

        Returns:
            {
                "success": True/False,
                "menu_text": "combined menu text...",
                "pages_checked": 5,
                "pdfs_read": 2,
                "errors": ["..."],
            }
        """
        if not website_url or not website_url.strip():
            return {
                "success": False,
                "menu_text": "",
                "pages_checked": 0,
                "pdfs_read": 0,
                "errors": ["No website URL provided"],
            }

        # Normalize URL
        url = website_url.strip()
        if not url.startswith("http"):
            url = "https://" + url

        all_text_parts = []
        errors = []
        pages_checked = 0
        pdfs_read = 0

        # Step 1: Fetch homepage
        print(f"    Fetching homepage: {url}")
        homepage_html = self._fetch_html(url)
        if not homepage_html:
            return {
                "success": False,
                "menu_text": "",
                "pages_checked": 0,
                "pdfs_read": 0,
                "errors": [f"Could not fetch homepage: {url}"],
            }

        homepage_text = self._extract_text_from_html(homepage_html)
        all_text_parts.append(homepage_text)
        pages_checked += 1

        # Step 2: Find menu links
        menu_links = self._find_menu_links(homepage_html, url)
        print(f"    Found {len(menu_links)} menu-related links")

        # Step 3: Follow each menu link
        visited = {url}
        second_level_links = []

        for link_url, is_pdf in menu_links[:MAX_MENU_PAGES]:
            if link_url in visited:
                continue
            visited.add(link_url)

            if is_pdf:
                print(f"    📄 Reading PDF: {link_url[:80]}")
                pdf_bytes = self._download_pdf(link_url)
                if pdf_bytes:
                    pdf_text = self._extract_text_from_pdf(pdf_bytes)
                    if pdf_text.strip():
                        all_text_parts.append(pdf_text)
                        pdfs_read += 1
                    else:
                        errors.append(f"PDF empty/unreadable: {link_url[:80]}")
                else:
                    errors.append(f"PDF download failed: {link_url[:80]}")
            else:
                print(f"    🔗 Following menu link: {link_url[:80]}")
                page_html = self._fetch_html(link_url)
                if page_html:
                    page_text = self._extract_text_from_html(page_html)
                    all_text_parts.append(page_text)
                    pages_checked += 1

                    # Also check for PDF links on this sub-page
                    sub_links = self._find_menu_links(page_html, link_url)
                    for sub_url, sub_is_pdf in sub_links:
                        if sub_url not in visited:
                            second_level_links.append((sub_url, sub_is_pdf))
                else:
                    errors.append(f"Could not fetch: {link_url[:80]}")

        # Step 4: Follow second-level links (PDFs found on menu pages)
        for link_url, is_pdf in second_level_links[:MAX_MENU_PAGES - len(visited)]:
            if link_url in visited:
                continue
            visited.add(link_url)

            if is_pdf:
                print(f"    📄 Reading nested PDF: {link_url[:80]}")
                pdf_bytes = self._download_pdf(link_url)
                if pdf_bytes:
                    pdf_text = self._extract_text_from_pdf(pdf_bytes)
                    if pdf_text.strip():
                        all_text_parts.append(pdf_text)
                        pdfs_read += 1
            else:
                page_html = self._fetch_html(link_url)
                if page_html:
                    page_text = self._extract_text_from_html(page_html)
                    all_text_parts.append(page_text)
                    pages_checked += 1

        combined_text = "\n".join(all_text_parts)
        return {
            "success": bool(combined_text.strip()),
            "menu_text": combined_text,
            "pages_checked": pages_checked,
            "pdfs_read": pdfs_read,
            "errors": errors,
        }

    def close(self):
        """Clean up Playwright resources."""
        if self._browser:
            try:
                self._browser.close()
            except Exception:
                pass
        if self._playwright:
            try:
                self._playwright.stop()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# CSV Processor
# ---------------------------------------------------------------------------
def find_column(headers: list, candidates: list) -> int:
    """Find the index of a column by checking multiple candidate names."""
    for i, h in enumerate(headers):
        h_lower = h.strip().lower().replace("_", " ").replace("-", " ")
        for candidate in candidates:
            if candidate.lower() in h_lower:
                return i
    return -1


def process_csv(input_path: str, output_path: str, use_playwright: bool = True):
    """
    Process a CSV of restaurant leads through the qualification pipeline.
    """
    print("\n" + "=" * 70)
    print("  DiRōNA Restaurant Qualifier v2.0")
    print("=" * 70)
    print(f"  Input:  {input_path}")
    print(f"  Output: {output_path}")
    print(f"  Playwright: {'Enabled' if use_playwright else 'Disabled'}")
    print("=" * 70 + "\n")

    # Read input CSV
    with open(input_path, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        headers = next(reader)
        rows = list(reader)

    # Find relevant columns
    website_col = find_column(headers, ["website", "url", "web", "site", "homepage"])
    name_col = find_column(headers, ["name", "restaurant", "business"])
    rating_col = find_column(headers, ["rating", "stars", "google rating"])
    price_col = find_column(headers, ["price", "price level", "price_level", "$"])

    if website_col == -1:
        print("❌ ERROR: Could not find a 'website' column in the CSV.")
        print(f"   Available columns: {headers}")
        sys.exit(1)

    if name_col == -1:
        name_col = 0  # Default to first column

    print(f"  Columns detected:")
    print(f"    Name:    col {name_col} → '{headers[name_col]}'")
    print(f"    Website: col {website_col} → '{headers[website_col]}'")
    if rating_col >= 0:
        print(f"    Rating:  col {rating_col} → '{headers[rating_col]}'")
    if price_col >= 0:
        print(f"    Price:   col {price_col} → '{headers[price_col]}'")
    print(f"  Total restaurants: {len(rows)}\n")

    # Initialize components
    engine = DisqualificationEngine()
    extractor = MenuExtractor(use_playwright=use_playwright)

    # Output headers: original + new qualification columns
    out_headers = headers + [
        "qualification_status",
        "disqualifiers_found",
        "menu_pages_checked",
        "menu_pdfs_read",
        "menu_errors",
        "qualification_details",
    ]

    # Stats
    stats = {
        "total": len(rows),
        "eligible": 0,
        "disqualified": 0,
        "pre_filtered": 0,
        "menu_not_found": 0,
        "no_website": 0,
        "errors": 0,
    }

    results = []

    for idx, row in enumerate(rows):
        restaurant_name = row[name_col] if name_col < len(row) else f"Row {idx + 1}"
        website = row[website_col] if website_col < len(row) else ""

        print(f"\n[{idx + 1}/{len(rows)}] {restaurant_name}")

        # --- Pre-filter: not a restaurant ---
        name_lower = restaurant_name.lower()
        non_restaurant_match = None
        for pattern in NON_RESTAURANT_COMPILED:
            m = pattern.search(name_lower)
            if m:
                non_restaurant_match = m.group()
                break
        if non_restaurant_match:
            print(f"  ⛔ Not a restaurant: matched '{non_restaurant_match}'")
            stats["pre_filtered"] += 1
            results.append(row + [
                "not_a_restaurant",
                f"Business name matched: {non_restaurant_match}",
                "", "", "", ""
            ])
            continue

        # --- Pre-filter: rating ---
        if rating_col >= 0 and rating_col < len(row):
            try:
                rating = float(row[rating_col].replace(",", "."))
                if rating < MIN_RATING:
                    print(f"  ⛔ Pre-filtered: Rating {rating} < {MIN_RATING}")
                    stats["pre_filtered"] += 1
                    results.append(row + [
                        "pre_filtered",
                        f"Rating {rating} below {MIN_RATING}",
                        "", "", "", ""
                    ])
                    continue
            except (ValueError, IndexError):
                pass

        # --- Pre-filter: price level ---
        if price_col >= 0 and price_col < len(row):
            try:
                price_str = row[price_col].strip()
                # Handle "$$$" format or numeric "3"
                if price_str.startswith("$"):
                    price_level = len(price_str.replace(" ", ""))
                else:
                    price_level = int(float(price_str))

                if price_level < MIN_PRICE_LEVEL:
                    print(f"  ⛔ Pre-filtered: Price level {price_level} < {MIN_PRICE_LEVEL}")
                    stats["pre_filtered"] += 1
                    results.append(row + [
                        "pre_filtered",
                        f"Price level {price_level} below {MIN_PRICE_LEVEL}",
                        "", "", "", ""
                    ])
                    continue
            except (ValueError, IndexError):
                pass

        # --- Check website ---
        if not website or not website.strip():
            print(f"  ⚠️  No website")
            stats["no_website"] += 1
            results.append(row + [
                "no_website", "", "", "", "No website URL available", ""
            ])
            continue

        # --- Extract menu ---
        try:
            extraction = extractor.extract_menu(website)
        except Exception as e:
            print(f"  ❌ Extraction error: {e}")
            stats["errors"] += 1
            results.append(row + [
                "error", "", "", "", str(e)[:200], ""
            ])
            continue

        if not extraction["success"] or not extraction["menu_text"].strip():
            print(f"  ⚠️  Menu not found ({extraction['pages_checked']} pages, {extraction['pdfs_read']} PDFs)")
            stats["menu_not_found"] += 1
            error_str = "; ".join(extraction["errors"][:3]) if extraction["errors"] else "No menu text found"
            results.append(row + [
                "menu_not_found", "", str(extraction["pages_checked"]),
                str(extraction["pdfs_read"]), error_str, ""
            ])
            continue

        # --- Run disqualification check ---
        result = engine.check_menu(extraction["menu_text"])

        if result["qualified"] is True:
            status = "eligible"
            stats["eligible"] += 1
            print(f"  ✅ ELIGIBLE ({extraction['pages_checked']} pages, {extraction['pdfs_read']} PDFs checked)")
        elif result["qualified"] is False:
            status = "disqualified"
            stats["disqualified"] += 1
            disq_str = ", ".join(result["disqualifiers"])
            print(f"  ❌ DISQUALIFIED: {disq_str}")
        else:
            status = "review"
            stats["menu_not_found"] += 1
            print(f"  ⚠️  Needs manual review")

        results.append(row + [
            status,
            "; ".join(result["disqualifiers"]),
            str(extraction["pages_checked"]),
            str(extraction["pdfs_read"]),
            "; ".join(extraction["errors"][:3]),
            " | ".join(result["details"][:5]),
        ])

        # Small delay between restaurants to be polite
        time.sleep(0.5)

    # --- Write output CSV ---
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(out_headers)
        writer.writerows(results)

    # --- Print summary ---
    print("\n" + "=" * 70)
    print("  QUALIFICATION SUMMARY")
    print("=" * 70)
    print(f"  Total restaurants:     {stats['total']}")
    print(f"  ✅ Eligible:           {stats['eligible']}")
    print(f"  ❌ Disqualified:       {stats['disqualified']}")
    print(f"  ⛔ Pre-filtered:       {stats['pre_filtered']}")
    print(f"  ⚠️  Menu not found:    {stats['menu_not_found']}")
    print(f"  ⚠️  No website:        {stats['no_website']}")
    print(f"  💥 Errors:             {stats['errors']}")
    print("=" * 70)
    print(f"\n  Results saved to: {output_path}\n")

    # Cleanup
    extractor.close()

    return stats


# ---------------------------------------------------------------------------
# Flask Web App
# ---------------------------------------------------------------------------
def run_webapp(port=5050):
    """Run the Flask web application."""
    try:
        from flask import Flask, request, render_template_string, send_file, jsonify
    except ImportError:
        print("Flask not installed. Run: pip3 install flask")
        sys.exit(1)

    app = Flask(__name__)

    HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DiRōNA Restaurant Qualifier v2.0</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0a0a0a; color: #e0e0e0; padding: 2rem;
        }
        .container { max-width: 1000px; margin: 0 auto; }
        h1 { font-size: 1.8rem; margin-bottom: 0.5rem; color: #fff; }
        .subtitle { color: #888; margin-bottom: 2rem; font-size: 0.95rem; }
        .card {
            background: #1a1a1a; border: 1px solid #333; border-radius: 12px;
            padding: 2rem; margin-bottom: 1.5rem;
        }
        .card h2 { font-size: 1.1rem; margin-bottom: 1rem; color: #ddd; }
        .upload-area {
            border: 2px dashed #444; border-radius: 8px; padding: 3rem;
            text-align: center; cursor: pointer; transition: all 0.2s;
        }
        .upload-area:hover { border-color: #666; background: #222; }
        .upload-area.dragover { border-color: #4CAF50; background: #1a2e1a; }
        input[type="file"] { display: none; }
        .btn {
            background: #2563EB; color: #fff; border: none; padding: 0.75rem 2rem;
            border-radius: 8px; font-size: 1rem; cursor: pointer; transition: background 0.2s;
        }
        .btn:hover { background: #1d4ed8; }
        .btn:disabled { background: #333; cursor: not-allowed; color: #666; }
        .criteria {
            display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem;
            font-size: 0.85rem; color: #aaa;
        }
        .criteria .item { padding: 0.4rem 0.6rem; background: #222; border-radius: 4px; }
        .criteria .item.counted { background: #2a2000; border: 1px solid #554400; }
        .progress-container { display: none; margin-top: 1rem; }
        .progress-bar {
            height: 6px; background: #333; border-radius: 3px; overflow: hidden;
        }
        .progress-fill {
            height: 100%; background: #2563EB; width: 0%; transition: width 0.3s;
        }
        .progress-text { font-size: 0.85rem; color: #888; margin-top: 0.5rem; }
        .results { display: none; }
        .stat-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin: 1rem 0; }
        .stat {
            background: #222; border-radius: 8px; padding: 1rem; text-align: center;
        }
        .stat .number { font-size: 1.8rem; font-weight: 700; }
        .stat .label { font-size: 0.8rem; color: #888; margin-top: 0.3rem; }
        .stat.eligible .number { color: #4CAF50; }
        .stat.disqualified .number { color: #f44336; }
        .stat.other .number { color: #FF9800; }
        .log {
            background: #111; border: 1px solid #333; border-radius: 8px;
            padding: 1rem; max-height: 400px; overflow-y: auto;
            font-family: 'SF Mono', Monaco, monospace; font-size: 0.8rem;
            line-height: 1.5; color: #888;
        }
        .log .eligible { color: #4CAF50; }
        .log .disqualified { color: #f44336; }
        .log .warning { color: #FF9800; }
        .log .info { color: #2196F3; }
        .checkbox-group { display: flex; align-items: center; gap: 0.5rem; margin: 1rem 0; }
        .checkbox-group input { width: 18px; height: 18px; }
        .checkbox-group label { font-size: 0.9rem; color: #aaa; }
    </style>
</head>
<body>
<div class="container">
    <h1>DiRōNA Restaurant Qualifier v2.0</h1>
    <p class="subtitle">Upload your CSV → menus are read (HTML + PDF) → disqualification rules applied</p>

    <div class="card">
        <h2>Disqualification Rules</h2>
        <div class="criteria">
            <div class="item">❌ Nachos</div>
            <div class="item">❌ Fish & Chips</div>
            <div class="item">❌ BBQ Ribs</div>
            <div class="item">❌ Buffet / All-You-Can-Eat</div>
            <div class="item">❌ Brazilian Steakhouse</div>
            <div class="item">❌ Hot Pot</div>
            <div class="item">❌ Teppanyaki</div>
            <div class="item counted">⚠️ More than 1 Pizza</div>
            <div class="item counted">⚠️ More than 1 Burger</div>
        </div>
        <p style="margin-top: 0.8rem; font-size: 0.8rem; color: #666;">
            Pre-filter: Google Rating ≥ 4.0 stars, Price Level ≥ $$$ (3+)
        </p>
    </div>

    <div class="card" id="upload-card">
        <h2>Upload Restaurant CSV</h2>
        <form id="uploadForm" enctype="multipart/form-data">
            <div class="upload-area" id="dropZone" onclick="document.getElementById('fileInput').click()">
                <p style="font-size: 1.1rem; margin-bottom: 0.5rem;">Drop CSV here or click to browse</p>
                <p style="font-size: 0.85rem; color: #666;">CSV must include: name, website columns</p>
                <p id="fileName" style="font-size: 0.9rem; color: #4CAF50; margin-top: 0.5rem;"></p>
            </div>
            <input type="file" id="fileInput" name="file" accept=".csv">
            <div class="checkbox-group">
                <input type="checkbox" id="usePlaywright" name="use_playwright" checked>
                <label for="usePlaywright">Use Playwright for JS-rendered sites (slower but more thorough)</label>
            </div>
            <button type="submit" class="btn" id="submitBtn" disabled>Qualify Restaurants</button>
        </form>

        <div class="progress-container" id="progressContainer">
            <div class="progress-bar"><div class="progress-fill" id="progressFill"></div></div>
            <p class="progress-text" id="progressText">Starting...</p>
        </div>
    </div>

    <div class="results" id="resultsSection">
        <div class="card">
            <h2>Results</h2>
            <div class="stat-grid">
                <div class="stat eligible">
                    <div class="number" id="statEligible">0</div>
                    <div class="label">Eligible</div>
                </div>
                <div class="stat disqualified">
                    <div class="number" id="statDisqualified">0</div>
                    <div class="label">Disqualified</div>
                </div>
                <div class="stat other">
                    <div class="number" id="statOther">0</div>
                    <div class="label">Review / Other</div>
                </div>
            </div>
            <div style="display: flex; gap: 1rem; margin-top: 1rem;">
                <button class="btn" id="downloadBtn">Download Results CSV</button>
                <button class="btn" id="clearBtn" style="background: #444;">Clear &amp; Start New</button>
            </div>
        </div>

        <div class="card">
            <h2>Processing Log</h2>
            <div class="log" id="logOutput"></div>
        </div>
    </div>
</div>

<script>
const dropZone = document.getElementById('dropZone');
const fileInput = document.getElementById('fileInput');
const submitBtn = document.getElementById('submitBtn');
const fileName = document.getElementById('fileName');

['dragenter', 'dragover'].forEach(e => {
    dropZone.addEventListener(e, (ev) => { ev.preventDefault(); dropZone.classList.add('dragover'); });
});
['dragleave', 'drop'].forEach(e => {
    dropZone.addEventListener(e, (ev) => { ev.preventDefault(); dropZone.classList.remove('dragover'); });
});
dropZone.addEventListener('drop', (e) => {
    fileInput.files = e.dataTransfer.files;
    fileInput.dispatchEvent(new Event('change'));
});

fileInput.addEventListener('change', () => {
    if (fileInput.files.length) {
        fileName.textContent = fileInput.files[0].name;
        submitBtn.disabled = false;
    }
});

document.getElementById('uploadForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    const formData = new FormData();
    formData.append('file', fileInput.files[0]);
    formData.append('use_playwright', document.getElementById('usePlaywright').checked ? '1' : '0');

    submitBtn.disabled = true;
    document.getElementById('progressContainer').style.display = 'block';
    document.getElementById('resultsSection').style.display = 'none';
    document.getElementById('logOutput').innerHTML = '';

    try {
        const resp = await fetch('/api/qualify', {
            method: 'POST',
            body: formData,
        });

        const reader = resp.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\\n');
            buffer = lines.pop();

            for (const line of lines) {
                if (!line.startsWith('data: ')) continue;
                const data = JSON.parse(line.slice(6));

                if (data.type === 'progress') {
                    document.getElementById('progressFill').style.width = data.percent + '%';
                    document.getElementById('progressText').textContent = data.message;
                    const logDiv = document.getElementById('logOutput');
                    const cls = data.status || 'info';
                    logDiv.innerHTML += `<div class="${cls}">${data.message}</div>`;
                    logDiv.scrollTop = logDiv.scrollHeight;
                }
                else if (data.type === 'complete') {
                    document.getElementById('progressContainer').style.display = 'none';
                    document.getElementById('resultsSection').style.display = 'block';
                    document.getElementById('statEligible').textContent = data.stats.eligible;
                    document.getElementById('statDisqualified').textContent = data.stats.disqualified;
                    document.getElementById('statOther').textContent =
                        data.stats.pre_filtered + data.stats.menu_not_found +
                        data.stats.no_website + data.stats.errors;
                    document.getElementById('downloadBtn').onclick = () => {
                        window.location.href = '/api/download/' + data.filename;
                    };
                }
            }
        }
    } catch (err) {
        alert('Error: ' + err.message);
    } finally {
        submitBtn.disabled = false;
    }
});

document.getElementById('clearBtn').addEventListener('click', () => {
    // Hide results
    document.getElementById('resultsSection').style.display = 'none';
    // Reset progress
    document.getElementById('progressContainer').style.display = 'none';
    document.getElementById('progressFill').style.width = '0%';
    document.getElementById('progressText').textContent = 'Starting...';
    // Clear log
    document.getElementById('logOutput').innerHTML = '';
    // Reset stats
    document.getElementById('statEligible').textContent = '0';
    document.getElementById('statDisqualified').textContent = '0';
    document.getElementById('statOther').textContent = '0';
    // Reset file input
    fileInput.value = '';
    fileName.textContent = '';
    submitBtn.disabled = true;
    // Scroll to top
    window.scrollTo({ top: 0, behavior: 'smooth' });
});
</script>
</body>
</html>
    """

    @app.route("/")
    def index():
        return render_template_string(HTML_TEMPLATE)

    @app.route("/api/qualify", methods=["POST"])
    def api_qualify():
        from flask import Response

        file = request.files.get("file")
        if not file:
            return jsonify({"error": "No file uploaded"}), 400

        use_playwright = request.form.get("use_playwright", "1") == "1"

        # Save uploaded file
        upload_dir = tempfile.mkdtemp()
        input_path = os.path.join(upload_dir, "input.csv")
        file.save(input_path)

        output_filename = f"dirona_qualified_{int(time.time())}.csv"
        output_path = os.path.join(upload_dir, output_filename)

        def generate():
            # Read CSV
            with open(input_path, "r", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                csv_headers = next(reader)
                csv_rows = list(reader)

            # Find columns
            w_col = find_column(csv_headers, ["website", "url", "web", "site", "homepage"])
            n_col = find_column(csv_headers, ["name", "restaurant", "business"])
            r_col = find_column(csv_headers, ["rating", "stars", "google rating"])
            p_col = find_column(csv_headers, ["price", "price level", "price_level", "$"])

            if w_col == -1:
                yield f"data: {json.dumps({'type': 'progress', 'percent': 0, 'message': '❌ No website column found', 'status': 'disqualified'})}\n\n"
                return

            if n_col == -1:
                n_col = 0

            engine = DisqualificationEngine()
            extractor = MenuExtractor(use_playwright=use_playwright)

            out_headers = csv_headers + [
                "qualification_status", "disqualifiers_found",
                "menu_pages_checked", "menu_pdfs_read",
                "menu_errors", "qualification_details",
            ]

            all_results = []
            local_stats = {
                "total": len(csv_rows), "eligible": 0, "disqualified": 0,
                "pre_filtered": 0, "menu_not_found": 0, "no_website": 0, "errors": 0,
            }

            for idx, row in enumerate(csv_rows):
                restaurant = row[n_col] if n_col < len(row) else f"Row {idx + 1}"
                website = row[w_col] if w_col < len(row) else ""
                pct = int(((idx + 1) / len(csv_rows)) * 100)

                # Pre-filter: not a restaurant
                name_lower = restaurant.lower()
                non_rest_match = None
                for pattern in NON_RESTAURANT_COMPILED:
                    m = pattern.search(name_lower)
                    if m:
                        non_rest_match = m.group()
                        break
                if non_rest_match:
                    msg = f"[{idx+1}/{len(csv_rows)}] {restaurant} — ⛔ Not a restaurant: '{non_rest_match}'"
                    yield f"data: {json.dumps({'type': 'progress', 'percent': pct, 'message': msg, 'status': 'warning'})}\n\n"
                    local_stats["pre_filtered"] += 1
                    all_results.append(row + [
                        "not_a_restaurant", f"Business name matched: {non_rest_match}", "", "", "", ""
                    ])
                    continue

                # Pre-filter: rating
                if r_col >= 0 and r_col < len(row):
                    try:
                        rating = float(row[r_col].replace(",", "."))
                        if rating < MIN_RATING:
                            msg = f"[{idx+1}/{len(csv_rows)}] {restaurant} — ⛔ Rating {rating} < {MIN_RATING}"
                            yield f"data: {json.dumps({'type': 'progress', 'percent': pct, 'message': msg, 'status': 'warning'})}\n\n"
                            local_stats["pre_filtered"] += 1
                            all_results.append(row + [
                                "pre_filtered", f"Rating {rating} below {MIN_RATING}", "", "", "", ""
                            ])
                            continue
                    except (ValueError, IndexError):
                        pass

                # Pre-filter: price level
                if p_col >= 0 and p_col < len(row):
                    try:
                        price_str = row[p_col].strip()
                        if price_str.startswith("$"):
                            price_level = len(price_str.replace(" ", ""))
                        else:
                            price_level = int(float(price_str))
                        if price_level < MIN_PRICE_LEVEL:
                            msg = f"[{idx+1}/{len(csv_rows)}] {restaurant} — ⛔ Price {price_level} < {MIN_PRICE_LEVEL}"
                            yield f"data: {json.dumps({'type': 'progress', 'percent': pct, 'message': msg, 'status': 'warning'})}\n\n"
                            local_stats["pre_filtered"] += 1
                            all_results.append(row + [
                                "pre_filtered", f"Price level {price_level} below {MIN_PRICE_LEVEL}", "", "", "", ""
                            ])
                            continue
                    except (ValueError, IndexError):
                        pass

                if not website or not website.strip():
                    msg = f"[{idx+1}/{len(csv_rows)}] {restaurant} — ⚠️ No website"
                    yield f"data: {json.dumps({'type': 'progress', 'percent': pct, 'message': msg, 'status': 'warning'})}\n\n"
                    local_stats["no_website"] += 1
                    all_results.append(row + [
                        "no_website", "", "", "", "No website URL", ""
                    ])
                    continue

                msg = f"[{idx+1}/{len(csv_rows)}] {restaurant} — Checking menu..."
                yield f"data: {json.dumps({'type': 'progress', 'percent': pct, 'message': msg, 'status': 'info'})}\n\n"

                try:
                    extraction = extractor.extract_menu(website)
                except Exception as e:
                    msg = f"[{idx+1}/{len(csv_rows)}] {restaurant} — 💥 Error: {str(e)[:100]}"
                    yield f"data: {json.dumps({'type': 'progress', 'percent': pct, 'message': msg, 'status': 'disqualified'})}\n\n"
                    local_stats["errors"] += 1
                    all_results.append(row + [
                        "error", "", "", "", str(e)[:200], ""
                    ])
                    continue

                if not extraction["success"]:
                    msg = f"[{idx+1}/{len(csv_rows)}] {restaurant} — ⚠️ Menu not found"
                    yield f"data: {json.dumps({'type': 'progress', 'percent': pct, 'message': msg, 'status': 'warning'})}\n\n"
                    local_stats["menu_not_found"] += 1
                    err_str = "; ".join(extraction["errors"][:3]) or "No menu text found"
                    all_results.append(row + [
                        "menu_not_found", "", str(extraction["pages_checked"]),
                        str(extraction["pdfs_read"]), err_str, ""
                    ])
                    continue

                result = engine.check_menu(extraction["menu_text"])

                if result["qualified"]:
                    status = "eligible"
                    local_stats["eligible"] += 1
                    msg = f"[{idx+1}/{len(csv_rows)}] {restaurant} — ✅ ELIGIBLE"
                    cls = "eligible"
                else:
                    status = "disqualified"
                    local_stats["disqualified"] += 1
                    disq = ", ".join(result["disqualifiers"])
                    msg = f"[{idx+1}/{len(csv_rows)}] {restaurant} — ❌ {disq}"
                    cls = "disqualified"

                yield f"data: {json.dumps({'type': 'progress', 'percent': pct, 'message': msg, 'status': cls})}\n\n"

                all_results.append(row + [
                    status,
                    "; ".join(result["disqualifiers"]),
                    str(extraction["pages_checked"]),
                    str(extraction["pdfs_read"]),
                    "; ".join(extraction["errors"][:3]),
                    " | ".join(result["details"][:5]),
                ])

                time.sleep(0.3)

            # Write output
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(out_headers)
                writer.writerows(all_results)

            extractor.close()

            yield f"data: {json.dumps({'type': 'complete', 'stats': local_stats, 'filename': output_filename})}\n\n"

        return Response(generate(), mimetype="text/event-stream")

    @app.route("/api/download/<filename>")
    def download(filename):
        # Search for the file in temp directories
        for tmp_dir in Path(tempfile.gettempdir()).iterdir():
            if tmp_dir.is_dir():
                fpath = tmp_dir / filename
                if fpath.exists():
                    return send_file(str(fpath), as_attachment=True, download_name=filename)
        return "File not found", 404

    print(f"\n🚀 DiRōNA Qualifier v2.0 running at http://localhost:{port}\n")
    app.run(host="0.0.0.0", port=port, debug=False)


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DiRōNA Restaurant Qualifier v2.0")
    parser.add_argument("--input", "-i", help="Input CSV file")
    parser.add_argument("--output", "-o", help="Output CSV file", default="dirona_qualified.csv")
    parser.add_argument("--web", action="store_true", help="Run as web application")
    parser.add_argument("--port", type=int, default=5050, help="Web app port (default: 5050)")
    parser.add_argument("--no-playwright", action="store_true", help="Disable Playwright (requests only)")

    args = parser.parse_args()

    if args.web:
        run_webapp(port=args.port)
    elif args.input:
        process_csv(args.input, args.output, use_playwright=not args.no_playwright)
    else:
        parser.print_help()
        print("\nExamples:")
        print("  python3 qualifier.py --web                          # Web app mode")
        print("  python3 qualifier.py -i leads.csv -o results.csv    # CLI mode")
        print("  python3 qualifier.py -i leads.csv --no-playwright   # Without Playwright")
