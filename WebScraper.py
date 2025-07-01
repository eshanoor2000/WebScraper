import os
import json
import time
import sys
import requests
import pymongo
from pymongo.errors import BulkWriteError
import logging
from datetime import datetime, date, timedelta, timezone
import re
from calendar import month_name
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from rapidfuzz import fuzz
import scrapy
import feedparser
from bs4 import BeautifulSoup
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from scrapy_splash import SplashRequest
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from dotenv import load_dotenv
import spacy
import dateutil.parser
import random
from urllib.parse import urlparse
load_dotenv()

# --- Configuration from environment variables ---
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "brand_monitoring")
RAW_COLLECTION = os.getenv("RAW_COLLECTION", "raw_articles")
PROCESSED_COLLECTION = os.getenv("PROCESSED_COLLECTION", "processed_articles")
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT")
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SPLASH_URL = os.getenv("SPLASH_URL")

# User agents for rotation to avoid blocking
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
]

# Logging Configuration
def configure_logging():
    """Configure logging settings for the application.
    
    Sets up logging with a standard format and output to stdout.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

# Initialize logging
configure_logging()

SCRAPED_COUNT = {
    "bing": 0,
    "rss": 0
}

ONTARIO_TERMS = {
    "ontario", 
    "toronto",
    "canada",
    "ottawa",
    "mississauga",
    "brampton",
    "hamilton",
    "london",
    "markham",
    "vaughan",
    "kitchener",
    "windsor",
    "richmond hill",
    "oakville",
    "burlington",
    "oshawa",
    "barrie",
    "st. catharines",
    "cambridge",
    "kingston",
    "whitby",
    "guelph",
    "thunder Bay",
    "waterloo",
    "brantford",
    "niagara Falls"
}

STANDARD_KEYWORDS = [
    "condominium authority of ontario",
    "condo authority",
    "condo authority of ontario",
    "ontario condominium act",
    "condominium act",
    "Condominium Authority of Ontario",
    "Condo Authority of Ontario",
    "CAO Ontario",
    "Condominium Authority",
    "Condo Authority",
    "Condo Tribunal Ontario",
    "Condominium Tribunal Ontario",
    "Condo Authority Tribunal",
    "Condominium Authority Tribunal",
    "CAO Tribunal",
    "ONCAT",
    "Ontario Condominium Authority Tribunal",
    "oncat",
    "Condominium Act Ontario",
    "Ontario Condominium Act",
    "Ontario condo laws",
    "Condo governance Ontario",
    "Ontario condo regulations",
    "Condo regulatory compliance Ontario",
    "Condo law reform Ontario",
    "Condo bylaws Ontario",
    "Condo policy changes Ontario",
    "Condo tribunal cases Ontario",
    "Condo tribunal decisions Ontario",
    "Condo tribunal rulings Ontario",
    "CAO tribunal rulings",
    "Condominium tribunal hearings",
    "Condo tribunal evidence submission",
    "Condo tribunal appeals Ontario",
    "Ontario condo tribunal process",
    "Condo tribunal enforcement Ontario",
    "Condo tribunal complaint process",
    "Condo legal disputes Ontario",
    "Condo legal challenges Ontario",
    "Condo tribunal case law Ontario",
    "Condo board disputes Ontario",
    "Condo board complaints Ontario",
    "Condo board corruption Ontario",
    "Ontario condo board governance",
    "Condo board misconduct Ontario",
    "Condo board election fraud Ontario",
    "Condo board mismanagement Ontario",
    "Condo board conflicts Ontario",
    "Condo board transparency issues Ontario",
    "Condo board legal responsibilities Ontario",
    "Condo board regulations Ontario",
    "Condo owner rights Ontario",
    "Condo owner disputes Ontario",
    "Condo owners legal issues Ontario",
    "Condo dispute resolution Ontario",
    "Condo resident rights Ontario",
    "Condo complaint process Ontario",
    "How to file condo complaint Ontario",
    "Condo tenant rights Ontario",
    "Condo transparency issues Ontario",
    "Ontario condo act violations",
    "Condo maintenance fees Ontario",
    "Condo fee increases Ontario",
    "Unfair condo fees Ontario",
    "Condo reserve fund Ontario",
    "Condo financial mismanagement Ontario",
    "Condo special assessments Ontario",
    "Condo financial fraud Ontario",
    "Condo board financial transparency Ontario",
    "Condo developer fraud Ontario",
    "Condo management fraud Ontario",
    "Condo property management Ontario",
    "Condo management disputes Ontario",
    "Condo management complaints Ontario",
    "Condo management corruption Ontario",
    "Ontario condo rental rules",
    "Ontario condo tenant disputes",
    "Condo property maintenance Ontario",
    "Condo security fraud Ontario",
    "Condo contract violations Ontario",
    "CAO vs Landlord and Tenant Board",
    "FSRA condo insurance Ontario",
    "Ontario Securities Commission condo fraud",
    "Ontario Human Rights Tribunal condo cases",
    "Ontario Building Code condo regulations",
    "Ontario Ministry of Municipal Affairs and Housing condo rules",
    "Ontario condo tribunal vs landlord tenant board"
]

EXCLUDED_DOMAINS = set([
    "zillow.com", "reco.on.ca", "wikipedia.org", "linkedin.com", "canada.ca", "ontario.ca",
    "condoauthorityontario.ca", "toronto.ca", "realtor.ca", "condos.ca", "property.ca", "agents.property.ca",
    "zolo.ca", "realtor.com", "investopedia.com", "expedia.com", "homes.com", "glassdoor.ca", "reddit.com",
    "apartments.com", "strata.ca", "rentcafe.com", "rentals.ca", "hotpads.com", "properties.lefigaro.com",
    "kijiji.ca", "apartmenthomeliving.com", "bloomberg.com", "apartmentlist.com", "nytimes.com", "justanswer.com",
    "highrises.com", "coastalheatpumps.com", "airtekshop.com", "a-plusquality.com", "quora.com", "youtube.com",
    "instagram.com", "imdb.com", "facebook.com", "wsj.com", "yelp.ca", "pinterest.com", "flickr.com", "ca.linkedin.com",
    "twitter.com", "tiktok.com", "airbnb.com", "movoto.com", "x.com", "rent.com", "movemeto.com", "ziprecruiter.com",
    "vimeo.com", "amazon.com", "rew.ca", "crunchbase.com", "ca.hotels.com", "hotels.com", "boston.com", "bostonglobe.com",
    "ctvnews.ca", "brampton.ca", "pas.gov.on.ca", "reca.ca", "trreb.ca", "mississauga.ca", "london.ca", "bostonagentmagazine.com",
    "oshawa.ca", "milton.ca", "tocondonews.com", "canlii.org"
])

RSS_STANDARD_FEEDS = [
    "https://www.ontariocanada.com/registry/rss?feed=news&lang=en",
    "https://globalnews.ca/ottawa/feed/",
    "https://globalnews.ca/london/feed/",
    "https://globalnews.ca/toronto/feed/",
    "https://globalnews.ca/guelph/feed/",
    "https://globalnews.ca/feed/",
    "https://www.cbc.ca/webfeed/rss/rss-canada-toronto",
    "https://www.cbc.ca/webfeed/rss/rss-canada-hamiltonnews",
    "https://www.cbc.ca/webfeed/rss/rss-canada-ottawa",
    "https://www.cbc.ca/webfeed/rss/rss-canada-kitchenerwaterloo",
    "https://www.cbc.ca/webfeed/rss/rss-canada-london",
    "https://www.cbc.ca/webfeed/rss/rss-canada-windsor",
    "https://www.cbc.ca/webfeed/rss/rss-canada-sudbury",
    "https://www.cbc.ca/webfeed/rss/rss-canada-thunderbay",
    "https://www.cbc.ca/webfeed/rss/rss-canada",
]

RSS_SITES_REQUIRING_SCRAPY = [
    "https://www.connectcre.ca/rss",
    "https://www.reminetwork.com/feed/"
]

ALL_RSS_FEEDS = RSS_STANDARD_FEEDS + RSS_SITES_REQUIRING_SCRAPY

nlp = spacy.load("en_core_web_sm")

def normalize_datetime(dt):
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def get_matched_keywords(text, keywords, max_tags=10):
    text_lower = (text or "").lower()
    matched = [kw for kw in keywords if kw.lower() in text_lower]
    
    # Debug logging to see what's happening
    if not matched and text:
        logging.debug(f"No keyword matches found in text: '{text[:200]}...' (length: {len(text)})")
        # Check for any partial matches
        partial_matches = []
        for kw in keywords[:5]:  # Check first 5 keywords for debugging
            if any(word in text_lower for word in kw.lower().split()):
                partial_matches.append(f"'{kw}' (partial)")
        if partial_matches:
            logging.debug(f"Partial matches found: {partial_matches}")
    
    return matched[:max_tags]

def get_collection(collection_name):
    if not hasattr(get_collection, "client"):
        get_collection.client = pymongo.MongoClient(MONGO_URI)
    return get_collection.client[MONGO_DB][collection_name]

def validate_db_connection():
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        logging.info("MongoDB connection verified.")
    except Exception as e:
        logging.error(f"MongoDB connection failed: {e}")
        sys.exit(1)

def send_scraper_email(success, scraped_count):
    subject = "Web Scraper Completed" if success else "❌ Web Scraper Failed"
    
    body = f"""
    Web scraping job completed.

    Bing: {scraped_count.get("bing", 0)} articles
    RSS: {scraped_count.get("rss", 0)} articles

    {"All spiders ran successfully." if success else "Some errors occurred during the run."}
    """

    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_SENDER
        msg["To"] = EMAIL_RECEIVER
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
        
        logging.info("Summary email sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send summary email: {e}")

def is_within_scrape_window(timestamp_str):
    try:
        timestamp = datetime.fromisoformat(timestamp_str)

        today = datetime.utcnow()
        first_of_this_month = datetime(today.year, today.month, 1)

        # Handle previous month start
        if today.month == 1:
            first_of_last_month = datetime(today.year - 1, 12, 1)
        else:
            first_of_last_month = datetime(today.year, today.month - 1, 1)

        return first_of_last_month <= timestamp < first_of_this_month
    except ValueError:
        return False

def is_relevant_location(text):
    text_lower = text.lower()
    return any(term.lower() in text_lower for term in ONTARIO_TERMS)

def get_valid_date(date_str):
    if not date_str:
        return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    
    date_formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%d %b %Y %H:%M:%S %Z",
        "%B %d, %Y",
        "%b %d, %Y",
    ]
    
    for fmt in date_formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except ValueError:
            continue
    
    try:
        if date_str.isdigit():
            dt = datetime.utcfromtimestamp(int(date_str)).replace(tzinfo=timezone.utc)
            return dt.isoformat()
    except:
        pass
    
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

def save_scraped_data(source, data):
    if not data:
        logging.info(f"No {source} articles to save.")
        return

    collection = get_collection(RAW_COLLECTION)
    
    for item in data:
        item.update({
            "source": source,
            "scraped_date": datetime.utcnow().isoformat(),
            "processing_status": "pending",
            "processed_at": None
        })
    
    try:
        result = collection.insert_many(data, ordered=False)
        SCRAPED_COUNT[source] += len(result.inserted_ids)
        logging.info(f"Saved {len(result.inserted_ids)} {source} articles to {RAW_COLLECTION}")
    except BulkWriteError as e:
        inserted = len(e.details['nInserted'])
        SCRAPED_COUNT[source] += inserted
        logging.info(f"Saved {inserted} {source} articles to {RAW_COLLECTION} (some duplicates skipped)")

def requires_splash(url):
    """Check if a URL requires Splash rendering.
    
    Args:
        url (str): The URL to check
        
    Returns:
        bool: True if the URL requires Splash rendering, False otherwise
    """
    try:
        response = requests.get(url, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
        if response.status_code != 200:
            return True

        soup = BeautifulSoup(response.text, "html.parser")
        text_content = soup.get_text(strip=True)

        if len(text_content) < 100 and len(soup.find_all("script")) > 5:
            return True

        return False
    except Exception:
        return True

def safe_get_published_date(parsed_date):
    try:
        if parsed_date:
            if isinstance(parsed_date, str):
                try:
                    dt = datetime.fromisoformat(parsed_date)
                    dt = normalize_datetime(dt)
                    return dt.isoformat()
                except Exception:
                    return parsed_date  # Already ISO or fallback
            elif isinstance(parsed_date, datetime):
                dt = normalize_datetime(parsed_date)
                return dt.isoformat()
        return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    except Exception:
        return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

def is_within_date_range(published_date_str):
    """Check if published date is within acceptable range (not older than 30 days from run date)."""
    try:
        # Parse the published date
        pub_dt = datetime.fromisoformat(published_date_str)
        pub_dt = normalize_datetime(pub_dt)
        
        # Get current run date (today)
        run_date = datetime.utcnow().replace(tzinfo=timezone.utc)
        
        # Calculate 30 days ago from run date
        thirty_days_ago = run_date - timedelta(days=30)
        
        # Check if published date is within the last 30 days
        return thirty_days_ago <= pub_dt <= run_date
    except Exception as e:
        logging.debug(f"Error checking date range for {published_date_str}: {e}")
        return False

META_DATE_PRIORITY = [
    'article:published_time', 'datePublished', 'pubdate', 'publishdate', 'date', 'og:published_time'
]

def extract_published_date_from_html(card):
    # 1. Try prioritized meta tags
    for meta_name in META_DATE_PRIORITY:
        meta = card.find('meta', attrs={'property': meta_name}) or card.find('meta', attrs={'name': meta_name})
        if meta and meta.get('content'):
            try:
                dt = dateutil.parser.parse(meta['content'], fuzzy=True)
                dt = normalize_datetime(dt)
                return dt.isoformat()
            except Exception:
                continue
    # 2. Try <time> elements
    for time_elem in card.find_all('time'):
        if time_elem.get('datetime'):
            try:
                dt = dateutil.parser.parse(time_elem['datetime'], fuzzy=True)
                dt = normalize_datetime(dt)
                return dt.isoformat()
            except Exception:
                continue
        if time_elem.text:
            try:
                dt = dateutil.parser.parse(time_elem.text, fuzzy=True)
                dt = normalize_datetime(dt)
                return dt.isoformat()
            except Exception:
                continue
    # 3. Try canonical/original link if present
    canonical = card.find('link', rel='canonical')
    if canonical and canonical.get('href'):
        try:
            resp = requests.get(canonical['href'], timeout=5)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                for meta_name in META_DATE_PRIORITY:
                    meta = soup.find('meta', attrs={'property': meta_name}) or soup.find('meta', attrs={'name': meta_name})
                    if meta and meta.get('content'):
                        try:
                            dt = dateutil.parser.parse(meta['content'], fuzzy=True)
                            dt = normalize_datetime(dt)
                            return dt.isoformat()
                        except Exception:
                            continue
        except Exception as e:
            logging.warning(f"Failed to fetch canonical/original link for date extraction: {e}")
    # 4. No valid date found
    return None

def extract_rss_published_date(entry):
    """Extract published date from RSS entry with robust parsing."""
    
    # 1. Try feedparser's parsed date fields first (these are already parsed)
    date_parsed_fields = ['published_parsed', 'updated_parsed', 'created_parsed']
    for field in date_parsed_fields:
        if hasattr(entry, field) and getattr(entry, field):
            try:
                time_struct = getattr(entry, field)
                if time_struct and len(time_struct) >= 6:
                    dt = datetime(*time_struct[:6], tzinfo=timezone.utc)
                    return dt.isoformat()
            except Exception as e:
                logging.debug(f"Failed to parse {field}: {e}")
                continue
    
    # 2. Try string date fields with multiple approaches
    date_fields = ['published', 'pubDate', 'updated', 'created', 'date', 'dc:date']
    for field in date_fields:
        date_str = entry.get(field)
        if date_str:
            try:
                # First try direct parsing
                dt = dateutil.parser.parse(date_str, fuzzy=True)
                dt = dt.astimezone(timezone.utc)
                return dt.isoformat()
            except Exception:
                try:
                    # Clean up common issues and try again
                    cleaned = date_str.replace('GMT', '').replace('UTC', '').replace(',', '').strip()
                    # Remove timezone abbreviations that confuse parser
                    cleaned = re.sub(r'\s+[A-Z]{3,4}$', '', cleaned)
                    dt = dateutil.parser.parse(cleaned, fuzzy=True)
                    dt = dt.astimezone(timezone.utc)
                    return dt.isoformat()
                except Exception:
                    continue
    
    # 3. Check if entry has any attribute containing 'date' or 'time'
    for attr_name in dir(entry):
        if ('date' in attr_name.lower() or 'time' in attr_name.lower()) and not attr_name.startswith('_'):
            try:
                attr_value = getattr(entry, attr_name)
                if isinstance(attr_value, str) and attr_value:
                    dt = dateutil.parser.parse(attr_value, fuzzy=True)
                    dt = dt.astimezone(timezone.utc)
                    return dt.isoformat()
            except Exception:
                continue
    
    # 4. Fallback: scan all string fields for date-like values
    for key, value in entry.items():
        if isinstance(value, str) and value and any(char.isdigit() for char in value):
            # Skip if it's clearly not a date (too short, no date patterns)
            if len(value) < 8:
                continue
            # Look for date patterns
            if re.search(r'\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{4}|\w{3}\s+\d{1,2}[,\s]+\d{4}', value):
                try:
                    cleaned = value.replace('GMT', '').replace('UTC', '').replace(',', '').strip()
                    dt = dateutil.parser.parse(cleaned, fuzzy=True)
                    dt = dt.astimezone(timezone.utc)
                    return dt.isoformat()
                except Exception:
                    continue
    
    logging.debug(f"RSS date parsing failed for entry: {entry.get('title', 'Unknown')} - Available fields: {list(entry.keys())}")
    return None

class BingNewsSpider(scrapy.Spider):
    """Spider for scraping news from Bing search results."""
    name = "bing_news_spider"
    custom_settings = {
        "DOWNLOAD_DELAY": 2,
        "CONCURRENT_REQUESTS": 2,
        "CONCURRENT_ITEMS": 1,
        "ITEM_PIPELINES": {},
        "LOG_LEVEL": "ERROR",
        "HTTPCACHE_ENABLED": False
    }

    def start_requests(self):
        # Run for all dates from March 6, 2025 up to today
        # (No date restriction)
        for keyword in STANDARD_KEYWORDS:
            query = keyword.replace(" ", "+")
            url = f"https://www.bing.com/news/search?q={query}&setlang=en"
            yield SplashRequest(
                url=url,
                callback=self.parse_results,
                args={"wait": 2},
                meta={"query": keyword}
            )

    def parse_results(self, response):
        """Parse Bing news search results.
        
        Args:
            response: The Scrapy response object
            
        Extracts article information from Bing news cards and saves relevant articles.
        """
        soup = BeautifulSoup(response.text, "html.parser")
        news_cards = soup.select("div.news-card")
        
        articles = []
        for card in news_cards:
            try:
                title_elem = card.select_one("a.title")
                link = title_elem.get("href") if title_elem else None
                title = title_elem.get_text(strip=True) if title_elem else None
                if not link or not title:
                    continue
                
                # Check domain exclusions first
                domain = self.extract_domain(link)
                if any(ex in domain for ex in EXCLUDED_DOMAINS):
                    logging.debug(f"Skipping excluded domain: {domain}")
                    continue

                if not is_relevant_location(f"{title} {link}"):
                    continue

                matched_keywords = get_matched_keywords(f"{title} {link}", STANDARD_KEYWORDS)
                if not matched_keywords:
                    logging.info(f"Discarding article (no tags): {title} {link}")
                    continue

                # Robust date extraction with multiple strategies
                published_date = None
                
                # Strategy 1: Try to extract from URL first (fastest)
                published_date = extract_date_from_url(link)
                
                # Strategy 2: If URL extraction fails, try scraping the article page
                if not published_date:
                    response_obj = robust_fetch_article_page(link)
                    if response_obj:
                        try:
                            article_soup = BeautifulSoup(response_obj.text, 'html.parser')
                            published_date = robust_extract_published_date(link, article_soup)
                        except Exception as e:
                            logging.warning(f"Error parsing article page {link}: {e}")
                
                # Strategy 3: If still no date, try extracting from Bing card itself
                if not published_date:
                    published_date = extract_published_date_from_html(card)
                
                if not published_date:
                    logging.info(f"Discarding article (no valid published date): {title} {link}")
                    continue

                published_date = safe_get_published_date(published_date)
                
                # Check if published date is within 30 days of run date
                if not is_within_date_range(published_date):
                    logging.debug(f"Discarding article (published more than 30 days ago): {title}")
                    continue
                
                if title and link and published_date:
                    articles.append({
                        "title": title,
                        "link": link,
                        "published_date": published_date,
                        "scraped_date": datetime.utcnow().isoformat(),
                        "tags": matched_keywords if matched_keywords is not None else [],
                        "source": "Other",
                        "subreddit": None,
                        "upvotes": None,
                        "comments": None,
                        "content": None
                    })
                
            except Exception as e:
                logging.error(f"Error parsing Bing news card: {e}")
                continue

        if not articles:
            logging.info(f"No relevant {self.name} articles found for this run.")

        save_scraped_data("bing", articles)

    def extract_domain(self, url):
        """Extract domain from a URL.
        
        Args:
            url (str): The URL to extract domain from
            
        Returns:
            str: The extracted domain or empty string if extraction fails
        """
        try:
            return url.split("//")[1].split("/")[0].replace("www.", "")
        except:
            return ""

class RSSFeedSpider(scrapy.Spider):
    """Spider for scraping RSS feeds."""
    name = "rss_feed_spider"
    custom_settings = {
        "DOWNLOAD_DELAY": 2,
        "CONCURRENT_REQUESTS": 2,
        "CONCURRENT_ITEMS": 1,
        "ITEM_PIPELINES": {},
        "LOG_LEVEL": "ERROR",
        "HTTPCACHE_ENABLED": False
    }
    
    def start_requests(self):
        # Run for all dates from March 6, 2025 up to today
        # (No date restriction)
        for url in ALL_RSS_FEEDS:
            if requires_splash(url):
                yield SplashRequest(
                    url,
                    self.parse_rss_splash,
                    args={"wait": 3, "html": 1, "png": 0},
                )
            else:
                yield scrapy.Request(url, callback=self.parse_rss)

    def parse_rss(self, response):
        """Parse standard RSS feeds with robust error handling"""
        try:
            # Use robust fetching for better reliability
            headers = {
                'User-Agent': random.choice(USER_AGENTS),
                'Accept': 'application/rss+xml, application/xml, text/xml',
            }
            
            feed = feedparser.parse(response.body)
            if feed.bozo:
                logging.warning(f"Skipping malformed RSS feed: {feed.bozo_exception}")
                return

            articles = []
            logging.info(f"Processing {len(feed.entries)} RSS entries")
            
            for entry in feed.entries:
                try:
                    title = getattr(entry, 'title', '').strip()
                    link = getattr(entry, 'link', '').strip()
                    
                    if not title or not link:
                        logging.debug(f"Skipping incomplete RSS entry: title={bool(title)}, link={bool(link)}")
                        continue

                    # Check domain exclusions
                    domain = self.extract_domain(link)
                    if any(ex in domain for ex in EXCLUDED_DOMAINS):
                        logging.debug(f"Skipping excluded domain: {domain}")
                        continue

                    if not is_relevant_location(f"{title} {link}"):
                        continue

                    # Enhanced date extraction with multiple fallbacks
                    published_date = extract_rss_published_date(entry)
                    
                    # If standard RSS date extraction fails, try URL extraction
                    if not published_date:
                        published_date = extract_date_from_url(link)
                    
                    # If still no date, try scraping the article page as last resort
                    if not published_date:
                        try:
                            response_obj = robust_fetch_article_page(link, max_retries=1, timeout=5)
                            if response_obj:
                                article_soup = BeautifulSoup(response_obj.text, 'html.parser')
                                published_date = robust_extract_published_date(link, article_soup)
                        except Exception as e:
                            logging.debug(f"Failed to scrape article page for date: {e}")
                    
                    if not published_date:
                        logging.debug(f"Discarding RSS article (no valid date): {title}")
                        continue
                        
                    # Restrict to articles published between March 6, 2025 and today
                    start_date = datetime(2025, 3, 6, tzinfo=timezone.utc)
                    end_date = datetime.utcnow().replace(tzinfo=timezone.utc)
                    try:
                        pub_dt = datetime.fromisoformat(published_date)
                        pub_dt = normalize_datetime(pub_dt)
                    except Exception:
                        continue
                    if not (start_date <= pub_dt <= end_date):
                        continue
                        
                    # Enhanced keyword matching - check all available text
                    summary = getattr(entry, 'summary', '') or getattr(entry, 'description', '')
                    content = ""
                    if hasattr(entry, 'content'):
                        content_list = getattr(entry, 'content', [])
                        if isinstance(content_list, list) and content_list:
                            content = content_list[0].get('value', '') if isinstance(content_list[0], dict) else str(content_list[0])
                        elif isinstance(content_list, str):
                            content = content_list
                    
                    text_for_tags = f"{title} {summary} {content}".strip()
                    matched_keywords = get_matched_keywords(text_for_tags, STANDARD_KEYWORDS)
                    
                    if not matched_keywords:
                        logging.debug(f"Discarding RSS article (no tags): {title}")
                        continue
                    
                    published_date = safe_get_published_date(published_date)
                    if not published_date:
                        continue
                    
                    # Check if published date is within 30 days of run date
                    if not is_within_date_range(published_date):
                        logging.debug(f"Discarding RSS article (published more than 30 days ago): {title}")
                        continue
                        
                    articles.append({
                        "title": title,
                        "link": link,
                        "published_date": published_date,
                        "scraped_date": datetime.utcnow().isoformat(),
                        "tags": matched_keywords,
                        "source": "Other",  # Changed from "rss" to "Other" as per requirements
                        "subreddit": None,
                        "upvotes": None,
                        "comments": None,
                        "content": summary or None
                    })
                    
                except Exception as e:
                    logging.error(f"Error processing RSS entry: {e}")
                    continue
                    
            if articles:
                logging.info(f"Found {len(articles)} relevant RSS articles")
                save_scraped_data("rss", articles)
            else:
                logging.info("No relevant RSS articles found for this run.")
                
        except Exception as e:
            logging.error(f"Error parsing RSS feed: {e}")

    def parse_rss_splash(self, response):
        """Parse RSS feeds that require Splash rendering."""
        try:
            soup = BeautifulSoup(response.text, "html.parser")
            items = soup.find_all("item") or soup.find_all("entry")
            articles = []
            for item in items:
                title = item.find("title").text if item.find("title") else ""
                link = item.find("link").get("href", "") if hasattr(item.find("link"), "get") else item.find("link").text if item.find("link") else ""
                if not title or not link:
                    continue
                
                # More robust date extraction for Splash-rendered feeds
                pub_date = None
                
                # Try multiple date tag variations with case-insensitive search
                date_tags = ['pubdate', 'published', 'date', 'updated', 'created', 'dc:date']
                for tag_name in date_tags:
                    # Try both lowercase and original case
                    tag = item.find(tag_name.lower()) or item.find(tag_name)
                    if tag and tag.text:
                        try:
                            # Clean the date text
                            date_text = tag.text.strip()
                            # Remove common problematic parts
                            cleaned = date_text.replace('GMT', '').replace('UTC', '').replace(',', '').strip()
                            cleaned = re.sub(r'\s+[A-Z]{3,4}$', '', cleaned)
                            
                            dt = dateutil.parser.parse(cleaned, fuzzy=True)
                            dt = normalize_datetime(dt)
                            pub_date = dt.isoformat()
                            break
                        except Exception as e:
                            logging.debug(f"Failed to parse date from {tag_name}: {date_text} - {e}")
                            continue
                
                # If no date found in standard tags, try to find any tag with date-like content
                if not pub_date:
                    for tag in item.find_all():
                        if tag.text and len(tag.text) > 8:
                            # Look for date patterns in tag text
                            if re.search(r'\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{4}|\w{3}\s+\d{1,2}[,\s]+\d{4}', tag.text):
                                try:
                                    cleaned = tag.text.replace('GMT', '').replace('UTC', '').replace(',', '').strip()
                                    dt = dateutil.parser.parse(cleaned, fuzzy=True)
                                    dt = normalize_datetime(dt)
                                    pub_date = dt.isoformat()
                                    break
                                except Exception:
                                    continue
                
                if not pub_date:
                    logging.debug(f"No valid date found for RSS article: {title}")
                    continue  # Discard if no valid date
                
                published_date = pub_date
                # Restrict to articles published between March 6, 2025 and today
                start_date = datetime(2025, 3, 6, tzinfo=timezone.utc)
                end_date = datetime.utcnow().replace(tzinfo=timezone.utc)
                try:
                    pub_dt = datetime.fromisoformat(published_date)
                    pub_dt = normalize_datetime(pub_dt)
                except Exception:
                    continue
                if not (start_date <= pub_dt <= end_date):
                    continue
                
                # Try to get summary/content from multiple possible tags
                summary = ""
                content = ""
                
                # Try description, summary, content tags
                for desc_tag in ['description', 'summary', 'content']:
                    desc_elem = item.find(desc_tag)
                    if desc_elem and desc_elem.text:
                        summary = desc_elem.text
                        break
                
                # Try content tag variations
                for content_tag in ['content', 'content:encoded', 'encoded']:
                    content_elem = item.find(content_tag)
                    if content_elem and content_elem.text:
                        content = content_elem.text
                        break
                
                text_for_tags = f"{title} {summary} {content}"
                matched_keywords = get_matched_keywords(text_for_tags, STANDARD_KEYWORDS)
                if not matched_keywords:
                    logging.info(f"Discarding RSS article (no tags): {title} {link}")
                    continue
                    
                published_date = safe_get_published_date(published_date)
                
                # Check if published date is within 30 days of run date
                if not is_within_date_range(published_date):
                    logging.debug(f"Discarding RSS Splash article (published more than 30 days ago): {title}")
                    continue
                
                if title and link and published_date:
                    articles.append({
                        "title": title,
                        "link": link,
                        "published_date": published_date,
                        "scraped_date": datetime.utcnow().isoformat(),
                        "tags": matched_keywords if matched_keywords is not None else [],
                        "source": "Other",  # Changed from "rss" to "Other"
                        "subreddit": None,
                        "upvotes": None,
                        "comments": None,
                        "content": summary or None
                    })
            
            if not articles:
                logging.info(f"No relevant {self.name} articles found for this run.")
            save_scraped_data("rss", articles)
        except Exception as e:
            logging.error(f"Error parsing Splash-rendered RSS feed: {e}")

    def extract_domain(self, url):
        """Extract domain from URL for exclusion checking."""
        try:
            parsed = urlparse(url)
            return parsed.netloc.lower()
        except:
            return ""

def run_scrapy_spiders():
    """Run Scrapy spiders with Splash configuration.
    
    Returns:
        bool: True if all spiders completed successfully, False otherwise
    """
    try:
        settings = get_project_settings()
        settings.set("SPLASH_URL", SPLASH_URL)

        # Splash middleware setup
        settings.set("DOWNLOADER_MIDDLEWARES", {
            'scrapy_splash.SplashCookiesMiddleware': 723,
            'scrapy_splash.SplashMiddleware': 725,
            'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
        })
        settings.set("SPIDER_MIDDLEWARES", {
            'scrapy_splash.SplashDeduplicateArgsMiddleware': 100,
        })
        settings.set("DUPEFILTER_CLASS", "scrapy_splash.SplashAwareDupeFilter")
        settings.set("HTTPCACHE_STORAGE", "scrapy_splash.SplashAwareFSCacheStorage")

        logging.info(f"Using Splash server: {SPLASH_URL}")

        process = CrawlerProcess(settings)
        process.crawl(BingNewsSpider)
        process.crawl(RSSFeedSpider)
        process.start(stop_after_crawl=True)
        logging.info("All Scrapy spiders completed successfully.")
        return True
    except Exception as e:
        logging.error(f"Scrapy spiders failed with error: {e}")
        return False

def run_web_scraping():
    """Run web scraping pipeline.
    
    Executes the web scraping process, validates database connection,
    and sends summary email.
    
    Returns:
        bool: True if scraping completed successfully, False otherwise
    """
    try:
        logging.info("Starting scheduled web scraping job")
        validate_db_connection()
        success = run_scrapy_spiders()
        
        if success:
            logging.info("Web scraping completed successfully")
        else:
            logging.error("Web scraping completed with errors")

        send_scraper_email(success, SCRAPED_COUNT)

        return success
    except Exception as e:
        logging.error(f"Fatal error in web scrapers: {e}")
        send_scraper_email(False, SCRAPED_COUNT)  # Still send email if crash
        return False

def robust_fetch_article_page(url, max_retries=3, timeout=10):
    """Robustly fetch article page with retries and error handling."""
    for attempt in range(max_retries):
        try:
            headers = {
                'User-Agent': random.choice(USER_AGENTS),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            # Add delay between retries
            if attempt > 0:
                time.sleep(random.uniform(2, 5))
            
            response = requests.get(url, timeout=timeout, headers=headers)
            
            if response.status_code == 200:
                return response
            elif response.status_code == 429:  # Rate limited
                logging.warning(f"Rate limited on {url}, waiting longer...")
                time.sleep(random.uniform(10, 20))
                continue
            else:
                logging.warning(f"HTTP {response.status_code} for {url}")
                continue
                
        except requests.exceptions.Timeout:
            logging.warning(f"Timeout on attempt {attempt + 1} for {url}")
            continue
        except requests.exceptions.RequestException as e:
            logging.warning(f"Request error on attempt {attempt + 1} for {url}: {e}")
            continue
    
    return None

def extract_date_from_url(url):
    """Extract date from URL patterns like /2025/06/26/ or /article-2025-06-26/."""
    date_patterns = [
        r'/(\d{4})/(\d{1,2})/(\d{1,2})/',
        r'/(\d{4})-(\d{1,2})-(\d{1,2})/',
        r'(\d{4})(\d{2})(\d{2})',
        r'/(\d{1,2})/(\d{1,2})/(\d{4})/',
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, url)
        if match:
            try:
                groups = match.groups()
                if len(groups) == 3:
                    # Try different date formats
                    if len(groups[0]) == 4:  # YYYY/MM/DD or YYYY-MM-DD
                        year, month, day = int(groups[0]), int(groups[1]), int(groups[2])
                    else:  # MM/DD/YYYY
                        month, day, year = int(groups[0]), int(groups[1]), int(groups[2])
                    
                    dt = datetime(year, month, day, tzinfo=timezone.utc)
                    return dt.isoformat()
            except (ValueError, IndexError):
                continue
    
    return None

def extract_date_from_text_content(soup):
    """Extract date from article text content using various patterns."""
    text = soup.get_text()
    
    # Common date patterns in article text
    date_patterns = [
        r'Published:?\s*([A-Za-z]+ \d{1,2},? \d{4})',
        r'Updated:?\s*([A-Za-z]+ \d{1,2},? \d{4})',
        r'(\d{1,2} [A-Za-z]+ \d{4})',
        r'([A-Za-z]+ \d{1,2}, \d{4})',
        r'(\d{4}-\d{2}-\d{2})',
    ]
    
    for pattern in date_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            try:
                dt = dateutil.parser.parse(match, fuzzy=True)
                dt = normalize_datetime(dt)
                # Only accept dates from the last 2 years to avoid false positives
                if dt.year >= 2023:
                    return dt.isoformat()
            except:
                continue
    
    return None

def robust_extract_published_date(url, soup=None):
    """Extract published date with multiple fallback strategies."""
    
    # Strategy 1: Extract from URL
    url_date = extract_date_from_url(url)
    if url_date:
        logging.debug(f"Extracted date from URL: {url_date}")
        return url_date
    
    if not soup:
        return None
    
    # Strategy 2: Try prioritized meta tags
    meta_tags = [
        'article:published_time', 'datePublished', 'pubdate', 'publishdate', 
        'date', 'og:published_time', 'article:modified_time', 'dateModified',
        'lastmod', 'dc.date', 'DC.date'
    ]
    
    for meta_name in meta_tags:
        meta = (soup.find('meta', attrs={'property': meta_name}) or 
                soup.find('meta', attrs={'name': meta_name}) or
                soup.find('meta', attrs={'itemprop': meta_name}))
        if meta and meta.get('content'):
            try:
                dt = dateutil.parser.parse(meta['content'], fuzzy=True)
                dt = normalize_datetime(dt)
                logging.debug(f"Extracted date from meta tag {meta_name}: {dt.isoformat()}")
                return dt.isoformat()
            except Exception:
                continue
    
    # Strategy 3: Try <time> elements
    for time_elem in soup.find_all('time'):
        if time_elem.get('datetime'):
            try:
                dt = dateutil.parser.parse(time_elem['datetime'], fuzzy=True)
                dt = normalize_datetime(dt)
                logging.debug(f"Extracted date from time element: {dt.isoformat()}")
                return dt.isoformat()
            except Exception:
                continue
        if time_elem.text:
            try:
                dt = dateutil.parser.parse(time_elem.text, fuzzy=True)
                dt = normalize_datetime(dt)
                logging.debug(f"Extracted date from time text: {dt.isoformat()}")
                return dt.isoformat()
            except Exception:
                continue
    
    # Strategy 4: Try JSON-LD structured data
    json_ld_scripts = soup.find_all('script', type='application/ld+json')
    for script in json_ld_scripts:
        try:
            data = json.loads(script.string)
            if isinstance(data, dict):
                date_fields = ['datePublished', 'dateCreated', 'dateModified']
                for field in date_fields:
                    if field in data:
                        dt = dateutil.parser.parse(data[field], fuzzy=True)
                        dt = normalize_datetime(dt)
                        logging.debug(f"Extracted date from JSON-LD: {dt.isoformat()}")
                        return dt.isoformat()
        except:
            continue
    
    # Strategy 5: Extract from text content
    text_date = extract_date_from_text_content(soup)
    if text_date:
        logging.debug(f"Extracted date from text content: {text_date}")
        return text_date
    
    # Strategy 6: Look for date in class names or IDs
    date_elements = soup.find_all(attrs={'class': re.compile(r'date|time|publish', re.I)})
    date_elements.extend(soup.find_all(attrs={'id': re.compile(r'date|time|publish', re.I)}))
    
    for elem in date_elements:
        if elem.text:
            try:
                dt = dateutil.parser.parse(elem.text, fuzzy=True)
                dt = normalize_datetime(dt)
                if dt.year >= 2023:  # Sanity check
                    logging.debug(f"Extracted date from element with date class/id: {dt.isoformat()}")
                    return dt.isoformat()
            except:
                continue
    
    return None

# Main Function
if __name__ == "__main__":
    configure_logging()
    validate_db_connection()

    logging.info("Starting GitHub-scheduled scraper job...")
    success = run_web_scraping()

    if success:
        logging.info("Job completed successfully.")
    else:
        logging.error("Job failed.")
