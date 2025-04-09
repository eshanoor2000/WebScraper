import os
import json
import time
import sys
import requests
import pymongo
from pymongo.errors import BulkWriteError
import logging
from datetime import datetime, date, timedelta
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
load_dotenv()

# Logging Configuration
def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

# Initialize logging
configure_logging()

EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))

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
    "oshawa.ca", "milton.ca", "tocondonews.com"
])

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "brand_monitoring")
RAW_COLLECTION = os.getenv("RAW_COLLECTION", "raw_articles")

RSS_STANDARD_FEEDS = [
    "https://www.ontariocanada.com/registry/rss?feed=news&lang=en",
    "https://www.canlii.org/en/on/oncat/rss_new.xml",
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
        
        if today.day < 6:
            end_date = datetime(today.year, today.month, 6)
            if today.month == 1:
                start_date = datetime(today.year - 1, 12, 6)
            else:
                start_date = datetime(today.year, today.month - 1, 6)
        else:
            start_date = datetime(today.year, today.month, 6)
            if today.month == 12:
                end_date = datetime(today.year + 1, 1, 6)
            else:
                end_date = datetime(today.year, today.month + 1, 6)
                
        return start_date <= timestamp < end_date
    except ValueError:
        return False

def is_relevant_location(text):
    text_lower = text.lower()
    return any(term.lower() in text_lower for term in ONTARIO_TERMS)

def get_valid_date(date_str):
    if not date_str:
        return datetime.utcnow().isoformat()
    
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
            return dt.isoformat()
        except ValueError:
            continue
    
    try:
        if date_str.isdigit():
            return datetime.utcfromtimestamp(int(date_str)).isoformat()
    except:
        pass
    
    return datetime.utcnow().isoformat()

def get_matched_keywords(text, keywords, fuzzy_threshold=90):
    matched_keywords = []
    text_lower = text.lower()
    
    for keyword in keywords:
        keyword_lower = keyword.lower()

        if keyword_lower in text_lower:
            matched_keywords.append(keyword)
            continue

        if any(fuzz.partial_ratio(keyword_lower, sentence.lower()) >= fuzzy_threshold
               for sentence in text_lower.split(".") if sentence):
            matched_keywords.append(keyword)

    return matched_keywords

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

class BingNewsSpider(scrapy.Spider):
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
        if date.today() < date(2024, 3, 6):
            logging.info("Bing scraper will start from March 6, 2024.")
            return

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
        soup = BeautifulSoup(response.text, "html.parser")
        news_cards = soup.select("div.news-card")
        
        articles = []
        for card in news_cards:
            try:
                title_elem = card.select_one("a.title")
                link = title_elem.get("href") if title_elem else None
                title = title_elem.get_text(strip=True) if title_elem else None
                
                date_elem = card.select_one(".source")
                published_date = get_valid_date(date_elem.get_text(strip=True).split("·")[-1].strip()) if date_elem else get_valid_date("")
                
                if not link or not title:
                    continue

                domain = self.extract_domain(link)
                if any(ex in domain for ex in EXCLUDED_DOMAINS):
                    continue

                if not is_relevant_location(f"{title} {link}"):
                    continue

                matched_keywords = get_matched_keywords(f"{title} {link}", STANDARD_KEYWORDS)
                if not matched_keywords:
                    continue

                articles.append({
                    "title": title,
                    "link": link,
                    "published_date": published_date,
                    "tags": matched_keywords
                })
                
            except Exception as e:
                logging.error(f"Error parsing Bing news card: {e}")
                continue

        if not articles:
            logging.info(f"No relevant {self.name} articles found for this run.")

        save_scraped_data("bing", articles)

    def extract_domain(self, url):
        try:
            return url.split("//")[1].split("/")[0].replace("www.", "")
        except:
            return ""

class RSSFeedSpider(scrapy.Spider):
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
        if date.today() < date(2024, 3, 6):
            logging.info("RSS scraping will begin on March 6, 2024.")
            return

        for url in ALL_RSS_FEEDS:
            if requires_splash(url):
                yield SplashRequest(
                    url=url,
                    callback=self.parse_rss_splash,
                    args={"wait": 2}
                )
            else:
                yield scrapy.Request(url, callback=self.parse_rss)

    def parse_rss(self, response):
        """Parse standard RSS feeds"""
        try:
            feed = feedparser.parse(response.body)
            if feed.bozo:
                logging.warning(f"Skipping malformed RSS feed: {feed.bozo_exception}")
                return

            articles = []
            for entry in feed.entries:
                if not entry.title or not entry.link:
                    logging.warning(f"Skipping incomplete RSS entry: {entry}")
                    continue

                if not is_relevant_location(f"{entry.title} {entry.link}"):
                    continue

                published_date = get_valid_date(entry.get("published", ""))
                
                if not is_within_scrape_window(published_date):
                    continue
                    
                matched_keywords = get_matched_keywords(entry.title, STANDARD_KEYWORDS)
                articles.append({
                    "title": entry.title,
                    "link": entry.link,
                    "published_date": published_date,
                    "tags": matched_keywords
                })

            if not articles:
                logging.info(f"No relevant {self.name} articles found for this run.")

            save_scraped_data("rss", articles)

        except Exception as e:
            logging.error(f"Error parsing RSS feed: {e}")

    def parse_rss_splash(self, response):
        """Parse RSS feeds that require Splash rendering"""
        try:
            soup = BeautifulSoup(response.text, "html.parser")
            items = soup.find_all("item") or soup.find_all("entry")
            
            articles = []
            for item in items:
                title = item.find("title").text if item.find("title") else ""
                link = item.find("link").get("href", "") if hasattr(item.find("link"), "get") else item.find("link").text if item.find("link") else ""
                
                if not title or not link:
                    continue
                
                pub_date = item.find("pubdate").text if item.find("pubdate") else item.find("published").text if item.find("published") else ""
                published_date = get_valid_date(pub_date)
                
                if not is_within_scrape_window(published_date):
                    continue
                    
                matched_keywords = get_matched_keywords(title, STANDARD_KEYWORDS)
                articles.append({
                    "title": title,
                    "link": link,
                    "published_date": published_date,
                    "tags": matched_keywords
                })
            
            save_scraped_data("rss", articles)
        except Exception as e:
            logging.error(f"Error parsing Splash-rendered RSS feed: {e}")

def run_scrapy_spiders():
    try:
        settings = get_project_settings()
        splash_url = os.getenv("SPLASH_URL", "http://localhost:8050")
        settings.set("SPLASH_URL", splash_url)

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

        logging.info(f"Using Splash server: {splash_url}")

        process = CrawlerProcess(settings)
        process.crawl(BingNewsSpider)
        process.crawl(RSSFeedSpider)
        process.start(stop_after_crawl=True)
        logging.info("All Scrapy spiders completed successfully.")
        return True
    except Exception as e:
        logging.error(f"Scrapy spiders failed with error: {e}")
        return False

def run_web_scrapers():
    """Run web scrapers - scheduler-compatible, with summary email"""
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

# Main Function
if __name__ == "__main__":
    configure_logging()
    validate_db_connection()

    logging.info("Starting GitHub-scheduled scraper job...")
    success = run_web_scrapers()

    if success:
        logging.info("Job completed successfully.")
    else:
        logging.error("Job failed.")
