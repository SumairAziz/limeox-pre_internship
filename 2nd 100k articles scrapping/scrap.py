#!/usr/bin/env python3
"""
Large-scale news URL discovery + article scraper (BeautifulSoup edition).

Pipeline (one thread per source, N worker threads inside each):
  PHASE 1: RSS/Atom feeds (configured + auto-discovered + common paths)
  PHASE 2: sitemaps (only when the feeds are exhausted and the source still needs more)
  -> fetch with requests (retries, UA rotation, throttling)
  -> blocked pages retried with Selenium, then Playwright
  -> BeautifulSoup extraction -> CSV (resumable)

The run stops as soon as TARGET_TOTAL_ARTICLES is reached across all sources.
Each source is capped at MAX_ARTICLES_PER_SOURCE so the dataset stays diverse.

Install:
    pip install requests beautifulsoup4 lxml tqdm selenium playwright
    playwright install chromium
    (Selenium also needs Google Chrome installed; Selenium 4.6+ fetches the driver itself.)
"""

# ---- Standard library imports ----
import csv
import gzip
import json
import logging
import random
import re
import threading
import time
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from pathlib import Path
from urllib.parse import urljoin, urlparse, urldefrag, urlunparse
from urllib.robotparser import RobotFileParser

# ---- Third-party imports ----
import requests
from bs4 import BeautifulSoup, FeatureNotFound   # HTML / XML parsing (needs lxml installed)
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from playwright.sync_api import sync_playwright

# =============================================================================
# CONFIGURATION
# =============================================================================

# --- Targets ---------------------------------------------------------------
TARGET_TOTAL_ARTICLES = 100_000     # stop the whole run when this many are saved
MAX_ARTICLES_PER_SOURCE = 2500      # per-source cap (keeps the dataset diverse)
MAX_CANDIDATES_PER_SOURCE = 20000   # hard cap on URLs tried per source
MIN_WORDS = 150                     # reject extractions shorter than this

# --- Parallelism -----------------------------------------------------------
MAX_PARALLEL_SOURCES = 5         # sources processed at the same time
WORKERS_PER_SOURCE = 5        # concurrent article fetches inside one source
REQUEST_DELAY = 0.4               # avg seconds between requests to the SAME site

# --- Network / anti-block --------------------------------------------------
REQUEST_TIMEOUT = 25
MAX_RETRIES = 3
BACKOFF_BASE = 2.0
RESPECT_ROBOTS = True

# --- Browser fallback (for blocked URLs) -----------------------------------
BROWSER_FALLBACK = True
BROWSER_ORDER = ("selenium", "playwright")
BROWSER_CONCURRENCY = 2
BROWSER_TIMEOUT = 30
BROWSER_RENDER_WAIT = 2.5

# --- Discovery -------------------------------------------------------------
USE_FEEDS = True                    # PHASE 1: RSS/Atom
USE_SITEMAPS = True                 # PHASE 2: sitemaps (used after feeds run dry)
MAX_SITEMAPS_PER_SOURCE = 400       # max sitemap files fetched per source
MAX_FEEDS_PER_SOURCE = 40           # max feeds parsed per source
ALLOW_NON_PATTERN_URLS = True       # fall back to URLs not matching 'pattern'

# --- Files -----------------------------------------------------------------
OUTPUT_CSV = "news_articles3.csv"
URLS_LOG = "news_urls_scraped3.txt"
LOG_FILE = "scraping3.log"
RESUME = True

# --- Sources ---------------------------------------------------------------
# Sources that need a custom URL pattern, extra feeds or extra sitemaps.
#   home:     site root / section page (robots.txt is read from its origin)
#   pattern:  regex marking real article URLs (optional)
#   feeds:    extra RSS/Atom URLs, tried BEFORE auto-discovery
#   sitemaps: extra sitemap URLs
CONFIGURED_SOURCES = {
    "BBC": {
        "home": "https://www.bbc.com/news",
        "pattern": r"/news/(articles/|[a-z\-]+-\d{6,})",
        "feeds": [
            "https://feeds.bbci.co.uk/news/rss.xml",
            "https://feeds.bbci.co.uk/news/world/rss.xml",
            "https://feeds.bbci.co.uk/news/uk/rss.xml",
            "https://feeds.bbci.co.uk/news/business/rss.xml",
            "https://feeds.bbci.co.uk/news/politics/rss.xml",
            "https://feeds.bbci.co.uk/news/technology/rss.xml",
            "https://feeds.bbci.co.uk/news/science_and_environment/rss.xml",
            "https://feeds.bbci.co.uk/news/health/rss.xml",
            "https://feeds.bbci.co.uk/news/education/rss.xml",
            "https://feeds.bbci.co.uk/news/entertainment_and_arts/rss.xml",
        ],
    },
    "The Guardian": {
        "home": "https://www.theguardian.com/international",
        "pattern": r"/\d{4}/[a-z]{3}/\d{2}/",
        "feeds": [
            "https://www.theguardian.com/world/rss",
            "https://www.theguardian.com/uk/rss",
            "https://www.theguardian.com/us-news/rss",
            "https://www.theguardian.com/uk/business/rss",
            "https://www.theguardian.com/uk/technology/rss",
            "https://www.theguardian.com/science/rss",
            "https://www.theguardian.com/environment/rss",
            "https://www.theguardian.com/uk/culture/rss",
            "https://www.theguardian.com/uk/sport/rss",
            "https://www.theguardian.com/lifeandstyle/rss",
        ],
    },
    "NPR": {
        "home": "https://www.npr.org",
        "pattern": r"/\d{4}/\d{2}/\d{2}/",
        "feeds": [
            "https://feeds.npr.org/1001/rss.xml",
            "https://feeds.npr.org/1004/rss.xml",
            "https://feeds.npr.org/1014/rss.xml",
            "https://feeds.npr.org/1006/rss.xml",
            "https://feeds.npr.org/1019/rss.xml",
            "https://feeds.npr.org/1007/rss.xml",
            "https://feeds.npr.org/1128/rss.xml",
        ],
    },
    "France 24": {"home": "https://www.france24.com/en", "pattern": r"/en/.+/\d{8}-"},
    "CBS News": {"home": "https://www.cbsnews.com", "pattern": r"/news/"},
    "The Verge": {
        "home": "https://www.theverge.com",
        "pattern": r"/\d{5,}/|/\d{4}/\d{1,2}/\d{1,2}/",
    },
}

# Everything below is (name, homepage). Feeds are auto-discovered from the homepage
# <link rel="alternate"> tags (then common feed paths); sitemaps come from robots.txt.
# No URL pattern is set, so the filters + MIN_WORDS decide what counts as an article.
SIMPLE_SOURCES = [
    # --- World / general news -------------------------------------------------
    ("Al Jazeera", "https://www.aljazeera.com"),
    ("DW", "https://www.dw.com/en"),
    ("Euronews", "https://www.euronews.com"),
    ("Sky News", "https://news.sky.com"),
    ("The Independent", "https://www.independent.co.uk"),
    ("Mirror", "https://www.mirror.co.uk"),
    ("Evening Standard", "https://www.standard.co.uk"),
    ("Metro UK", "https://metro.co.uk"),
    ("RTE", "https://www.rte.ie/news"),
    ("Irish Examiner", "https://www.irishexaminer.com"),
    ("The Scotsman", "https://www.scotsman.com"),
    ("Newsweek", "https://www.newsweek.com"),
    ("Time", "https://time.com"),
    ("USA Today", "https://www.usatoday.com"),
    ("NBC News", "https://www.nbcnews.com"),
    ("ABC News US", "https://abcnews.go.com"),
    ("CNN", "https://edition.cnn.com"),
    ("Fox News", "https://www.foxnews.com"),
    ("NY Post", "https://nypost.com"),
    ("HuffPost", "https://www.huffpost.com"),
    ("Politico", "https://www.politico.com"),
    ("The Hill", "https://thehill.com"),
    ("Axios", "https://www.axios.com"),
    ("Vox", "https://www.vox.com"),
    ("Slate", "https://slate.com"),
    ("Salon", "https://www.salon.com"),
    ("The Daily Beast", "https://www.thedailybeast.com"),
    ("Mother Jones", "https://www.motherjones.com"),
    ("The Intercept", "https://theintercept.com"),
    ("ProPublica", "https://www.propublica.org"),
    ("Washington Examiner", "https://www.washingtonexaminer.com"),
    ("National Review", "https://www.nationalreview.com"),
    ("Reason", "https://reason.com"),
    ("The Conversation", "https://theconversation.com/global"),
    ("LA Times", "https://www.latimes.com"),
    ("Chicago Tribune", "https://www.chicagotribune.com"),
    ("Seattle Times", "https://www.seattletimes.com"),
    ("Denver Post", "https://www.denverpost.com"),
    ("Star Tribune", "https://www.startribune.com"),
    ("SFGate", "https://www.sfgate.com"),
    ("Miami Herald", "https://www.miamiherald.com"),
    ("Deseret News", "https://www.deseret.com"),
    # --- Middle East ----------------------------------------------------------
    ("Al-Monitor", "https://www.al-monitor.com"),
    ("Middle East Eye", "https://www.middleeasteye.net"),
    ("Times of Israel", "https://www.timesofisrael.com"),
    ("Jerusalem Post", "https://www.jpost.com"),
    ("Arab News", "https://www.arabnews.com"),
    ("Gulf News", "https://gulfnews.com"),
    ("Khaleej Times", "https://www.khaleejtimes.com"),
    ("The National UAE", "https://www.thenationalnews.com"),
    # --- South Asia -----------------------------------------------------------
    ("Dawn", "https://www.dawn.com"),
    ("Express Tribune", "https://tribune.com.pk"),
    ("The News Intl", "https://www.thenews.com.pk"),
    ("Pakistan Today", "https://www.pakistantoday.com.pk"),
    ("Business Recorder", "https://www.brecorder.com"),
    ("Times of India", "https://timesofindia.indiatimes.com"),
    ("Hindustan Times", "https://www.hindustantimes.com"),
    ("The Hindu", "https://www.thehindu.com"),
    ("NDTV", "https://www.ndtv.com"),
    ("Indian Express", "https://indianexpress.com"),
    ("Firstpost", "https://www.firstpost.com"),
    ("The Print", "https://theprint.in"),
    ("Scroll", "https://scroll.in"),
    ("The Wire", "https://thewire.in"),
    ("Economic Times", "https://economictimes.indiatimes.com"),
    ("Business Standard", "https://www.business-standard.com"),
    ("Livemint", "https://www.livemint.com"),
    ("Daily Star BD", "https://www.thedailystar.net"),
    ("Kathmandu Post", "https://kathmandupost.com"),
    ("Down To Earth", "https://www.downtoearth.org.in"),
    # --- East / South-East Asia ----------------------------------------------
    ("Japan Times", "https://www.japantimes.co.jp"),
    ("SCMP", "https://www.scmp.com"),
    ("Straits Times", "https://www.straitstimes.com"),
    ("CNA", "https://www.channelnewsasia.com"),
    ("Korea Herald", "https://www.koreaherald.com"),
    ("Korea Times", "https://www.koreatimes.co.kr"),
    ("Taipei Times", "https://www.taipeitimes.com"),
    ("Bangkok Post", "https://www.bangkokpost.com"),
    ("Jakarta Post", "https://www.thejakartapost.com"),
    ("Rappler", "https://www.rappler.com"),
    # --- Oceania / Canada -----------------------------------------------------
    ("ABC Australia", "https://www.abc.net.au/news"),
    ("Sydney Morning Herald", "https://www.smh.com.au"),
    ("The Age", "https://www.theage.com.au"),
    ("Brisbane Times", "https://www.brisbanetimes.com.au"),
    ("NZ Herald", "https://www.nzherald.co.nz"),
    ("Stuff NZ", "https://www.stuff.co.nz"),
    ("RNZ", "https://www.rnz.co.nz"),
    ("CBC", "https://www.cbc.ca/news"),
    ("Global News", "https://globalnews.ca"),
    ("Toronto Star", "https://www.thestar.com"),
    ("National Post", "https://nationalpost.com"),
    ("CTV News", "https://www.ctvnews.ca"),
    # --- Africa / Latin America ----------------------------------------------
    ("News24", "https://www.news24.com"),
    ("IOL", "https://www.iol.co.za"),
    ("Daily Maverick", "https://www.dailymaverick.co.za"),
    ("Mail & Guardian", "https://mg.co.za"),
    ("Nation Africa", "https://nation.africa"),
    ("Premium Times", "https://www.premiumtimesng.com"),
    ("Punch Nigeria", "https://punchng.com"),
    ("Vanguard Nigeria", "https://www.vanguardngr.com"),
    ("MercoPress", "https://en.mercopress.com"),
    # --- Technology -----------------------------------------------------------
    ("Ars Technica", "https://arstechnica.com"),
    ("TechCrunch", "https://techcrunch.com"),
    ("Wired", "https://www.wired.com"),
    ("Engadget", "https://www.engadget.com"),
    ("ZDNet", "https://www.zdnet.com"),
    ("CNET", "https://www.cnet.com"),
    ("Mashable", "https://mashable.com"),
    ("Gizmodo", "https://gizmodo.com"),
    ("The Register", "https://www.theregister.com"),
    ("TechRadar", "https://www.techradar.com"),
    ("Tom's Hardware", "https://www.tomshardware.com"),
    ("Tom's Guide", "https://www.tomsguide.com"),
    ("Android Authority", "https://www.androidauthority.com"),
    ("9to5Mac", "https://9to5mac.com"),
    ("9to5Google", "https://9to5google.com"),
    ("MacRumors", "https://www.macrumors.com"),
    ("The Next Web", "https://thenextweb.com"),
    ("VentureBeat", "https://venturebeat.com"),
    ("Lifehacker", "https://lifehacker.com"),
    ("How-To Geek", "https://www.howtogeek.com"),
    ("BleepingComputer", "https://www.bleepingcomputer.com"),
    ("Krebs on Security", "https://krebsonsecurity.com"),
    ("The Hacker News", "https://thehackernews.com"),
    ("SecurityWeek", "https://www.securityweek.com"),
    ("Dark Reading", "https://www.darkreading.com"),
    ("Phoronix", "https://www.phoronix.com"),
    ("XDA", "https://www.xda-developers.com"),
    ("Windows Central", "https://www.windowscentral.com"),
    ("Neowin", "https://www.neowin.net"),
    ("Digital Trends", "https://www.digitaltrends.com"),
    ("PCMag", "https://www.pcmag.com"),
    ("Tech Xplore", "https://techxplore.com"),
    # --- Science / health / environment --------------------------------------
    ("Phys.org", "https://phys.org"),
    ("ScienceDaily", "https://www.sciencedaily.com"),
    ("Live Science", "https://www.livescience.com"),
    ("Space.com", "https://www.space.com"),
    ("Science News", "https://www.sciencenews.org"),
    ("Scientific American", "https://www.scientificamerican.com"),
    ("Popular Science", "https://www.popsci.com"),
    ("Smithsonian Magazine", "https://www.smithsonianmag.com"),
    ("NASA", "https://www.nasa.gov"),
    ("ScienceAlert", "https://www.sciencealert.com"),
    ("EurekAlert", "https://www.eurekalert.org"),
    ("Medical Xpress", "https://medicalxpress.com"),
    ("News-Medical", "https://www.news-medical.net"),
    ("Medical News Today", "https://www.medicalnewstoday.com"),
    ("Healthline", "https://www.healthline.com"),
    ("WebMD", "https://www.webmd.com"),
    ("Verywell Health", "https://www.verywellhealth.com"),
    ("Psychology Today", "https://www.psychologytoday.com"),
    ("Mongabay", "https://news.mongabay.com"),
    ("Carbon Brief", "https://www.carbonbrief.org"),
    ("Inside Climate News", "https://insideclimatenews.org"),
    ("Grist", "https://grist.org"),
    ("Universe Today", "https://www.universetoday.com"),
    ("Treehugger", "https://www.treehugger.com"),
    # --- Business / finance ---------------------------------------------------
    ("CNBC", "https://www.cnbc.com"),
    ("MarketWatch", "https://www.marketwatch.com"),
    ("Business Insider", "https://www.businessinsider.com"),
    ("Fortune", "https://fortune.com"),
    ("Investopedia", "https://www.investopedia.com"),
    ("Quartz", "https://qz.com"),
    ("Fast Company", "https://www.fastcompany.com"),
    ("Inc", "https://www.inc.com"),
    ("Entrepreneur", "https://www.entrepreneur.com"),
    ("Benzinga", "https://www.benzinga.com"),
    ("Motley Fool", "https://www.fool.com"),
    ("Nasdaq", "https://www.nasdaq.com"),
    # --- Sport ----------------------------------------------------------------
    ("ESPN", "https://www.espn.com"),
    ("Sky Sports", "https://www.skysports.com"),
    ("CBS Sports", "https://www.cbssports.com"),
    ("Sporting News", "https://www.sportingnews.com"),
    ("Goal", "https://www.goal.com"),
    ("ESPNcricinfo", "https://www.espncricinfo.com"),
    ("Cricbuzz", "https://www.cricbuzz.com"),
    ("Fox Sports", "https://www.foxsports.com"),
    ("Cyclingnews", "https://www.cyclingnews.com"),
    ("Autosport", "https://www.autosport.com"),
    ("Motorsport.com", "https://www.motorsport.com"),
    # --- Entertainment / games / culture -------------------------------------
    ("Variety", "https://variety.com"),
    ("Hollywood Reporter", "https://www.hollywoodreporter.com"),
    ("Deadline", "https://deadline.com"),
    ("Rolling Stone", "https://www.rollingstone.com"),
    ("Pitchfork", "https://pitchfork.com"),
    ("IGN", "https://www.ign.com"),
    ("Polygon", "https://www.polygon.com"),
    ("Kotaku", "https://kotaku.com"),
    ("PC Gamer", "https://www.pcgamer.com"),
    ("GameSpot", "https://www.gamespot.com"),
    ("Eurogamer", "https://www.eurogamer.net"),
    ("Rock Paper Shotgun", "https://www.rockpapershotgun.com"),
    ("Screen Rant", "https://screenrant.com"),
    ("Collider", "https://collider.com"),
    ("IndieWire", "https://www.indiewire.com"),
    ("Consequence", "https://consequence.net"),
    ("NME", "https://www.nme.com"),
    ("Billboard", "https://www.billboard.com"),
    ("Entertainment Weekly", "https://ew.com"),
    ("People", "https://people.com"),
    ("The AV Club", "https://www.avclub.com"),
    ("Den of Geek", "https://www.denofgeek.com"),
    ("Empire", "https://www.empireonline.com"),
    ("Mental Floss", "https://www.mentalfloss.com"),
    ("Atlas Obscura", "https://www.atlasobscura.com"),
    ("Travel + Leisure", "https://www.travelandleisure.com"),
    ("Serious Eats", "https://www.seriouseats.com"),
]

# Final source table: configured sources first, then the simple ones.
SOURCES = dict(CONFIGURED_SOURCES)
for _name, _home in SIMPLE_SOURCES:
    SOURCES.setdefault(_name, {"home": _home})

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0",
]

EXCLUDED_SEGMENTS = {
    "live", "video", "videos", "gallery", "galleries", "tag", "tags",
    "author", "authors", "topic", "topics", "podcasts", "podcast",
    "profile", "search", "newsletters", "subscribe",
}
EXCLUDED_EXTENSIONS = (
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".pdf", ".mp3", ".mp4",
    ".xml", ".rss", ".json", ".css", ".js",
)
SITEMAP_SKIP_WORDS = ("video", "image", "tag", "author", "topic", "podcast", "gallery")
BLOCK_STATUSES = {401, 403, 407, 429, 451, 503}
BLOCK_TITLE_MARKERS = (
    "access denied", "just a moment", "attention required", "captcha",
    "forbidden", "are you a robot", "verify you are human",
)
FEED_TYPES = {
    "application/rss+xml", "application/atom+xml", "application/rdf+xml",
    "application/xml", "text/xml",
}
COMMON_FEED_PATHS = [
    "/feed", "/feed/", "/rss", "/rss.xml", "/feed.xml", "/atom.xml",
    "/rss/index.xml", "/xml/rss/all.xml", "/rss/all", "/feeds/all.rss.xml",
]
CSV_FIELDS = ["source", "title", "url", "date", "author", "content"]

# Tags that never contain article body text.
JUNK_TAGS = ["script", "style", "noscript", "nav", "header", "footer", "aside",
             "form", "iframe", "svg", "button", "figure", "figcaption", "select"]
# class/id fragments of page furniture (share bars, related links, ads, comments...).
JUNK_ATTR_RE = re.compile(
    r"comment|share|social|related|promo|newsletter|advert|sidebar|subscribe|"
    r"cookie|breadcrumb|recommend|trending|paywall|signup|outbrain|taboola|"
    r"caption|byline-meta|tags-list|read-more|most-read",
    re.I,
)
# CSS selectors for the likely article container (best one is chosen by text length).
CONTAINER_SELECTORS = (
    "article, [itemprop=articleBody], .article-body, .article-content, .article__body, "
    ".entry-content, .post-content, .story-body, .story-content, .content-body, "
    ".post-body, #article-body, main"
)

# =============================================================================
# SETUP
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(threadName)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8")],
)
for noisy in ("urllib3", "selenium", "bs4"):
    logging.getLogger(noisy).setLevel(logging.ERROR)

csv.field_size_limit(2**31 - 1)
_local = threading.local()
_browser_sem = threading.Semaphore(BROWSER_CONCURRENCY)
STOP = threading.Event()            # set once TARGET_TOTAL_ARTICLES is reached


# =============================================================================
# NETWORK HELPERS
# =============================================================================

# Per-site rate limiter shared by all threads of one source.
class Throttle:

    def __init__(self, delay):
        self.delay = delay
        self.lock = threading.Lock()
        self.next_slot = 0.0

    # Reserves the next request slot (with jitter) and sleeps until it arrives.
    def wait(self):
        with self.lock:
            now = time.monotonic()
            slot = max(now, self.next_slot)
            self.next_slot = slot + self.delay * random.uniform(0.7, 1.3)
        if slot > now:
            time.sleep(slot - now)


# Returns this thread's own requests.Session.
def get_session():
    if not hasattr(_local, "session"):
        s = requests.Session()
        adapter = HTTPAdapter(pool_connections=4, pool_maxsize=WORKERS_PER_SOURCE * 2)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _local.session = s
    return _local.session


# Downloads a URL with retries/backoff; returns (bytes or None, last status).
def fetch(url, throttle):
    status = None
    for attempt in range(1, MAX_RETRIES + 2):
        if STOP.is_set():
            return None, status
        throttle.wait()
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        try:
            r = get_session().get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        except requests.RequestException:
            status = None
            time.sleep(BACKOFF_BASE * attempt)
            continue

        status = r.status_code
        if status == 200:
            return r.content, status
        if status in (404, 410):
            return None, status
        if status in BLOCK_STATUSES or status >= 500:
            retry_after = r.headers.get("Retry-After", "")
            delay = float(retry_after) if retry_after.isdigit() else BACKOFF_BASE * attempt
            time.sleep(min(delay, 30))
            continue
        return None, status
    return None, status


# Loads a page in headless Chrome via Selenium; returns HTML or None.
def selenium_fetch(url):
    driver = None
    try:
        options = ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(BROWSER_TIMEOUT)
        driver.get(url)
        time.sleep(BROWSER_RENDER_WAIT)
        return driver.page_source
    except Exception as error:
        logging.warning("Selenium failed for %s: %s", url, error)
        return None
    finally:
        if driver:
            driver.quit()


# Loads a page in headless Chromium via Playwright; returns HTML or None.
def playwright_fetch(url):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                page = browser.new_page(user_agent=random.choice(USER_AGENTS))
                page.goto(url, wait_until="domcontentloaded", timeout=BROWSER_TIMEOUT * 1000)
                page.wait_for_timeout(int(BROWSER_RENDER_WAIT * 1000))
                return page.content()
            finally:
                browser.close()
    except Exception as error:
        logging.warning("Playwright failed for %s: %s", url, error)
        return None


# Detects tiny pages or bot-check titles so a useless browser result isn't accepted.
def looks_blocked(html):
    text = html if isinstance(html, str) else html.decode("utf-8", "ignore")
    if len(text) < 500:
        return True
    match = re.search(r"<title[^>]*>(.*?)</title>", text[:6000], re.I | re.S)
    title = match.group(1).lower() if match else ""
    return any(marker in title for marker in BLOCK_TITLE_MARKERS)


# Tries each browser in BROWSER_ORDER and returns the first good page, or None.
def browser_fetch(url):
    fetchers = {"selenium": selenium_fetch, "playwright": playwright_fetch}
    with _browser_sem:
        for name in BROWSER_ORDER:
            html = fetchers[name](url)
            if html and not looks_blocked(html):
                logging.info("Browser fallback (%s) succeeded: %s", name, url)
                return html
    return None


# =============================================================================
# BEAUTIFULSOUP HELPERS
# =============================================================================

# Parses HTML ("lxml", falling back to html.parser) or XML ("xml", needs lxml).
def make_soup(markup, parser="lxml"):
    try:
        return BeautifulSoup(markup, parser)
    except FeatureNotFound:
        if parser == "xml":
            raise
        return BeautifulSoup(markup, "html.parser")


# =============================================================================
# URL HELPERS
# =============================================================================

# Strips fragment, query string and trailing slash so one article = one key.
def normalize_url(url):
    url, _ = urldefrag(url.strip())
    p = urlparse(url)
    return urlunparse((p.scheme, p.netloc.lower(), p.path.rstrip("/"), "", "", ""))


# Cheap filter: plausible article URLs on the source's own site only.
def candidate_ok(url, base_host):
    p = urlparse(url)
    if p.scheme not in ("http", "https"):
        return False
    host = (p.hostname or "").lower().removeprefix("www.")
    if host != base_host and not host.endswith("." + base_host):
        return False
    path = p.path.rstrip("/").lower()
    if not path:
        return False
    if set(path.split("/")) & EXCLUDED_SEGMENTS:
        return False
    return not path.endswith(EXCLUDED_EXTENSIONS)


# =============================================================================
# DISCOVERY: robots.txt, RSS/Atom feeds (phase 1), sitemaps (phase 2)
# =============================================================================

# Holds a site's robots.txt rules and the sitemap URLs it advertises.
class SiteRules:

    def __init__(self, origin, throttle):
        self.rp = RobotFileParser()
        self.sitemaps = []
        content, _ = fetch(origin + "/robots.txt", throttle)
        if content:
            self.rp.parse(content.decode("utf-8", "ignore").splitlines())
            self.rp.modified()
            self.sitemaps = list(self.rp.site_maps() or [])
        else:
            self.rp.allow_all = True

    def allowed(self, url):
        return (not RESPECT_ROBOTS) or self.rp.can_fetch("*", url)


# Returns the article link of one RSS <item> or Atom <entry>.
def entry_link(entry):
    links = entry.find_all("link")
    for link in links:                                  # Atom: <link href=".." rel="alternate">
        href = link.get("href")
        if href and link.get("rel") in (None, "alternate"):
            return href.strip()
    for link in links:                                  # RSS: <link>https://...</link>
        text = link.get_text(strip=True)
        if text:
            return text
    guid = entry.find("guid")                           # last resort: permalink guid
    if guid and guid.get_text(strip=True).startswith("http"):
        return guid.get_text(strip=True)
    return ""


# PHASE 1. Configured feeds first, then feeds advertised on the homepage, then common paths.
def feed_urls(cfg, origin, throttle):
    candidates = list(cfg.get("feeds", []))
    advertised = []
    content, _ = fetch(cfg["home"], throttle)
    if content:
        soup = make_soup(content)
        for link in soup.find_all("link", href=True):
            rel = " ".join(link.get("rel") or []).lower()
            if "alternate" in rel and (link.get("type") or "").lower() in FEED_TYPES:
                advertised.append(urljoin(cfg["home"], link["href"]))
    candidates.extend(advertised)
    if not candidates:
        candidates.extend(origin + p for p in COMMON_FEED_PATHS)

    used = 0
    for feed_url in dict.fromkeys(candidates):
        if used >= MAX_FEEDS_PER_SOURCE or STOP.is_set():
            break
        data, _ = fetch(feed_url, throttle)
        if not data:
            continue
        try:
            entries = make_soup(data, "xml").find_all(["item", "entry"])
        except Exception as error:
            logging.warning("Bad feed %s: %s", feed_url, error)
            continue
        if not entries:
            continue
        used += 1
        for entry in entries:
            link = entry_link(entry)
            if link:
                yield link


# Parses sitemap XML (gzip-aware); returns ('index' | 'urls', [(url, lastmod), ...]).
def parse_sitemap(content):
    if content[:2] == b"\x1f\x8b":
        content = gzip.decompress(content)
    soup = make_soup(content, "xml")
    kind = "index" if soup.find("sitemapindex") else "urls"
    entries = []
    for node in soup.find_all(["sitemap", "url"]):
        loc = node.find("loc", recursive=False)
        if not loc or not loc.get_text(strip=True):
            continue
        lastmod = node.find("lastmod", recursive=False)
        entries.append((loc.get_text(strip=True), lastmod.get_text(strip=True) if lastmod else ""))
    return kind, entries


# PHASE 2. Walks sitemap files lazily (news sitemaps first, newest first).
def sitemap_urls(roots, throttle):
    roots = list(dict.fromkeys(roots))
    roots.sort(key=lambda u: "news" not in u.lower())
    queue = deque(roots)
    fetched = 0

    while queue and fetched < MAX_SITEMAPS_PER_SOURCE and not STOP.is_set():
        sitemap = queue.popleft()
        content, _ = fetch(sitemap, throttle)
        fetched += 1
        if not content:
            continue
        try:
            kind, entries = parse_sitemap(content)
        except Exception as error:
            logging.warning("Bad sitemap %s: %s", sitemap, error)
            continue

        entries.sort(key=lambda e: (e[1], e[0]), reverse=True)
        if kind == "index":
            children = [
                loc for loc, _ in entries
                if not any(w in loc.lower() for w in SITEMAP_SKIP_WORDS)
            ]
            queue.extendleft(reversed(children))
        else:
            for loc, _ in entries:
                yield loc


# Lazy stream of de-duplicated candidates: ALL feeds first, then sitemaps only if still needed.
def iter_candidates(cfg, origin, base_host, throttle, rules):
    pattern = re.compile(cfg["pattern"]) if cfg.get("pattern") else None
    seen = set()

    def emit(urls):
        for url in urls:
            if not candidate_ok(url, base_host):
                continue
            key = normalize_url(url)
            if key in seen:
                continue
            seen.add(key)
            yield key

    phases = []
    if USE_FEEDS:
        phases.append(lambda: feed_urls(cfg, origin, throttle))
    if USE_SITEMAPS:
        roots = rules.sitemaps + cfg.get("sitemaps", [])
        if not roots:
            roots = [origin + p for p in ("/sitemap.xml", "/news-sitemap.xml", "/sitemap_index.xml")]
        phases.append(lambda: sitemap_urls(roots, throttle))

    for make_phase in phases:                     # a phase starts only after the previous one is exhausted
        buffered = []
        for url in make_phase():
            if not pattern or pattern.search(url):
                yield from emit([url])
            elif ALLOW_NON_PATTERN_URLS:
                buffered.append(url)
                if len(buffered) >= 500:
                    yield from emit(buffered)
                    buffered = []
        if buffered:
            yield from emit(buffered)


# =============================================================================
# EXTRACTION (BeautifulSoup)
# =============================================================================

# Recursively finds the first value stored under `key` in parsed JSON-LD.
def find_key(node, key):
    if isinstance(node, dict):
        if key in node:
            return node[key]
        for value in node.values():
            found = find_key(value, key)
            if found:
                return found
    elif isinstance(node, list):
        for item in node:
            found = find_key(item, key)
            if found:
                return found
    return None


# Collects all JSON-LD blocks of a page as parsed objects.
def json_ld_blocks(soup):
    blocks = []
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            blocks.append(json.loads(tag.string or tag.get_text() or ""))
        except (ValueError, TypeError):
            continue
    return blocks


# First non-empty <meta> content among the given (attribute, value) pairs.
def meta_content(soup, *pairs):
    for attr, value in pairs:
        tag = soup.find("meta", attrs={attr: value})
        if tag and (tag.get("content") or "").strip():
            return tag["content"].strip()
    return ""


# Article title: og:title -> <h1> -> <title>, with " | Site" suffixes trimmed.
def get_title(soup):
    title = meta_content(soup, ("property", "og:title"), ("name", "twitter:title"))
    if not title:
        h1 = soup.find("h1")
        title = h1.get_text(" ", strip=True) if h1 else ""
    if not title and soup.title:
        title = soup.title.get_text(strip=True)
    return re.split(r"\s+[|\u2013\u2014]\s+", title)[0].strip()


# Publication date: meta tags -> <time datetime> -> JSON-LD.
def get_date(soup, ld):
    date = meta_content(
        soup, ("property", "article:published_time"), ("name", "article:published_time"),
        ("itemprop", "datePublished"), ("name", "date"), ("name", "pubdate"),
        ("name", "publishdate"), ("name", "DC.date.issued"), ("property", "og:pubdate"),
    )
    if date:
        return date
    time_tag = soup.find("time", attrs={"datetime": True})
    if time_tag:
        return time_tag["datetime"].strip()
    for block in ld:
        value = find_key(block, "datePublished")
        if isinstance(value, str) and value:
            return value
    return ""


# Author: meta tags -> rel=author -> byline element -> JSON-LD.
def get_author(soup, ld):
    author = meta_content(soup, ("name", "author"), ("property", "article:author"),
                          ("name", "parsely-author"), ("name", "byl"))
    if author and not author.lower().startswith("http"):
        return author
    tag = soup.find(attrs={"rel": "author"}) or soup.find(attrs={"itemprop": "author"})
    if tag and tag.get_text(strip=True):
        return tag.get_text(" ", strip=True)[:150]
    tag = soup.find(class_=re.compile(r"byline|author", re.I))
    if tag and tag.get_text(strip=True):
        return re.sub(r"^\s*by\s+", "", tag.get_text(" ", strip=True), flags=re.I)[:150]
    for block in ld:
        value = find_key(block, "author")
        if isinstance(value, list) and value:
            value = value[0]
        if isinstance(value, dict):
            value = value.get("name")
        if isinstance(value, str) and value:
            return value
    return ""


# Removes navigation, ads, share bars etc. in place so only body text remains.
def strip_junk(soup):
    for tag in soup(JUNK_TAGS):
        tag.decompose()
    doomed = []
    for tag in soup.find_all(True):
        if tag.attrs is None:
            continue
        marker = " ".join(tag.get("class") or []) + " " + (tag.get("id") or "")
        if JUNK_ATTR_RE.search(marker):
            doomed.append(tag)
    for tag in doomed:
        if tag.attrs is not None:                       # skip tags removed along with a parent
            tag.decompose()


# Paragraph texts of one container (short fragments and consecutive duplicates dropped).
def paragraphs_of(container):
    texts = []
    for p in container.find_all("p"):
        text = p.get_text(" ", strip=True)
        if len(text.split()) >= 5 and (not texts or texts[-1] != text):
            texts.append(text)
    return texts


# Converts raw HTML into title/date/author/content with BeautifulSoup, or (None, reason).
def extract_article(html, url):
    soup = make_soup(html)
    og_type = meta_content(soup, ("property", "og:type")).lower()
    ld = json_ld_blocks(soup)
    title = get_title(soup)
    date = get_date(soup, ld)
    author = get_author(soup, ld)

    strip_junk(soup)
    best = []
    for container in soup.select(CONTAINER_SELECTORS):
        texts = paragraphs_of(container)
        if sum(len(t) for t in texts) > sum(len(t) for t in best):
            best = texts
    if not best and soup.body:
        best = paragraphs_of(soup.body)

    text = "\n\n".join(best).strip()
    if not title or not text:
        return None, "empty"
    if og_type == "website" and not date:               # section/home page, not an article
        return None, "empty"
    if len(text.split()) < MIN_WORDS:
        return None, "short"
    return {"title": title, "date": date, "author": author, "content": text}, None


# Fetches one URL (requests, then Selenium/Playwright if blocked) and extracts it.
def process_url(url, source, throttle, rules):
    if STOP.is_set():
        return None, "stopped"
    if not rules.allowed(url):
        return None, "robots"

    content, status = fetch(url, throttle)
    if content is None and BROWSER_FALLBACK and (status in BLOCK_STATUSES or status is None):
        content = browser_fetch(url)
    if content is None:
        return None, "blocked" if status in BLOCK_STATUSES else "error"

    try:
        data, reason = extract_article(content, url)
    except Exception as error:
        logging.warning("Extraction crashed for %s: %s", url, error)
        return None, "error"
    if not data:
        return None, reason
    data.update(source=source, url=url)
    return data, None


# =============================================================================
# STORAGE (thread-safe, incremental, resumable)
# =============================================================================

class Store:

    # Opens the CSV: in resume mode reloads saved URLs/counts and appends, else starts fresh.
    def __init__(self, csv_path, urls_path, resume):
        self.csv_path = Path(csv_path)
        self.urls_path = Path(urls_path)
        self.lock = threading.Lock()
        self.urls = set()
        self.counts = Counter()
        self.total = 0

        if resume and self.csv_path.exists():
            with open(self.csv_path, newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    self.urls.add(normalize_url(row["url"]))
                    self.counts[row["source"]] += 1
                    self.total += 1
            self._file = open(self.csv_path, "a", newline="", encoding="utf-8-sig")
            self._writer = csv.DictWriter(self._file, fieldnames=CSV_FIELDS)
        else:
            self._file = open(self.csv_path, "w", newline="", encoding="utf-8-sig")
            self._writer = csv.DictWriter(self._file, fieldnames=CSV_FIELDS)
            self._writer.writeheader()
            self.urls_path.write_text("", encoding="utf-8")

    def seen(self, url):
        return url in self.urls

    # Saves one row (flushed immediately); returns False for duplicates or once the target is hit.
    def add(self, row):
        with self.lock:
            key = normalize_url(row["url"])
            if key in self.urls or self.total >= TARGET_TOTAL_ARTICLES:
                return False
            self.urls.add(key)
            self.counts[row["source"]] += 1
            self.total += 1
            self._writer.writerow({k: row.get(k, "") for k in CSV_FIELDS})
            self._file.flush()
            with open(self.urls_path, "a", encoding="utf-8") as f:
                f.write(row["url"] + "\n")
            if self.total >= TARGET_TOTAL_ARTICLES:
                STOP.set()
            return True

    def close(self):
        self._file.close()


# =============================================================================
# PER-SOURCE WORKER
# =============================================================================

# Runs discovery + scraping for one source until its cap/target is met or candidates run out.
def scrape_source(name, cfg, overall, store):
    stats = Counter()
    need = MAX_ARTICLES_PER_SOURCE - store.counts[name]
    if need <= 0 or STOP.is_set():
        return stats

    parsed = urlparse(cfg["home"])
    origin = f"{parsed.scheme}://{parsed.netloc}"
    base_host = (parsed.hostname or "").lower().removeprefix("www.")
    throttle = Throttle(REQUEST_DELAY)

    rules = SiteRules(origin, throttle)
    candidates = iter_candidates(cfg, origin, base_host, throttle, rules)

    exhausted = False
    pending = set()

    with ThreadPoolExecutor(WORKERS_PER_SOURCE, thread_name_prefix=name[:6]) as pool:
        while True:
            while (
                not exhausted
                and not STOP.is_set()
                and stats["ok"] < need
                and len(pending) < WORKERS_PER_SOURCE * 2
                and stats["tried"] + len(pending) < MAX_CANDIDATES_PER_SOURCE
            ):
                try:
                    url = next(candidates)
                except StopIteration:
                    exhausted = True
                    break
                except Exception:
                    logging.exception("Discovery crashed for %s", name)
                    exhausted = True
                    break
                if store.seen(url):
                    continue
                pending.add(pool.submit(process_url, url, name, throttle, rules))

            if not pending:
                break

            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                if future.cancelled():
                    continue
                stats["tried"] += 1
                try:
                    row, reason = future.result()
                except Exception:
                    logging.exception("Worker crashed")
                    row, reason = None, "error"

                if row and stats["ok"] < need and store.add(row):
                    stats["ok"] += 1
                    overall.update(1)
                elif not row:
                    stats[reason] += 1

            overall.set_postfix(sources_active=threading.active_count(), refresh=True)
            if stats["ok"] >= need or STOP.is_set():
                for f in pending:
                    f.cancel()

    tqdm.write(f"[{name}] saved {stats['ok']} (tried {stats['tried']}, blocked {stats['blocked']})")
    logging.info("%s finished: %s", name, dict(stats))
    return stats


# =============================================================================
# MAIN
# =============================================================================

def main():
    store = Store(OUTPUT_CSV, URLS_LOG, RESUME)
    if store.total >= TARGET_TOTAL_ARTICLES:
        STOP.set()
    names = list(SOURCES)

    overall = tqdm(
        total=TARGET_TOTAL_ARTICLES, initial=min(store.total, TARGET_TOTAL_ARTICLES),
        desc="TOTAL", unit="art",
        bar_format="{desc} {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} "
                   "[{elapsed}<{remaining}, {rate_fmt}]{postfix}",
    )

    results = {}
    try:
        with ThreadPoolExecutor(MAX_PARALLEL_SOURCES, thread_name_prefix="src") as pool:
            futures = {pool.submit(scrape_source, n, SOURCES[n], overall, store): n for n in names}
            for future, name in futures.items():
                try:
                    results[name] = future.result()
                except Exception:
                    logging.exception("Source %s crashed", name)
                    results[name] = Counter()
    except KeyboardInterrupt:
        STOP.set()
        logging.warning("Interrupted by user; progress is saved, rerun to resume.")
        print("\nInterrupted. Progress saved; rerun to resume.")
    finally:
        overall.close()
        store.close()

    print("\n" + "=" * 62)
    print(f"{'SOURCE':<24}{'SAVED':>8}{'TRIED':>8}{'BLOCKED':>9}{'SHORT':>7}{'ERR':>6}")
    for name in sorted(names, key=lambda n: -store.counts[n]):
        s = results.get(name, Counter())
        if store.counts[name] == 0 and s["tried"] == 0:
            continue
        print(
            f"{name:<24}{store.counts[name]:>8}{s['tried']:>8}{s['blocked']:>9}"
            f"{s['short'] + s['empty']:>7}{s['error']:>6}"
        )
    print(f"\nTotal articles in {OUTPUT_CSV}: {store.total}")


if __name__ == "__main__":
    main()