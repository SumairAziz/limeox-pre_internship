#!/usr/bin/env python3
"""
Large-scale news URL discovery + article scraper (BeautifulSoup edition).

Pipeline (one thread per source, N worker threads inside each):
  PHASE 1: RSS/Atom feeds (configured + auto-discovered + common paths)
  PHASE 2: sitemaps (only when the feeds are exhausted and the source still needs more)
  -> fetch with curl_cffi (TLS impersonation) or requests (retries, throttling)
  -> blocked pages retried with persistent Selenium, then persistent Playwright
  -> BeautifulSoup extraction -> CSV (resumable)

The run stops as soon as TARGET_TOTAL_ARTICLES is reached across all sources.
Each source is capped at MAX_ARTICLES_PER_SOURCE so the dataset stays diverse.

ACCEPTANCE CHECKS (implemented by this version):
  1. Existing CSV / URL log / log file are only ever appended to; no truncation,
     no second header, and a missing final newline is repaired with one append.
  2. Ctrl+C stores progress; a restart reloads the CSV, URL log, attempt ledger and
     sitemap discovery state, so finished URLs/sitemaps are skipped.
  3. On the first run of this version the sidecar state files are created and the
     attempt ledger is bootstrapped in memory from the CSV and URL log.
  4. A network outage pauses workers, writes no failure ledger entries and launches
     no browsers; a monitor resumes everything when connectivity returns.
  5. Domains that block plain HTTP are routed to the browser after the circuit
     breaker trips and that decision survives restarts in the domain state file.
  6. The final table reports articles saved through plain HTTP vs browser routes.

Install:
    pip install requests beautifulsoup4 lxml tqdm selenium playwright
    pip install curl_cffi        # optional: TLS fingerprint impersonation
    playwright install chromium
    (Selenium also needs Google Chrome installed; Selenium 4.6+ fetches the driver itself.)
"""

# ---- Standard library imports ----
import atexit
import csv
import gzip
import json
import logging
import os
import random
import re
import signal
import socket
import threading
import time
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
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

# ---- Optional dependency: curl_cffi (TLS impersonation) ---------------------
try:
    from curl_cffi import requests as curl_requests
    CURL_CFFI_AVAILABLE = True
except Exception:
    curl_requests = None
    CURL_CFFI_AVAILABLE = False

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
MAX_DELAY = 6.0                   # adaptive per-domain delay ceiling (seconds)

# --- Network / anti-block --------------------------------------------------
REQUEST_TIMEOUT = 25
MAX_RETRIES = 3
BACKOFF_BASE = 2.0
RESPECT_ROBOTS = True

# --- Connectivity / outage handling ----------------------------------------
NETWORK_CHECK_TIMEOUT = 3.0       # TCP connect timeout for the connectivity probe
NETWORK_CHECK_CACHE = 5.0         # seconds a probe result is reused
OFFLINE_RETRY_BASE = 10.0         # monitor starts retrying after this many seconds
OFFLINE_RETRY_MAX = 120.0         # capped monitor backoff (seconds)

# --- Browser fallback (for blocked URLs) -----------------------------------
BROWSER_FALLBACK = True
BROWSER_ORDER = ("selenium", "playwright")
BROWSER_CONCURRENCY = 2           # global cap on concurrent browser pages
BROWSER_PER_DOMAIN = 1            # per-domain browser slots (one blocked site can't hog all)
BROWSER_TIMEOUT = 30
BROWSER_RENDER_WAIT = 2.5         # max seconds to wait for <p>/<article> content
BROWSER_RECYCLE_EVERY = 50        # pages before a persistent browser is recreated
BROWSER_DISCOVERY_MAX = 20        # browser fetches allowed for discovery per source
BROWSER_DOMAIN_DELAY = 1.0        # per-domain spacing between browser page loads

# --- Discovery -------------------------------------------------------------
USE_FEEDS = True                    # PHASE 1: RSS/Atom
USE_SITEMAPS = True                 # PHASE 2: sitemaps (used after feeds run dry)
MAX_SITEMAPS_PER_SOURCE = 400       # max sitemap files fetched per source
MAX_FEEDS_PER_SOURCE = 40           # max feeds parsed per source
ALLOW_NON_PATTERN_URLS = True       # fall back to URLs not matching 'pattern'
SITEMAP_MAX_AGE_DAYS = 90           # skip sitemap URL entries older than this (0/None disables)
SITEMAP_RECHECK_HOURS = 24          # re-read a consumed sitemap with unknown lastmod after this

# --- Resumable state / attempt policy --------------------------------------
ATTEMPT_LEDGER = "news_attempted3.jsonl"
DISCOVERY_STATE = "news_discovery_state3.jsonl"
DOMAIN_STATE = "news_domain_state3.json"
RETRY_FAILED_ON_RESUME = True       # retry blocked/error URLs on later runs (bounded below)
MAX_TOTAL_ATTEMPTS = 2              # total attempts across runs for blocked/error URLs
DOMAIN_SAVE_INTERVAL = 10.0         # seconds between opportunistic domain-state writes

# --- Per-domain routing circuit breaker ------------------------------------
BROWSER_ONLY_MIN_SAMPLES = 20       # plain attempts before the success-rate test is applied
BROWSER_ONLY_THRESHOLD = 0.05       # below this plain success rate -> browser_only
PLAIN_REPROBE_EVERY = 100           # browser-routed URLs before a plain re-probe
DOMAIN_DEAD_THRESHOLD = 15          # consecutive total failures -> stop this source for the run

# --- Files -----------------------------------------------------------------
OUTPUT_CSV = "news_articles3.csv"
URLS_LOG = "news_urls_scraped3.txt"
LOG_FILE = "scraping3.log"
RESUME = True

# --- Sources ---------------------------------------------------------------
# Sources that need a custom URL pattern, extra feeds or extra sitemaps.
#   home:     site root / section page (robots.txt is read from its origin)
#   pattern:  regex marking real article URLs (optional)
#   strict_pattern: if True, URLs not matching 'pattern' are never used
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
    "France 24": {
        "home": "https://www.france24.com/en",
        "pattern": r"/en/.+/\d{8}-",
        "strict_pattern": True,           # only English article URLs, no fallback
    },
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

# Chrome-only UAs: the requests fallback must not mix Safari/Firefox UA strings
# with a Chrome-like TLS fingerprint. One UA is chosen per thread session.
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
]
BROWSER_USER_AGENT = USER_AGENTS[0]   # fixed UA used by persistent browsers

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
NO_RETRY_BLOCK_STATUSES = {401, 403, 407, 451}
TERMINAL_OUTCOMES = {"ok", "short", "empty", "robots", "http_404", "http_410"}
RETRYABLE_OUTCOMES = {"blocked", "error"}
OUTAGE_MARKERS = (
    "ERR_INTERNET_DISCONNECTED", "ERR_NAME_NOT_RESOLVED", "ERR_NETWORK_CHANGED",
)
BLOCK_TITLE_MARKERS = (
    "access denied", "just a moment", "attention required", "captcha",
    "forbidden", "are you a robot", "verify you are human",
    "verifying you are human", "checking your browser", "ddos protection",
    "consent", "enable javascript", "unusual traffic",
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
    handlers=[logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")],
)
for noisy in ("urllib3", "selenium", "bs4", "curl_cffi", "playwright"):
    logging.getLogger(noisy).setLevel(logging.ERROR)

csv.field_size_limit(2**31 - 1)
_local = threading.local()
_browser_sem = threading.Semaphore(BROWSER_CONCURRENCY)
STOP = threading.Event()            # set once TARGET_TOTAL_ARTICLES is reached
SHUTDOWN = threading.Event()        # set on Ctrl+C / SIGTERM: no ledger writes for in-flight URLs
OFFLINE = threading.Event()         # set while the machine has no network
_net_lock = threading.Lock()
_net_cache = {"checked": 0.0, "online": True}
_curl_warn_lock = threading.Lock()
_curl_warned = False
_domain_lock = threading.Lock()
_domains = {}
_domain_last_save = 0.0
_browser_registry_lock = threading.Lock()
_selenium_registry = set()
_playwright_registry = set()


# =============================================================================
# NETWORK HELPERS
# =============================================================================

# -----------------------------------------------------------------------------
# Connectivity: one short TCP probe, cached, shared by every worker.
# -----------------------------------------------------------------------------

def is_online(force=False):
    now = time.monotonic()
    with _net_lock:
        if not force and now - _net_cache["checked"] < NETWORK_CHECK_CACHE:
            return _net_cache["online"]
    online = False
    for host, port in (("1.1.1.1", 443), ("8.8.8.8", 53)):
        try:
            with socket.create_connection((host, port), timeout=NETWORK_CHECK_TIMEOUT):
                online = True
                break
        except OSError:
            continue
    with _net_lock:
        _net_cache["checked"] = time.monotonic()
        _net_cache["online"] = online
    return online


def mark_offline():
    with _net_lock:
        was_online = not OFFLINE.is_set()
        OFFLINE.set()
    if was_online:
        logging.warning("Network down: all workers paused (no ledger writes, no browser launches)")


# Called after a network exception; returns True when the machine is offline.
def note_network_error():
    if is_online(force=True):
        return False
    mark_offline()
    return True


def wait_until_online():
    while OFFLINE.is_set() and not STOP.is_set():
        STOP.wait(1.0)


# Single monitor thread: retry the probe with capped backoff and log transitions once.
def network_monitor():
    while not STOP.is_set():
        if not OFFLINE.wait(1.0):
            continue
        delay = OFFLINE_RETRY_BASE
        while not STOP.is_set() and OFFLINE.is_set():
            if is_online(force=True):
                with _net_lock:
                    OFFLINE.clear()
                logging.info("Network restored: workers resuming")
                break
            if STOP.wait(delay):
                return
            delay = min(OFFLINE_RETRY_MAX, delay * 1.5)


# Per-site rate limiter shared by all threads of one source. When a domain state
# is attached, its adaptive delay is read before each reservation.
class Throttle:

    def __init__(self, delay, domain=None):
        self.delay = delay
        self.domain = domain
        self.lock = threading.Lock()
        self.next_slot = 0.0

    # Reserves the next request slot (with jitter) and sleeps until it arrives.
    def wait(self):
        with self.lock:
            now = time.monotonic()
            slot = max(now, self.next_slot)
            current = self.domain.get_delay() if self.domain else self.delay
            self.next_slot = slot + current * random.uniform(0.7, 1.3)
        if slot > now:
            STOP.wait(slot - now)


# -----------------------------------------------------------------------------
# Per-domain routing memory (adaptive delay + browser-only circuit breaker)
# -----------------------------------------------------------------------------

class DomainState:

    def __init__(self, host, data=None):
        data = data or {}
        self.host = host
        self.lock = threading.RLock()
        self.plain_ok = int(data.get("plain_ok", 0) or 0)
        self.plain_fail = int(data.get("plain_fail", 0) or 0)
        self.browser_ok = int(data.get("browser_ok", 0) or 0)
        self.browser_fail = int(data.get("browser_fail", 0) or 0)
        self.browser_only = bool(data.get("browser_only", False))
        self.since_probe = int(data.get("since_probe", 0) or 0)
        self.delay = min(MAX_DELAY, max(REQUEST_DELAY, float(data.get("delay", REQUEST_DELAY))))
        self.consecutive_failures = 0        # per-run only
        self.dead = False                    # per-run only
        self.browser_sem = threading.Semaphore(BROWSER_PER_DOMAIN)
        self.browser_throttle = Throttle(BROWSER_DOMAIN_DELAY)

    def get_delay(self):
        with self.lock:
            return self.delay

    # "plain" or "browser" for the next article URL; may schedule a plain re-probe.
    def route_for_url(self):
        with self.lock:
            if self.dead:
                return "dead"
            if not self.browser_only:
                return "plain"
            self.since_probe += 1
            if self.since_probe >= PLAIN_REPROBE_EVERY:
                self.since_probe = 0
                logging.info("Domain %s: re-probing plain HTTP (browser-routed %d URLs)",
                             self.host, PLAIN_REPROBE_EVERY)
                return "plain"
            return "browser"

    def note_plain_result(self, ok, counted=True):
        message = None
        changed = False
        with self.lock:
            if ok:
                if self.browser_only and counted:
                    self.browser_only = False
                    self.plain_ok = 0
                    self.plain_fail = 0
                    self.since_probe = 0
                    message = ("Domain %s: plain HTTP works again; leaving browser-only mode"
                               % self.host)
                    changed = True
                if counted:
                    self.plain_ok += 1
                if self.delay > REQUEST_DELAY:
                    self.delay = max(REQUEST_DELAY, self.delay * 0.97)
            elif counted:
                self.plain_fail += 1

            if counted and not self.browser_only:
                total = self.plain_ok + self.plain_fail
                if total >= BROWSER_ONLY_MIN_SAMPLES:
                    rate = self.plain_ok / total
                    if rate < BROWSER_ONLY_THRESHOLD:
                        self.browser_only = True
                        self.since_probe = 0
                        message = ("Domain %s: plain success rate %.3f after %d attempts; "
                                   "switching to browser_only" % (self.host, rate, total))
                        changed = True
        if message:
            logging.info(message)
        if changed:
            save_domain_state(force=True)
        else:
            save_domain_state()

    # A 429/403 burst doubles the domain delay (bounded by MAX_DELAY).
    def note_block(self, status):
        if status not in (403, 429):
            return
        with self.lock:
            self.delay = min(MAX_DELAY, max(self.delay * 2.0, self.delay + 0.5))
        save_domain_state()

    def note_browser_result(self, ok):
        with self.lock:
            if ok:
                self.browser_ok += 1
            else:
                self.browser_fail += 1
        save_domain_state()

    # A fully failed URL (both routes exhausted). Returns True on the dead transition.
    def note_route_failure(self):
        with self.lock:
            self.consecutive_failures += 1
            if self.consecutive_failures >= DOMAIN_DEAD_THRESHOLD and not self.dead:
                self.dead = True
                logging.warning(
                    "Domain %s: dead after %d consecutive failures; stopping this source "
                    "for this run (URLs left un-ledgered)",
                    self.host, self.consecutive_failures,
                )
                return True
        return False

    def note_route_success(self):
        with self.lock:
            self.consecutive_failures = 0

    def to_dict(self):
        with self.lock:
            return {
                "plain_ok": self.plain_ok,
                "plain_fail": self.plain_fail,
                "browser_ok": self.browser_ok,
                "browser_fail": self.browser_fail,
                "browser_only": self.browser_only,
                "since_probe": self.since_probe,
                "delay": round(self.delay, 3),
            }


def load_domain_state():
    path = Path(DOMAIN_STATE)
    if not path.exists():
        logging.info("No domain state file yet; starting with no routing hints")
        return
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("top-level JSON is not an object")
    except Exception as error:
        logging.warning("Domain state %s is missing/corrupt (%s); ignoring it",
                        DOMAIN_STATE, error)
        return
    with _domain_lock:
        for host, data in raw.items():
            if isinstance(data, dict):
                _domains[host] = DomainState(host, data)
    logging.info("Loaded routing hints for %d domains", len(_domains))


def get_domain(host):
    with _domain_lock:
        domain = _domains.get(host)
        if domain is None:
            domain = DomainState(host)
            _domains[host] = domain
        return domain


# Hint file only: rewritten atomically via temp file + os.replace, never required.
def save_domain_state(force=False):
    global _domain_last_save
    now = time.time()
    with _domain_lock:
        if not force and now - _domain_last_save < DOMAIN_SAVE_INTERVAL:
            return
        _domain_last_save = now
        data = {domain.host: domain.to_dict() for domain in _domains.values()}
        path = Path(DOMAIN_STATE)
        tmp = Path(DOMAIN_STATE + ".tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as handle:
                json.dump(data, handle, indent=1, sort_keys=True)
                handle.flush()
            os.replace(tmp, path)
        except Exception as error:
            logging.warning("Could not save domain state %s: %s", DOMAIN_STATE, error)


# -----------------------------------------------------------------------------
# Plain HTTP client: curl_cffi (TLS impersonation) with a requests fallback
# -----------------------------------------------------------------------------

def _warn_curl_fallback(reason):
    global _curl_warned
    with _curl_warn_lock:
        if _curl_warned:
            return
        _curl_warned = True
    logging.warning("curl_cffi unavailable (%s); falling back to requests", reason)


def _new_requests_session():
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=4, pool_maxsize=WORKERS_PER_SOURCE * 2)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers["User-Agent"] = random.choice(USER_AGENTS)
    return session


# Returns ("curl"|"requests", client) for this thread.
def get_http_client():
    if not hasattr(_local, "http_client"):
        if CURL_CFFI_AVAILABLE:
            try:
                _local.http_client = ("curl", curl_requests.Session(impersonate="chrome"))
            except Exception as error:
                _warn_curl_fallback(type(error).__name__)
                _local.http_client = ("requests", _new_requests_session())
        else:
            _warn_curl_fallback("not installed")
            _local.http_client = ("requests", _new_requests_session())
    return _local.http_client


def _is_curl_error(error):
    return (
        type(error).__module__.split(".")[0] == "curl_cffi"
        or type(error).__name__ in ("RequestsError", "CurlError", "CurlECode")
        or isinstance(error, (TimeoutError, ConnectionError, OSError))
    )


def _retry_after_seconds(value):
    value = (value or "").strip()
    if not value:
        return None
    try:
        if value.isdigit():
            return float(value)
        when = parsedate_to_datetime(value)
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())
    except Exception:
        return None


def _looks_html_blocked(content, headers):
    if not content:
        return False
    content_type = ((headers.get("Content-Type") if headers else "") or "").lower()
    if content_type and "html" not in content_type:
        return False
    head = content[:1000].lower()
    if b"<html" not in head and b"<!doctype html" not in head:
        return False
    return looks_blocked(content)


# Downloads a URL with the required retry policy.
# Returns (bytes or None, last status, error kind: None|"offline"|"stopped").
def fetch(url, throttle, domain=None, purpose="article"):
    status = None
    attempts = MAX_RETRIES + 1
    for attempt in range(1, attempts + 1):
        if STOP.is_set() or SHUTDOWN.is_set():
            return None, status, "stopped"
        wait_until_online()
        if STOP.is_set() or SHUTDOWN.is_set():
            return None, status, "stopped"
        throttle.wait()
        wait_until_online()
        if OFFLINE.is_set():
            continue

        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        client_kind, client = get_http_client()
        try:
            response = client.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        except Exception as error:
            if client_kind == "curl":
                is_network = _is_curl_error(error)
            else:
                is_network = isinstance(error, requests.RequestException)
            if not is_network:
                raise
            status = None
            if note_network_error():
                logging.warning("Fetch paused for %s (attempt %d): offline (%s)",
                                url, attempt, type(error).__name__)
                return None, status, "offline"
            if domain:
                domain.note_plain_result(False, counted=(purpose == "article"))
            logging.warning("Fetch error for %s (attempt %d/%d): %s: %s",
                            url, attempt, attempts, type(error).__name__,
                            str(error).splitlines()[0] if str(error) else "")
            if attempt < attempts:
                if STOP.wait(min(BACKOFF_BASE * attempt, 30)):
                    return None, status, "stopped"
            continue

        status = response.status_code
        if status == 200:
            if purpose == "article" and _looks_html_blocked(response.content, response.headers):
                if domain:
                    domain.note_plain_result(False, counted=True)
                    domain.note_block(403)
                logging.info("Plain HTTP 200 looks like a bot-check/consent page; "
                             "treating as 403: %s", url)
                return None, 403, None
            if domain:
                domain.note_plain_result(True, counted=(purpose == "article"))
            return response.content, status, None

        if domain:
            counted = purpose == "article" and status not in (404, 410)
            domain.note_plain_result(False, counted=counted)
            if status in (403, 429) or status >= 500:
                domain.note_block(status)

        if status in (404, 410):
            logging.info("HTTP %s for %s (attempt %d) - terminal", status, url, attempt)
            return None, status, None
        if status in NO_RETRY_BLOCK_STATUSES:
            logging.info("HTTP %s for %s (attempt %d) - not retried", status, url, attempt)
            return None, status, None
        if purpose.startswith("discovery") and 400 <= status < 500:
            logging.info("HTTP %s for discovery URL %s (attempt %d) - not retried",
                         status, url, attempt)
            return None, status, None
        if status == 429 or status >= 500:
            retry_after = _retry_after_seconds(response.headers.get("Retry-After"))
            delay = retry_after if retry_after is not None else BACKOFF_BASE * attempt
            if attempt < attempts:
                logging.info("HTTP %s for %s (attempt %d/%d); retrying in %.1fs",
                             status, url, attempt, attempts, min(delay, 30))
                if STOP.wait(min(delay, 30)):
                    return None, status, "stopped"
            else:
                logging.info("HTTP %s for %s (attempt %d/%d); attempts exhausted",
                             status, url, attempt, attempts)
            continue
        logging.info("HTTP %s for %s (attempt %d) - not retried", status, url, attempt)
        return None, status, None
    return None, status, None


# Detects tiny pages or bot-check titles so a useless browser result isn't accepted.
def looks_blocked(html):
    text = html if isinstance(html, str) else html.decode("utf-8", "ignore")
    if len(text) < 500:
        return True
    match = re.search(r"<title[^>]*>(.*?)</title>", text[:6000], re.I | re.S)
    title = match.group(1).lower() if match else ""
    return any(marker in title for marker in BLOCK_TITLE_MARKERS)


# -----------------------------------------------------------------------------
# Persistent browsers (one driver/context per worker thread)
# -----------------------------------------------------------------------------

def _first_line(error):
    message = getattr(error, "msg", None) or str(error) or type(error).__name__
    return message.splitlines()[0]


def _is_outage_text(text):
    upper = text.upper()
    return any(marker in upper for marker in OUTAGE_MARKERS)


def _chrome_ua_for(version):
    major = str(version or "").split(".")[0]
    if not major.isdigit():
        return BROWSER_USER_AGENT
    return ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            f"(KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36")


def _get_selenium_driver():
    driver = getattr(_local, "selenium_driver", None)
    if driver is not None:
        return driver
    options = ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(f"--user-agent={BROWSER_USER_AGENT}")   # CDP refines this below
    options.page_load_strategy = "eager"
    options.add_experimental_option("prefs", {
        "profile.managed_default_content_settings.images": 2,
    })
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(BROWSER_TIMEOUT)
    # Use the real Chrome major version in the UA (avoids a fixed-version mismatch).
    try:
        driver.execute_cdp_cmd("Network.setUserAgentOverride", {
            "userAgent": _chrome_ua_for(driver.capabilities.get("browserVersion", "")),
        })
    except Exception:
        pass
    _local.selenium_driver = driver
    _local.selenium_pages = 0
    with _browser_registry_lock:
        _selenium_registry.add(driver)
    return driver


def _close_selenium_driver():
    driver = getattr(_local, "selenium_driver", None)
    _local.selenium_driver = None
    _local.selenium_pages = 0
    if driver is not None:
        with _browser_registry_lock:
            _selenium_registry.discard(driver)
        try:
            driver.quit()
        except Exception:
            pass


# Returns (html or None, "ok"|"fail"|"offline").
def selenium_fetch(url):
    if getattr(_local, "selenium_broken", False):
        return None, "fail"
    try:
        driver = _get_selenium_driver()
    except Exception as error:
        _local.selenium_broken = True
        logging.warning("Selenium driver could not start in this thread; "
                        "using Playwright only: %s", _first_line(error))
        return None, "fail"
    try:
        driver.get(url)
        deadline = time.monotonic() + BROWSER_RENDER_WAIT
        while time.monotonic() < deadline and not STOP.is_set():
            try:
                if driver.find_elements("css selector", "article, p"):
                    break
            except Exception:
                break
            time.sleep(0.2)
        html = driver.page_source
        _local.selenium_pages = getattr(_local, "selenium_pages", 0) + 1
        if _local.selenium_pages >= BROWSER_RECYCLE_EVERY:
            _close_selenium_driver()
        return html, "ok"
    except Exception as error:
        message = _first_line(error)
        if _is_outage_text(str(error)):
            logging.warning("Selenium outage signal for %s: %s", url, message)
            mark_offline()
            _close_selenium_driver()
            return None, "offline"
        logging.warning("Selenium failed for %s: %s", url, message)
        _close_selenium_driver()
        return None, "fail"


def _playwright_route(route):
    try:
        if route.request.resource_type in ("image", "font", "media"):
            route.abort()
        else:
            route.continue_()
    except Exception:
        pass


def _get_playwright_context():
    context = getattr(_local, "pw_context", None)
    if context is not None:
        return context
    playwright = sync_playwright().start()
    try:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=_chrome_ua_for(getattr(browser, "version", "")),
        )
        context.route("**/*", _playwright_route)
    except Exception:
        try:
            playwright.stop()
        except Exception:
            pass
        raise
    _local.pw = playwright
    _local.pw_browser = browser
    _local.pw_context = context
    _local.pw_pages = 0
    with _browser_registry_lock:
        _playwright_registry.add((playwright, browser, context))
    return context


def _close_playwright():
    playwright = getattr(_local, "pw", None)
    browser = getattr(_local, "pw_browser", None)
    context = getattr(_local, "pw_context", None)
    _local.pw = None
    _local.pw_browser = None
    _local.pw_context = None
    _local.pw_pages = 0
    if playwright is None:
        return
    with _browser_registry_lock:
        _playwright_registry.discard((playwright, browser, context))
    try:
        if context is not None:
            context.close()
    except Exception:
        pass
    try:
        if browser is not None:
            browser.close()
    except Exception:
        pass
    try:
        playwright.stop()
    except Exception:
        pass


# Returns (html or None, "ok"|"fail"|"offline").
def playwright_fetch(url):
    if getattr(_local, "pw_broken", False):
        return None, "fail"
    try:
        context = _get_playwright_context()
    except Exception as error:
        _local.pw_broken = True
        logging.warning("Playwright could not start in this thread; "
                        "using Selenium only: %s", _first_line(error))
        return None, "fail"
    page = None
    recycle = False
    try:
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=BROWSER_TIMEOUT * 1000)
        try:
            page.wait_for_selector("article, p", timeout=int(BROWSER_RENDER_WAIT * 1000),
                                   state="attached")
        except Exception:
            pass
        html = page.content()
        _local.pw_pages = getattr(_local, "pw_pages", 0) + 1
        recycle = _local.pw_pages >= BROWSER_RECYCLE_EVERY
        return html, "ok"
    except Exception as error:
        message = _first_line(error)
        if _is_outage_text(str(error)):
            logging.warning("Playwright outage signal for %s: %s", url, message)
            mark_offline()
            return None, "offline"
        logging.warning("Playwright failed for %s: %s", url, message)
        recycle = True
        return None, "fail"
    finally:
        if page is not None:
            try:
                page.close()
            except Exception:
                pass
        if recycle:
            _close_playwright()


# Tries each browser in BROWSER_ORDER; returns (html, "ok"|"fail"|"offline"|"stopped").
def browser_fetch(url, domain=None, purpose="article"):
    if STOP.is_set() or SHUTDOWN.is_set():
        return None, "stopped"
    if OFFLINE.is_set():
        return None, "offline"
    if not is_online():
        mark_offline()
        return None, "offline"
    wait_until_online()
    if STOP.is_set() or SHUTDOWN.is_set():
        return None, "stopped"
    if OFFLINE.is_set():
        return None, "offline"

    if domain is not None:
        with domain.browser_sem:
            domain.browser_throttle.wait()
            return _browser_fetch_inner(url, domain, purpose)
    return _browser_fetch_inner(url, domain, purpose)


def _browser_fetch_inner(url, domain, purpose):
    with _browser_sem:
        for name in BROWSER_ORDER:
            if STOP.is_set() or SHUTDOWN.is_set():
                return None, "stopped"
            wait_until_online()
            if OFFLINE.is_set():
                return None, "offline"
            if name == "selenium":
                html, kind = selenium_fetch(url)
            else:
                html, kind = playwright_fetch(url)
            if kind in ("offline", "stopped"):
                return None, kind
            if html and not looks_blocked(html):
                if domain is not None and purpose == "article":
                    domain.note_browser_result(True)
                logging.info("Browser fallback (%s) succeeded: %s", name, url)
                return html, "ok"
            logging.info("Browser fallback (%s) failed for %s", name, url)
        if domain is not None and purpose == "article":
            domain.note_browser_result(False)
        return None, "fail"


# Called on a worker thread: Playwright sync objects may only be closed there.
def close_thread_browsers():
    _close_selenium_driver()
    _close_playwright()


# A barrier makes the pool run one closer on EACH worker thread, not several on one.
def _close_after_barrier(barrier):
    try:
        barrier.wait(timeout=BROWSER_TIMEOUT * 2)
    except threading.BrokenBarrierError:
        pass
    close_thread_browsers()


def close_all_browsers():
    with _browser_registry_lock:
        drivers = list(_selenium_registry)
        contexts = list(_playwright_registry)
        _selenium_registry.clear()
        _playwright_registry.clear()
    for driver in drivers:
        try:
            driver.quit()
        except Exception:
            pass
    for playwright, browser, context in contexts:
        try:
            context.close()
        except Exception:
            pass
        try:
            browser.close()
        except Exception:
            pass
        try:
            playwright.stop()
        except Exception:
            pass
    for attr in ("selenium_driver", "selenium_pages", "pw", "pw_browser", "pw_context", "pw_pages"):
        if hasattr(_local, attr):
            setattr(_local, attr, None)


atexit.register(close_all_browsers)


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


# Safe key for untrusted lines in the URL log / ledger (never crashes resume).
def _normalize_key(url):
    try:
        return normalize_url(url)
    except Exception:
        return str(url).strip()


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
# RESUMABLE STATE: attempt ledger + sitemap discovery (append-only JSONL)
# =============================================================================

def _append_safe(path):
    # Make sure an existing JSONL file ends with a newline before appending.
    path = Path(path)
    if path.exists() and path.stat().st_size:
        with open(path, "rb+") as handle:
            handle.seek(-1, 2)
            if handle.read(1) != b"\n":
                handle.write(b"\n")
                handle.flush()
                logging.warning("Appended a missing final newline to %s", path.name)


class AttemptLedger:

    def __init__(self, path, saved_urls, source_by_url):
        self.path = Path(path)
        self.lock = threading.Lock()
        self.entries = {}
        _append_safe(self.path)
        if self.path.exists():
            with open(self.path, encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except ValueError:
                        logging.warning("Ignoring corrupt ledger line in %s", self.path.name)
                        continue
                    url = _normalize_key(str(entry.get("u", "")))
                    outcome = entry.get("r")
                    if not url or outcome not in TERMINAL_OUTCOMES | RETRYABLE_OUTCOMES:
                        continue
                    self.entries[url] = {
                        "u": url,
                        "s": entry.get("s", ""),
                        "r": outcome,
                        "n": int(entry.get("n", 1) or 1),
                        "t": int(entry.get("t", 0) or 0),
                        "route": entry.get("route", ""),
                    }
        else:
            with open(self.path, "a", encoding="utf-8"):
                pass
        self._file = open(self.path, "a", encoding="utf-8")
        self._bootstrap(saved_urls, source_by_url)

    # Older runs predate the ledger: their saved URLs are treated as "ok".
    def _bootstrap(self, saved_urls, source_by_url):
        now = int(time.time())
        lines = []
        with self.lock:
            for url in saved_urls:
                key = _normalize_key(url)
                if not key or key in self.entries:
                    continue
                entry = {
                    "u": key,
                    "s": source_by_url.get(key, ""),
                    "r": "ok",
                    "n": 1,
                    "t": now,
                    "route": "",
                }
                self.entries[key] = entry
                lines.append(json.dumps(entry, ensure_ascii=False) + "\n")
            if lines:
                self._file.write("".join(lines))
                self._file.flush()
                logging.info("Bootstrapped %d saved URLs into %s",
                             len(lines), self.path.name)

    def count(self):
        with self.lock:
            return len(self.entries)

    # Checked before a URL is submitted, before throttling/network/browser work.
    def should_skip(self, url):
        key = _normalize_key(url)
        with self.lock:
            entry = self.entries.get(key)
        if not entry:
            return False
        outcome = entry.get("r")
        if outcome in TERMINAL_OUTCOMES:
            return True
        if outcome in RETRYABLE_OUTCOMES:
            if not RETRY_FAILED_ON_RESUME:
                return True
            return int(entry.get("n", 1)) >= MAX_TOTAL_ATTEMPTS
        return False

    def record(self, url, source, outcome, route=""):
        if outcome not in TERMINAL_OUTCOMES | RETRYABLE_OUTCOMES:
            return
        if SHUTDOWN.is_set() or OFFLINE.is_set():
            return
        key = _normalize_key(url)
        with self.lock:
            previous = self.entries.get(key)
            if outcome in RETRYABLE_OUTCOMES:
                attempts = int(previous.get("n", 0)) + 1 if previous else 1
            else:
                attempts = 1
            entry = {
                "u": key,
                "s": source,
                "r": outcome,
                "t": int(time.time()),
                "route": route,
                "n": attempts,
            }
            self.entries[key] = entry
            self._file.write(json.dumps(entry, ensure_ascii=False) + "\n")
            self._file.flush()

    def close(self):
        with self.lock:
            try:
                self._file.flush()
                self._file.close()
            except Exception:
                pass


class DiscoveryState:

    def __init__(self, path):
        self.path = Path(path)
        self.lock = threading.Lock()
        self.done = {}
        _append_safe(self.path)
        if self.path.exists():
            with open(self.path, encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except ValueError:
                        logging.warning("Ignoring corrupt discovery line in %s", self.path.name)
                        continue
                    sitemap = str(entry.get("sitemap", "")).strip()
                    if not sitemap:
                        continue
                    children = entry.get("children")
                    self.done[sitemap] = {
                        "lastmod": str(entry.get("lastmod", "") or ""),
                        "t": int(entry.get("t", 0) or 0),
                        "children": children if isinstance(children, list) else None,
                    }
        else:
            with open(self.path, "a", encoding="utf-8"):
                pass
        self._file = open(self.path, "a", encoding="utf-8")
        if self.done:
            logging.info("Loaded %d completed sitemap records", len(self.done))

    def should_skip(self, sitemap, lastmod):
        with self.lock:
            entry = self.done.get(sitemap)
        if not entry:
            return False
        if entry.get("lastmod", "") != (lastmod or ""):
            return False
        if lastmod:
            return True
        age = time.time() - int(entry.get("t", 0) or 0)
        return age >= SITEMAP_RECHECK_HOURS * 3600

    # Cached children let a skipped sitemap index still enqueue unprocessed children.
    def children_of(self, sitemap):
        with self.lock:
            entry = self.done.get(sitemap)
            if not entry:
                return None
            children = entry.get("children")
            return list(children) if isinstance(children, list) else None

    def record(self, sitemap, lastmod, children=None):
        with self.lock:
            entry = {
                "sitemap": sitemap,
                "lastmod": lastmod or "",
                "t": int(time.time()),
            }
            if children is not None:
                entry["children"] = [[loc, lm or ""] for loc, lm in children]
            self.done[sitemap] = {
                "lastmod": entry["lastmod"],
                "t": entry["t"],
                "children": entry.get("children"),
            }
            self._file.write(json.dumps(entry, ensure_ascii=False) + "\n")
            self._file.flush()

    def close(self):
        with self.lock:
            try:
                self._file.flush()
                self._file.close()
            except Exception:
                pass


# =============================================================================
# DISCOVERY: robots.txt, RSS/Atom feeds (phase 1), sitemaps (phase 2)
# =============================================================================

# Shared per-source handles used by discovery and article workers.
class SourceContext:

    def __init__(self, name, cfg, origin, base_host, throttle, domain, ledger, discovery):
        self.name = name
        self.cfg = cfg
        self.origin = origin
        self.base_host = base_host
        self.throttle = throttle
        self.domain = domain
        self.ledger = ledger
        self.discovery = discovery
        self.rules = None
        self._budget_lock = threading.Lock()
        self.browser_budget = BROWSER_DISCOVERY_MAX

    def take_browser_budget(self):
        with self._budget_lock:
            if self.browser_budget <= 0:
                return False
            self.browser_budget -= 1
            return True


# Plain discovery fetch, with a bounded browser retry for browser_only domains.
def discovery_fetch(url, ctx, purpose="discovery"):
    content, status, error_kind = fetch(url, ctx.throttle, ctx.domain, purpose=purpose)
    if error_kind in ("offline", "stopped"):
        return None
    blocked = status in BLOCK_STATUSES
    if content is not None and purpose != "robots" and _looks_html_blocked(content, None):
        blocked = True
    if blocked and ctx.domain.browser_only and ctx.take_browser_budget():
        logging.info("Browser discovery fallback for %s", url)
        html, kind = browser_fetch(url, ctx.domain, purpose="discovery")
        if kind == "ok" and html:
            return html
    return content


# Holds a site's robots.txt rules and the sitemap URLs it advertises.
class SiteRules:

    def __init__(self, origin, ctx):
        self.rp = RobotFileParser()
        self.sitemaps = []
        content = discovery_fetch(origin + "/robots.txt", ctx, purpose="robots")
        if content:
            try:
                self.rp.parse(content.decode("utf-8", "ignore").splitlines())
                self.rp.modified()
                self.sitemaps = list(self.rp.site_maps() or [])
            except Exception as error:
                logging.warning("Bad robots.txt for %s: %s", origin, error)
                self.rp.allow_all = True
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
def feed_urls(cfg, origin, ctx):
    candidates = list(cfg.get("feeds", []))
    advertised = []
    content = discovery_fetch(cfg["home"], ctx, purpose="discovery_html")
    if content:
        try:
            soup = make_soup(content)
            for link in soup.find_all("link", href=True):
                rel = " ".join(link.get("rel") or []).lower()
                if "alternate" in rel and (link.get("type") or "").lower() in FEED_TYPES:
                    advertised.append(urljoin(cfg["home"], link["href"]))
        except Exception as error:
            logging.warning("Could not parse homepage %s: %s", cfg["home"], error)
    candidates.extend(advertised)
    if not candidates:
        candidates.extend(origin + p for p in COMMON_FEED_PATHS)

    used = 0
    for feed_url in dict.fromkeys(candidates):
        if used >= MAX_FEEDS_PER_SOURCE or STOP.is_set() or ctx.domain.dead:
            break
        data = discovery_fetch(feed_url, ctx)
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
    if soup.find("sitemapindex"):
        kind = "index"
    elif soup.find("urlset"):
        kind = "urls"
    else:
        raise ValueError("response is not a sitemap")
    entries = []
    for node in soup.find_all(["sitemap", "url"]):
        loc = node.find("loc", recursive=False)
        if not loc or not loc.get_text(strip=True):
            continue
        lastmod = node.find("lastmod", recursive=False)
        entries.append((loc.get_text(strip=True), lastmod.get_text(strip=True) if lastmod else ""))
    return kind, entries


def _parse_lastmod(value):
    if not value:
        return None
    text = value.strip()
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text[:10], "%Y-%m-%d")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _lastmod_too_old(value):
    if not SITEMAP_MAX_AGE_DAYS:
        return False
    parsed = _parse_lastmod(value)
    if parsed is None:
        return False
    cutoff = datetime.now(timezone.utc) - timedelta(days=SITEMAP_MAX_AGE_DAYS)
    return parsed < cutoff


# PHASE 2. Walks sitemap files lazily (news sitemaps first, newest first).
def sitemap_urls(roots, ctx):
    roots = list(dict.fromkeys(roots))
    roots.sort(key=lambda u: "news" not in u.lower())
    queue = deque((url, "") for url in roots)
    fetched = 0

    while (queue and fetched < MAX_SITEMAPS_PER_SOURCE
           and not STOP.is_set() and not ctx.domain.dead):
        sitemap, parent_lastmod = queue.popleft()
        if ctx.discovery.should_skip(sitemap, parent_lastmod):
            cached_children = ctx.discovery.children_of(sitemap)
            if cached_children:
                queue.extendleft(reversed(cached_children))
                logging.info("Skipping consumed sitemap %s; re-queued %d cached children",
                             sitemap, len(cached_children))
            else:
                logging.info("Skipping already consumed sitemap %s", sitemap)
            continue
        content = discovery_fetch(sitemap, ctx)
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
            children = []
            for loc, child_lastmod in entries:
                if any(word in loc.lower() for word in SITEMAP_SKIP_WORDS):
                    continue
                if _lastmod_too_old(child_lastmod):
                    logging.info("Skipping old child sitemap %s (lastmod %s)",
                                 loc, child_lastmod)
                    continue
                children.append((loc, child_lastmod))
            queue.extendleft(reversed(children))
            ctx.discovery.record(sitemap, parent_lastmod, children=children)
        else:
            for loc, entry_lastmod in entries:
                if _lastmod_too_old(entry_lastmod):
                    continue
                yield loc
            # Reached only when this sitemap was fully consumed, not when abandoned.
            ctx.discovery.record(sitemap, parent_lastmod)


# Lazy stream of de-duplicated candidates: ALL feeds first, then sitemaps only if still needed.
# Yields the ORIGINAL URL; normalize_url() is used only as the dedupe/ledger/store key.
def iter_candidates(cfg, origin, base_host, ctx):
    pattern = re.compile(cfg["pattern"]) if cfg.get("pattern") else None
    strict_pattern = bool(cfg.get("strict_pattern", False))
    rules = ctx.rules
    seen = set()

    def emit(urls):
        for url in urls:
            if not candidate_ok(url, base_host):
                continue
            key = _normalize_key(url)
            if key in seen:
                continue
            seen.add(key)
            yield url

    phases = []
    if USE_FEEDS:
        phases.append(lambda: feed_urls(cfg, origin, ctx))
    if USE_SITEMAPS:
        roots = rules.sitemaps + cfg.get("sitemaps", [])
        if not roots:
            roots = [origin + p for p in ("/sitemap.xml", "/news-sitemap.xml", "/sitemap_index.xml")]
        phases.append(lambda: sitemap_urls(roots, ctx))

    for make_phase in phases:                     # a phase starts only after the previous one is exhausted
        buffered = []
        for url in make_phase():
            if ctx.domain.dead:
                return
            if not pattern or pattern.search(url):
                yield from emit([url])
            elif strict_pattern:
                continue
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


# Fetches one URL (plain, then persistent browsers if blocked) and extracts it.
# Returns (row or None, reason, route) where route is "plain" or "browser".
def process_url(url, ctx):
    key = _normalize_key(url)
    if STOP.is_set() or SHUTDOWN.is_set():
        return None, "stopped", ""
    if ctx.ledger.should_skip(key):
        return None, "skipped", ""
    if ctx.domain.dead:
        return None, "dead", ""
    if not ctx.rules.allowed(url):
        ctx.ledger.record(key, ctx.name, "robots", "")
        return None, "robots", ""

    content = None
    status = None
    route = ctx.domain.route_for_url()
    if route == "dead":
        return None, "dead", ""
    if route == "browser" and not BROWSER_FALLBACK:
        route = "plain"

    if route == "browser":
        if OFFLINE.is_set():
            return None, "offline", "browser"
        html, kind = browser_fetch(url, ctx.domain, purpose="article")
        if kind == "offline":
            return None, "offline", "browser"
        if kind == "stopped":
            return None, "stopped", ""
        if html:
            content = html
            status = 200
        else:
            status = 403          # browser-only domain: blocked after every route
    else:
        route = "plain"
        content, status, error_kind = fetch(url, ctx.throttle, ctx.domain, purpose="article")
        if error_kind == "offline":
            return None, "offline", "plain"
        if error_kind == "stopped":
            return None, "stopped", ""
        if content is None and BROWSER_FALLBACK and (
                status in BLOCK_STATUSES or status is None):
            if OFFLINE.is_set():
                return None, "offline", "plain"
            html, kind = browser_fetch(url, ctx.domain, purpose="article")
            if kind == "offline":
                return None, "offline", "browser"
            if kind == "stopped":
                return None, "stopped", ""
            route = "browser"
            if html:
                content = html
                status = 200

    if content is None:
        if SHUTDOWN.is_set():
            return None, "stopped", ""
        if ctx.domain.dead:
            return None, "dead", ""
        if status in (404, 410):
            reason = f"http_{status}"
        elif status in BLOCK_STATUSES:
            reason = "blocked"
        else:
            reason = "error"
        logging.info("Article failed via %s for %s (status=%s, reason=%s)",
                     route, url, status, reason)
        if reason in ("http_404", "http_410"):
            ctx.ledger.record(key, ctx.name, reason, route)
            return None, reason, route
        if ctx.domain.note_route_failure():
            return None, "dead", ""
        ctx.ledger.record(key, ctx.name, reason, route)
        return None, reason, route

    if SHUTDOWN.is_set():
        return None, "stopped", ""
    try:
        data, reason = extract_article(content, url)
    except Exception as error:
        logging.warning("Extraction crashed for %s: %s", url, error)
        ctx.domain.note_route_success()
        ctx.ledger.record(key, ctx.name, "error", route)
        return None, "error", route
    ctx.domain.note_route_success()
    if not data:
        ctx.ledger.record(key, ctx.name, reason, route)
        return None, reason, route
    data.update(source=ctx.name, url=url)
    return data, None, route


# Ensures an interrupted worker releases its own (thread-bound) browsers immediately.
def process_url_with_cleanup(url, ctx):
    try:
        return process_url(url, ctx)
    finally:
        if SHUTDOWN.is_set() or STOP.is_set():
            close_thread_browsers()


# =============================================================================
# STORAGE (thread-safe, incremental, resumable)
# =============================================================================

class Store:

    # Hard append-only storage: existing CSV / URL log are never truncated or recreated.
    def __init__(self, csv_path, urls_path, resume):
        self.csv_path = Path(csv_path)
        self.urls_path = Path(urls_path)
        self.lock = threading.Lock()
        self.urls = set()
        self.source_by_url = {}
        self.counts = Counter()
        self.total = 0
        if not resume:
            logging.warning("RESUME=False requested, but existing files are append-only; "
                            "resuming instead of truncating")

        size = self.csv_path.stat().st_size if self.csv_path.exists() else 0
        if size:
            self._ensure_final_newline()
            with open(self.csv_path, newline="", encoding="utf-8-sig") as handle:
                for row in csv.DictReader(handle):
                    url = (row.get("url") or "").strip()
                    source = row.get("source") or ""
                    if not url:
                        continue
                    key = _normalize_key(url)
                    self.urls.add(key)
                    self.source_by_url[key] = source
                    self.counts[source] += 1
                    self.total += 1
            logging.info("Reloaded %d CSV rows for %d sources", self.total, len(self.counts))

        self._file = open(self.csv_path, "a", newline="", encoding="utf-8-sig")
        self._writer = csv.DictWriter(self._file, fieldnames=CSV_FIELDS)
        if size == 0:
            self._writer.writeheader()
            self._file.flush()
            logging.info("Created CSV header in %s", self.csv_path.name)

        if not self.urls_path.exists():
            with open(self.urls_path, "a", encoding="utf-8"):
                pass
        _append_safe(self.urls_path)
        with open(self.urls_path, encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                url = line.strip().lstrip("\ufeff")
                if url:
                    self.urls.add(_normalize_key(url))
        self._urls_file = open(self.urls_path, "a", encoding="utf-8")

    # A crash can leave the CSV without its final newline; append exactly one.
    def _ensure_final_newline(self):
        with open(self.csv_path, "rb+") as handle:
            handle.seek(-1, 2)
            if handle.read(1) != b"\n":
                handle.write(b"\n")
                handle.flush()
                logging.warning("CSV %s did not end with a newline; appended one",
                                self.csv_path.name)

    def seen(self, url):
        return _normalize_key(url) in self.urls

    # Saves one row (flushed immediately); returns False for duplicates or once the target is hit.
    def add(self, row):
        with self.lock:
            key = _normalize_key(row["url"])
            if key in self.urls or self.total >= TARGET_TOTAL_ARTICLES:
                return False
            self.urls.add(key)
            self.source_by_url[key] = row["source"]
            self.counts[row["source"]] += 1
            self.total += 1
            self._writer.writerow({k: row.get(k, "") for k in CSV_FIELDS})
            self._file.flush()
            self._urls_file.write(row["url"] + "\n")
            self._urls_file.flush()
            if self.total >= TARGET_TOTAL_ARTICLES:
                STOP.set()
            return True

    def close(self):
        with self.lock:
            for handle in (self._file, self._urls_file):
                try:
                    handle.flush()
                    handle.close()
                except Exception:
                    pass


# =============================================================================
# PER-SOURCE WORKER
# =============================================================================

# Runs discovery + scraping for one source until its cap/target is met or candidates run out.
def _scrape_source(name, cfg, overall, store, ledger, discovery):
    stats = Counter()
    need = MAX_ARTICLES_PER_SOURCE - store.counts[name]
    if need <= 0 or STOP.is_set():
        logging.info("%s already at its cap or run stopped; skipping (%d saved)",
                     name, store.counts[name])
        return stats

    parsed = urlparse(cfg["home"])
    origin = f"{parsed.scheme}://{parsed.netloc}"
    base_host = (parsed.hostname or "").lower().removeprefix("www.")
    domain = get_domain(base_host)
    if domain.dead:
        logging.warning("%s skipped: domain marked dead in this run", name)
        return stats
    throttle = Throttle(REQUEST_DELAY, domain)

    ctx = SourceContext(name, cfg, origin, base_host, throttle, domain, ledger, discovery)
    ctx.rules = SiteRules(origin, ctx)
    candidates = iter_candidates(cfg, origin, base_host, ctx)

    exhausted = False
    pending = set()

    with ThreadPoolExecutor(WORKERS_PER_SOURCE, thread_name_prefix=name[:6]) as pool:
        while True:
            while (
                not exhausted
                and not STOP.is_set()
                and not domain.dead
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
                key = _normalize_key(url)
                if store.seen(key) or ledger.should_skip(key):
                    stats["skipped"] += 1
                    continue
                if domain.dead:
                    exhausted = True
                    break
                pending.add(pool.submit(process_url_with_cleanup, url, ctx))

            if domain.dead:
                logging.warning("[%s] domain is dead; stopping this source for this run", name)
                for future in pending:
                    future.cancel()
                break
            if not pending:
                break

            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                if future.cancelled():
                    continue
                stats["tried"] += 1
                try:
                    row, reason, route = future.result()
                except Exception:
                    logging.exception("Worker crashed")
                    row, reason, route = None, "error", ""
                if route in ("plain", "browser"):
                    stats["try_" + route] += 1

                if row and stats["ok"] < need and store.add(row):
                    stats["ok"] += 1
                    overall.update(1)
                    ledger.record(_normalize_key(row["url"]), name, "ok", route)
                    if route in ("plain", "browser"):
                        stats["route_" + route] += 1
                elif not row and reason not in ("skipped", "dead", "offline", "stopped"):
                    stats[reason] += 1

            overall.set_postfix(sources_active=threading.active_count(), refresh=True)
            if stats["ok"] >= need or STOP.is_set() or domain.dead:
                for future in pending:
                    future.cancel()
                if stats["ok"] >= need or STOP.is_set():
                    break

        if pending:
            wait(pending, timeout=BROWSER_TIMEOUT * 2)
        barrier = threading.Barrier(WORKERS_PER_SOURCE)
        cleanup = [pool.submit(_close_after_barrier, barrier)
                   for _ in range(WORKERS_PER_SOURCE)]
        wait(cleanup, timeout=BROWSER_TIMEOUT * 2 + 10)

    tqdm.write(
        f"[{name}] saved {stats['ok']} (tried {stats['tried']}, skipped {stats['skipped']}, "
        f"blocked {stats['blocked']}, short {stats['short']}, empty {stats['empty']}, "
        f"error {stats['error']}, plain {stats['route_plain']}/{stats['try_plain']} tried, "
        f"browser {stats['route_browser']}/{stats['try_browser']} tried)"
    )
    logging.info("%s finished: %s", name, dict(stats))
    return stats


# Discovery can use a browser in the source thread too; always close thread-local ones.
def scrape_source(name, cfg, overall, store, ledger, discovery):
    try:
        return _scrape_source(name, cfg, overall, store, ledger, discovery)
    finally:
        close_thread_browsers()


# =============================================================================
# MAIN
# =============================================================================

def _handle_stop_signal(signum, frame):
    STOP.set()
    SHUTDOWN.set()
    logging.warning("Signal %s received; stopping gracefully", signum)


def _install_signal_handlers():
    for signal_name in ("SIGINT", "SIGTERM"):
        signum = getattr(signal, signal_name, None)
        if signum is None:
            continue
        try:
            signal.signal(signum, _handle_stop_signal)
        except (ValueError, OSError):
            pass


def main():
    _install_signal_handlers()
    logging.info("=== RUN START ===")
    load_domain_state()

    store = Store(OUTPUT_CSV, URLS_LOG, RESUME)
    ledger = AttemptLedger(ATTEMPT_LEDGER, store.urls, store.source_by_url)
    discovery = DiscoveryState(DISCOVERY_STATE)

    capped = [f"{name}={store.counts[name]}"
              for name in SOURCES if store.counts[name] >= MAX_ARTICLES_PER_SOURCE]
    startup_line = (
        f"RESUME: {store.total} articles in CSV, {ledger.count()} URLs in ledger, "
        f"per-source saved counts for sources already at cap: "
        f"{', '.join(capped) if capped else 'none'}"
    )
    print(startup_line)
    logging.info(startup_line)

    if store.total >= TARGET_TOTAL_ARTICLES:
        STOP.set()
    names = list(SOURCES)

    overall = tqdm(
        total=TARGET_TOTAL_ARTICLES, initial=min(store.total, TARGET_TOTAL_ARTICLES),
        desc="TOTAL", unit="art",
        bar_format="{desc} {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} "
                   "[{elapsed}<{remaining}, {rate_fmt}]{postfix}",
    )

    monitor = threading.Thread(target=network_monitor, name="netmon", daemon=True)
    monitor.start()

    results = {}
    try:
        with ThreadPoolExecutor(MAX_PARALLEL_SOURCES, thread_name_prefix="src") as pool:
            futures = {
                pool.submit(scrape_source, n, SOURCES[n], overall, store, ledger, discovery): n
                for n in names
            }
            for future, name in futures.items():
                try:
                    results[name] = future.result()
                except Exception:
                    logging.exception("Source %s crashed", name)
                    results[name] = Counter()
    except KeyboardInterrupt:
        STOP.set()
        SHUTDOWN.set()
        logging.warning("Interrupted by user; progress is saved, rerun to resume.")
        print("\nInterrupted. Progress saved; rerun to resume.")
    finally:
        STOP.set()
        SHUTDOWN.set()
        overall.close()
        save_domain_state(force=True)
        close_all_browsers()
        ledger.close()
        discovery.close()
        store.close()

    print("\n" + "=" * 90)
    print(f"{'SOURCE':<24}{'SAVED':>8}{'TRIED':>8}{'BLOCKED':>9}{'SHORT':>7}{'ERR':>6}"
          f"{'PLAIN':>8}{'BROWSER':>9}")
    for name in sorted(names, key=lambda n: -store.counts[n]):
        s = results.get(name, Counter())
        if store.counts[name] == 0 and s["tried"] == 0:
            continue
        print(
            f"{name:<24}{store.counts[name]:>8}{s['tried']:>8}{s['blocked']:>9}"
            f"{s['short'] + s['empty']:>7}{s['error']:>6}"
            f"{s['route_plain']:>8}{s['route_browser']:>9}"
        )
    print(f"\nTotal articles in {OUTPUT_CSV}: {store.total}")


if __name__ == "__main__":
    main()