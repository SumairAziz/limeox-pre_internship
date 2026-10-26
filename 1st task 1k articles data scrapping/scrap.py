#!/usr/bin/env python3
"""
Combined news URL discovery + article scraper.

Pipeline (runs in parallel, one thread per source, N worker threads inside each):
  robots.txt -> sitemaps (newest first) + RSS/Atom feeds -> lazy candidate URLs
  -> fetch with requests (retries, UA rotation, throttling)
  -> blocked pages retried with Selenium, then Playwright
  -> trafilatura extraction -> CSV (resumable)

Discovery is LAZY: URLs are pulled from sitemaps only as needed, so if many
articles are blocked/empty the scraper keeps pulling more candidates until the
per-source SUCCESS target is met (or MAX_CANDIDATES_PER_SOURCE is reached).

Install:
    pip install requests feedparser trafilatura tqdm selenium playwright
    playwright install chromium
    (Selenium also needs Google Chrome installed; Selenium 4.6+ fetches the driver itself.)
"""

# ---- Standard library imports ----
import csv                      # read/write the output CSV
import gzip                     # decompress .xml.gz sitemaps
import json                     # parse trafilatura's JSON output
import logging                  # write diagnostics to a log file
import random                   # random delays and user-agent rotation
import re                       # compile and match URL patterns and page titles
import threading                # locks, thread-local sessions, semaphores
import time                     # sleeping and monotonic clock for throttling
import xml.etree.ElementTree as ET   # parse sitemap XML
from collections import Counter, deque  # Counter for stats, deque for the sitemap queue
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED  # thread pools
from html.parser import HTMLParser       # find <link> feed tags in homepage HTML
from pathlib import Path                 # file path handling
from urllib.parse import urljoin, urlparse, urldefrag, urlunparse  # URL manipulation
from urllib.robotparser import RobotFileParser  # parse robots.txt rules

# ---- Third-party imports ----
import feedparser               # parse RSS/Atom feeds into entries
import requests                 # HTTP client
import trafilatura              # extract clean article text from HTML
from requests.adapters import HTTPAdapter  # tune connection pooling
from tqdm import tqdm           # progress bars
from selenium import webdriver  # drive a real Chrome browser (fallback #1)
from selenium.webdriver.chrome.options import Options as ChromeOptions  # Chrome launch flags
from playwright.sync_api import sync_playwright  # drive Chromium via Playwright (fallback #2)

# =============================================================================
# CONFIGURATION  (everything you'd want to tweak lives here)
# =============================================================================

# --- Targets ---------------------------------------------------------------
TARGET_ARTICLES_PER_SOURCE = 1000   # SUCCESSFUL articles wanted per source
MAX_CANDIDATES_PER_SOURCE = 8000    # hard cap on URLs tried per source (safety net)
MIN_WORDS = 150                     # reject extractions shorter than this

# --- Parallelism -----------------------------------------------------------
MAX_PARALLEL_SOURCES = 8            # sources processed at the same time
WORKERS_PER_SOURCE = 6              # concurrent article fetches inside one source
REQUEST_DELAY = 0.4                 # avg seconds between requests to the SAME site

# --- Network / anti-block --------------------------------------------------
REQUEST_TIMEOUT = 25                # seconds before a plain request is abandoned
MAX_RETRIES = 3                     # retries on 403/429/5xx/timeouts
BACKOFF_BASE = 2.0                  # seconds, multiplied by attempt number
RESPECT_ROBOTS = True               # skip URLs disallowed by robots.txt

# --- Browser fallback (for blocked URLs) -----------------------------------
BROWSER_FALLBACK = True             # retry blocked pages with a real browser
BROWSER_ORDER = ("selenium", "playwright")  # order the browsers are tried in
BROWSER_CONCURRENCY = 2             # max simultaneous headless browsers
BROWSER_TIMEOUT = 30                # seconds to wait for a browser page load
BROWSER_RENDER_WAIT = 2.5           # seconds to let JavaScript render after load

# --- Discovery -------------------------------------------------------------
USE_SITEMAPS = True                 # discover URLs through sitemaps
USE_FEEDS = True                    # discover URLs through RSS/Atom feeds
MAX_SITEMAPS_PER_SOURCE = 80        # max sitemap files fetched per source
MAX_FEEDS_PER_SOURCE = 12           # max feeds parsed per source
ALLOW_NON_PATTERN_URLS = True       # fall back to URLs not matching 'pattern'

# --- Files -----------------------------------------------------------------
OUTPUT_CSV = "news_articles2.csv"    # where scraped articles are saved
URLS_LOG = "news_urls_scraped2.txt"  # every successfully scraped URL
LOG_FILE = "scraping2.log"           # diagnostics (kept out of the console)
RESUME = True                       # continue from existing OUTPUT_CSV

# --- Sources ---------------------------------------------------------------
# home:     site root / section page (robots.txt is read from its origin)
# pattern:  regex marking real article URLs (preferred first; see above flag)
# sitemaps: optional extra sitemap URLs
SOURCES = {
    "BBC": {
        "home": "https://www.bbc.com/news",
        "pattern": r"/news/(articles/|[a-z\-]+-\d{6,})",
    },
    "The Guardian": {
        "home": "https://www.theguardian.com/international",
        "pattern": r"/\d{4}/[a-z]{3}/\d{2}/",
    },
    "NPR": {
        "home": "https://www.npr.org",
        "pattern": r"/\d{4}/\d{2}/\d{2}/",
    },
    "France 24": {
        "home": "https://www.france24.com/en",
        "pattern": r"/en/.+/\d{8}-",
    },
    "CBS News": {
        "home": "https://www.cbsnews.com",
        "pattern": r"/news/",
    },
    "The Verge": {
        "home": "https://www.theverge.com",
        "pattern": r"/\d{5,}/|/\d{4}/\d{1,2}/\d{1,2}/",
    },
}

# Browser-like identities; one is picked at random per request to look less bot-like.
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0",
]

# URL path segments that indicate non-article pages (video hubs, tag pages, etc.).
EXCLUDED_SEGMENTS = {
    "live", "video", "videos", "gallery", "galleries", "tag", "tags",
    "author", "authors", "topic", "topics", "podcasts", "podcast",
    "profile", "search", "newsletters", "subscribe",
}
# File extensions that are never articles (media, data, assets).
EXCLUDED_EXTENSIONS = (
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".pdf", ".mp3", ".mp4",
    ".xml", ".rss", ".json", ".css", ".js",
)
# Sitemap files whose names contain these words list non-article content; skip them.
SITEMAP_SKIP_WORDS = ("video", "image", "tag", "author", "topic", "podcast", "gallery")
# HTTP statuses that usually mean "you're being blocked or rate-limited".
BLOCK_STATUSES = {401, 403, 407, 429, 451, 503}
# Words in a page <title> that reveal a bot-check or "access denied" page.
BLOCK_TITLE_MARKERS = (
    "access denied", "just a moment", "attention required", "captcha",
    "forbidden", "are you a robot", "verify you are human",
)
# MIME types that <link rel="alternate"> uses to advertise feeds.
FEED_TYPES = {
    "application/rss+xml", "application/atom+xml", "application/rdf+xml",
    "application/xml", "text/xml",
}
# Guessed feed locations, tried only when the homepage advertises none.
COMMON_FEED_PATHS = [
    "/feed", "/feed/", "/rss", "/rss.xml", "/feed.xml", "/atom.xml",
    "/rss/index.xml", "/xml/rss/all.xml", "/rss/all",
]
# Column order of the output CSV.
CSV_FIELDS = ["source", "title", "url", "date", "author", "content"]

# =============================================================================
# SETUP
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(threadName)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8")],
)
for noisy in ("trafilatura", "htmldate", "urllib3", "courlan", "selenium"):
    logging.getLogger(noisy).setLevel(logging.ERROR)

csv.field_size_limit(2**31 - 1)     # allow very long article text in CSV cells
_local = threading.local()          # holds one requests.Session per thread
_browser_sem = threading.Semaphore(BROWSER_CONCURRENCY)  # caps simultaneous browsers


# =============================================================================
# NETWORK HELPERS
# =============================================================================

# Per-site rate limiter shared by all threads of one source; spaces request start times apart.
class Throttle:

    # Stores the average delay and the lock/time slot used to coordinate threads.
    def __init__(self, delay):
        self.delay = delay
        self.lock = threading.Lock()
        self.next_slot = 0.0

    # Reserves the next request time slot (with random jitter) and sleeps until it arrives.
    def wait(self):
        with self.lock:
            now = time.monotonic()
            slot = max(now, self.next_slot)
            self.next_slot = slot + self.delay * random.uniform(0.7, 1.3)
        if slot > now:
            time.sleep(slot - now)


# Returns this thread's own requests.Session, creating it with a larger connection pool on first use.
def get_session():
    if not hasattr(_local, "session"):
        s = requests.Session()
        adapter = HTTPAdapter(pool_connections=4, pool_maxsize=WORKERS_PER_SOURCE * 2)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _local.session = s
    return _local.session


# Downloads a URL with requests, retrying blocks and errors with backoff; returns (bytes or None, last status).
def fetch(url, throttle):
    status = None
    for attempt in range(1, MAX_RETRIES + 2):
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


# Loads a page in headless Chrome via Selenium (runs JavaScript) and returns its HTML, or None on failure.
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


# Loads a page in headless Chromium via Playwright (runs JavaScript) and returns its HTML, or None on failure.
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


# Detects tiny pages or bot-check/"access denied" titles so a useless browser result isn't accepted.
def looks_blocked(html):
    text = html if isinstance(html, str) else html.decode("utf-8", "ignore")
    if len(text) < 500:
        return True
    match = re.search(r"<title[^>]*>(.*?)</title>", text[:6000], re.I | re.S)
    title = match.group(1).lower() if match else ""
    return any(marker in title for marker in BLOCK_TITLE_MARKERS)


# Tries each browser in BROWSER_ORDER (Selenium, then Playwright) and returns the first good page, or None.
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
# URL HELPERS
# =============================================================================

# Strips fragment, query string and trailing slash so the same article always produces the same key.
def normalize_url(url):
    url, _ = urldefrag(url.strip())
    p = urlparse(url)
    return urlunparse((p.scheme, p.netloc.lower(), p.path.rstrip("/"), "", "", ""))


# Cheap filter that accepts only plausible article URLs on the source's own site (no tags, media, homepages).
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
# DISCOVERY: robots.txt, sitemaps, feeds
# =============================================================================

# Holds a site's robots.txt rules and the sitemap URLs it advertises.
class SiteRules:

    # Downloads and parses robots.txt (allowing everything if it is missing) and records its sitemaps.
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

    # Returns True if the URL may be fetched (always True when RESPECT_ROBOTS is off).
    def allowed(self, url):
        return (not RESPECT_ROBOTS) or self.rp.can_fetch("*", url)


# Parses sitemap XML (gzip-aware) and returns its type ('index' or 'urls') with a list of (url, lastmod).
def parse_sitemap(content):
    if content[:2] == b"\x1f\x8b":
        content = gzip.decompress(content)
    root = ET.fromstring(content)
    kind = "index" if root.tag.split("}")[-1].lower() == "sitemapindex" else "urls"
    entries = []
    for element in root:
        loc = lastmod = ""
        for child in element:
            tag = child.tag.split("}")[-1].lower()
            if tag == "loc" and child.text:
                loc = child.text.strip()
            elif tag == "lastmod" and child.text:
                lastmod = child.text.strip()
        if loc:
            entries.append((loc, lastmod))
    return kind, entries


# HTML parser that collects feed links from <link rel="alternate" type="application/rss+xml"> tags.
class FeedLinkFinder(HTMLParser):

    # Remembers the page URL (to resolve relative links) and starts an empty feed list.
    def __init__(self, base_url):
        super().__init__()
        self.base_url = base_url
        self.feeds = []

    # Called for every opening tag; records the href of any <link> that advertises a feed.
    def handle_starttag(self, tag, attrs):
        if tag != "link":
            return
        a = {k.lower(): (v or "") for k, v in attrs}
        if "alternate" in a.get("rel", "").lower() and a.get("type", "").lower() in FEED_TYPES:
            if a.get("href"):
                self.feeds.append(urljoin(self.base_url, a["href"]))


# Finds feeds on the homepage (or guesses common paths) and yields the article links inside them.
def feed_urls(home, origin, throttle):
    candidates = []
    content, _ = fetch(home, throttle)
    if content:
        finder = FeedLinkFinder(home)
        finder.feed(content.decode("utf-8", "ignore"))
        candidates.extend(finder.feeds)
    if not candidates:
        candidates.extend(origin + p for p in COMMON_FEED_PATHS)

    used = 0
    for feed_url in dict.fromkeys(candidates):
        if used >= MAX_FEEDS_PER_SOURCE:
            break
        data, _ = fetch(feed_url, throttle)
        if not data:
            continue
        parsed = feedparser.parse(data)
        if not parsed.entries:
            continue
        used += 1
        for entry in parsed.entries:
            link = (entry.get("link") or "").strip()
            if link:
                yield link


# Walks sitemap files lazily (news sitemaps first, newest first) and yields page URLs as they are found.
def sitemap_urls(roots, throttle):
    roots = list(dict.fromkeys(roots))
    roots.sort(key=lambda u: "news" not in u.lower())
    queue = deque(roots)
    fetched = 0

    while queue and fetched < MAX_SITEMAPS_PER_SOURCE:
        sitemap = queue.popleft()
        content, _ = fetch(sitemap, throttle)
        fetched += 1
        if not content:
            continue
        try:
            kind, entries = parse_sitemap(content)
        except (ET.ParseError, OSError, EOFError) as error:
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


# Combines feeds and sitemaps into one lazy stream of de-duplicated candidate article URLs, pattern matches first.
def iter_candidates(cfg, origin, base_host, throttle, rules):
    pattern = re.compile(cfg["pattern"]) if cfg.get("pattern") else None
    seen = set()

    # Filters, normalizes and de-duplicates a batch of URLs, yielding the ones that pass.
    def emit(urls, pattern_only):
        for url in urls:
            if not candidate_ok(url, base_host):
                continue
            if bool(pattern and pattern.search(url)) != pattern_only and pattern:
                continue
            key = normalize_url(url)
            if key in seen:
                continue
            seen.add(key)
            yield key

    streams = []
    if USE_FEEDS:
        streams.append(lambda: feed_urls(cfg["home"], origin, throttle))
    if USE_SITEMAPS:
        roots = rules.sitemaps + cfg.get("sitemaps", [])
        if not roots:
            roots = [origin + p for p in ("/sitemap.xml", "/news-sitemap.xml", "/sitemap_index.xml")]
        streams.append(lambda: sitemap_urls(roots, throttle))

    for make_stream in streams:
        buffered = []
        for url in make_stream():
            if not pattern:
                yield from emit([url], True)
            elif pattern.search(url):
                yield from emit([url], True)
            elif ALLOW_NON_PATTERN_URLS:
                buffered.append(url)
                if len(buffered) >= 500:
                    yield from emit(buffered, False)
                    buffered = []
        if ALLOW_NON_PATTERN_URLS and buffered:
            yield from emit(buffered, False)


# =============================================================================
# EXTRACTION
# =============================================================================

# Converts raw HTML into title/date/author/content with trafilatura, or returns (None, reason) if unusable.
def extract_article(html, url):
    raw = trafilatura.extract(
        html,
        url=url,
        output_format="json",
        with_metadata=True,
        include_comments=False,
        include_tables=False,
        favor_precision=True,
    )
    if not raw:
        return None, "empty"
    data = json.loads(raw)
    title = (data.get("title") or "").strip()
    text = (data.get("text") or "").strip()
    if not title or not text:
        return None, "empty"
    if len(text.split()) < MIN_WORDS:
        return None, "short"
    return {
        "title": title,
        "date": data.get("date") or "",
        "author": data.get("author") or "",
        "content": text,
    }, None


# Fetches one URL (requests, then Selenium/Playwright if blocked) and extracts it; returns (row or None, failure reason).
def process_url(url, source, throttle, rules):
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

# Thread-safe CSV writer that also remembers saved URLs and per-source counts so runs can resume.
class Store:

    # Opens the CSV: in resume mode it reloads saved URLs/counts and appends, otherwise it starts a fresh file.
    def __init__(self, csv_path, urls_path, resume):
        self.csv_path = Path(csv_path)
        self.urls_path = Path(urls_path)
        self.lock = threading.Lock()
        self.urls = set()
        self.counts = Counter()

        if resume and self.csv_path.exists():
            with open(self.csv_path, newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    self.urls.add(normalize_url(row["url"]))
                    self.counts[row["source"]] += 1
            self._file = open(self.csv_path, "a", newline="", encoding="utf-8-sig")
            self._writer = csv.DictWriter(self._file, fieldnames=CSV_FIELDS)
        else:
            self._file = open(self.csv_path, "w", newline="", encoding="utf-8-sig")
            self._writer = csv.DictWriter(self._file, fieldnames=CSV_FIELDS)
            self._writer.writeheader()
            self.urls_path.write_text("", encoding="utf-8")

    # Returns True if this URL is already saved, so it can be skipped without fetching.
    def seen(self, url):
        return url in self.urls

    # Saves one article row (flushed immediately) under a lock; returns False if it was a duplicate.
    def add(self, row):
        with self.lock:
            key = normalize_url(row["url"])
            if key in self.urls:
                return False
            self.urls.add(key)
            self.counts[row["source"]] += 1
            self._writer.writerow({k: row.get(k, "") for k in CSV_FIELDS})
            self._file.flush()
            with open(self.urls_path, "a", encoding="utf-8") as f:
                f.write(row["url"] + "\n")
            return True

    # Closes the CSV file so everything is written to disk.
    def close(self):
        self._file.close()


# =============================================================================
# PER-SOURCE WORKER
# =============================================================================

# Runs discovery and scraping for one source until its target is met or candidates run out; returns its stats.
def scrape_source(name, cfg, bar, overall, store):
    parsed = urlparse(cfg["home"])
    origin = f"{parsed.scheme}://{parsed.netloc}"
    base_host = (parsed.hostname or "").lower().removeprefix("www.")
    throttle = Throttle(REQUEST_DELAY)
    stats = Counter()

    # Updates this source's progress bar with an optional status label and the live failure counters.
    def refresh(state=None):
        if state:
            bar.set_description_str(f"{name:<13} {state}")
        bar.set_postfix(
            tried=stats["tried"], blocked=stats["blocked"],
            short=stats["short"] + stats["empty"], err=stats["error"],
            refresh=True,
        )

    # Advances both this source's bar and the overall bar by one saved article.
    def tick():
        bar.update(1)
        overall.update(1)

    need = TARGET_ARTICLES_PER_SOURCE - store.counts[name]
    if need <= 0:
        bar.set_description_str(f"{name:<13} done")
        return stats

    refresh("discovering")
    rules = SiteRules(origin, throttle)
    candidates = iter_candidates(cfg, origin, base_host, throttle, rules)
    refresh("scraping")

    exhausted = False
    pending = set()

    with ThreadPoolExecutor(WORKERS_PER_SOURCE, thread_name_prefix=name[:6]) as pool:
        while True:
            while (
                not exhausted
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
                    tick()
                elif not row:
                    stats[reason] += 1
            refresh()

            if stats["ok"] >= need:
                for f in pending:
                    f.cancel()

    if stats["ok"] >= need:
        refresh("done")
    else:
        refresh("SHORT")
    logging.info("%s finished: %s", name, dict(stats))
    return stats


# =============================================================================
# MAIN
# =============================================================================

# Entry point: builds the progress bars, runs all sources in parallel, then prints the summary table.
def main():
    store = Store(OUTPUT_CSV, URLS_LOG, RESUME)
    names = list(SOURCES)
    already = sum(min(store.counts[n], TARGET_ARTICLES_PER_SOURCE) for n in names)

    overall = tqdm(
        total=TARGET_ARTICLES_PER_SOURCE * len(names), initial=already,
        desc=f"{'OVERALL':<13} total", unit="art", position=0, leave=True,
        bar_format="{desc} {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} "
                   "[{elapsed}<{remaining}, {rate_fmt}]",
    )
    bars = {
        name: tqdm(
            total=TARGET_ARTICLES_PER_SOURCE,
            initial=min(store.counts[name], TARGET_ARTICLES_PER_SOURCE),
            desc=f"{name:<13} queued", unit="art", position=i + 1, leave=True,
        )
        for i, name in enumerate(names)
    }

    results = {}
    try:
        with ThreadPoolExecutor(MAX_PARALLEL_SOURCES, thread_name_prefix="src") as pool:
            futures = {
                pool.submit(scrape_source, n, SOURCES[n], bars[n], overall, store): n
                for n in names
            }
            for future, name in futures.items():
                try:
                    results[name] = future.result()
                except Exception:
                    logging.exception("Source %s crashed", name)
                    results[name] = Counter()
    except KeyboardInterrupt:
        logging.warning("Interrupted by user; progress is saved, rerun to resume.")
        print("\nInterrupted. Progress saved; rerun to resume.")
    finally:
        for b in bars.values():
            b.close()
        overall.close()
        store.close()

    print("\n" + "=" * 62)
    print(f"{'SOURCE':<14}{'SAVED':>8}{'TRIED':>8}{'BLOCKED':>9}{'SHORT':>7}{'ERR':>6}")
    for name in names:
        s = results.get(name, Counter())
        saved = store.counts[name]
        flag = "" if saved >= TARGET_ARTICLES_PER_SOURCE else "  <- SHORT"
        print(
            f"{name:<14}{saved:>8}{s['tried']:>8}{s['blocked']:>9}"
            f"{s['short'] + s['empty']:>7}{s['error']:>6}{flag}"
        )
    print(f"\nTotal articles in {OUTPUT_CSV}: {sum(store.counts.values())}")


if __name__ == "__main__":
    main()