import csv
import re
import sys
from collections import Counter, defaultdict
from urllib.parse import urlparse

csv.field_size_limit(sys.maxsize)

INPUT = "news_validation_all.csv"
OUTPUT = "news_validation_all_categorized.csv"
SAMPLES = "category_review_samples.csv"

CATEGORIES = [
    "Market/Stocks",
    "Business/Economy",
    "Technology",
    "Health",
    "Energy",
    "Politics",
    "World",
    "Sports",
    "Crime/Justice",
    "Science/Environment",
    "Culture/Entertainment",
    "Lifestyle",
    "Education",
    "Other",
]

KEYWORDS = {
    "Market/Stocks": [
        "stock market", "stock exchange", "wall street", "nasdaq", "dow jones",
        "s&p 500", "ftse", "nikkei", "sensex", "nifty", "tech stocks",
        "stock prices", "stocks fell", "stocks rose", "stocks dropped",
        "stocks gained", "stocks lost", "stocks closed", "stocks ended",
        "shares fell", "shares rose", "shares jumped", "shares dropped",
        "share price", "equity", "equities", "investor", "investors",
        "earnings call", "earnings report", "earnings season", "ipo",
        "dividend", "portfolio", "government bond", "treasury bond",
        "bond yield", "bond market", "treasury yield", "market rally",
        "stock rally", "bull market", "bear market", "market cap",
        "valuation", "hedge fund", "buyout",
        "share buyback", "stock buyback", "market close", "market open",
        "stock index", "quarterly results",
        "gold price", "gold prices", "silver price", "silver prices",
        "crude price", "oil price", "bitcoin", "crypto", "cryptocurrency",
        "etf", "forex", "commodities", "futures", "broker", "brokers",
        "quarterly profit", "revenue forecast", "market index", "selloff",
        "sell-off", "shareholders", "shareholder", "market value",
        "financial market", "financial markets", "market volatility",
        "stock trading", "trading halted",
    ],
    "Business/Economy": [
        "economy", "economic", "economics", "gdp", "inflation", "deflation",
        "recession", "unemployment", "jobs report", "job market", "labor market",
        "trade", "tariff", "tariffs", "business", "companies", "company",
        "corporate", "corporation", "ceo", "executive", "startup", "startups",
        "entrepreneur", "retail", "sales", "profit", "revenue", "loss",
        "bankruptcy", "layoffs", "hiring", "merger", "acquisition", "investment",
        "invest", "bank", "banking", "finance", "financial", "loan", "mortgage",
        "credit", "debt", "tax", "taxes", "subsidy", "supply chain",
        "manufacturing", "factory", "industry", "industrial", "small business",
        "consumer", "consumers", "spending", "cost of living", "wages", "salary",
        "union", "pension", "insurance", "real estate", "private equity",
        "housing market", "rent", "rents", "property", "airline", "airlines",
        "tourism", "travel industry", "hospitality", "contract", "deal",
        "economic growth", "central bank", "interest rate", "interest rates",
        "car sales", "vehicle sales", "automaker", "automakers", "car maker",
        "car makers", "chief operating officer", "consultant", "workplace",
        "employees", "bosses", "jobs", "staff", "jobs cull", "farmer",
        "farmers", "agriculture", "agricultural", "crops", "fertilizer",
    ],
    "Technology": [
        "technology", "tech", "ai", "artificial intelligence", "machine learning",
        "software", "hardware", "app", "apps", "smartphone", "iphone", "android",
        "google", "apple", "microsoft", "meta", "facebook", "instagram",
        "whatsapp", "twitter", "tiktok", "youtube", "social media", "internet",
        "online", "website", "cyber", "cybersecurity", "hack", "hacked",
        "hacker", "data breach", "privacy", "chip", "chips", "semiconductor",
        "processor", "computer", "laptop", "gadget", "gadgets", "device",
        "robot", "robotics", "algorithm", "digital", "platform", "cloud",
        "data center", "openai", "chatgpt", "chatbot", "blockchain", "nft",
        "virtual reality", "augmented reality", "metaverse", "streaming",
        "netflix", "spacex", "satellite", "telecom", "5g", "broadband",
        "self-driving", "autonomous", "drone", "drones", "silicon valley",
        "xbox", "playstation", "nintendo", "dataset", "data pipeline",
        "electric flight", "electric flights", "smart tv", "console",
        "video game console",
    ],
    "Health": [
        "health", "healthcare", "hospital", "hospitals", "doctor", "doctors",
        "nurse", "nurses", "patient", "patients", "medical", "medicine",
        "medication", "drug", "drugs", "disease", "illness", "virus", "vaccine",
        "vaccines", "vaccination", "covid", "coronavirus", "pandemic",
        "outbreak", "infection", "cancer", "diabetes", "obesity",
        "heart disease", "mental health", "depression", "anxiety", "therapy",
        "treatment", "symptoms", "diagnosis", "surgery", "clinic", "fda", "cdc",
        "nhs", "abortion", "pregnancy", "birth control", "fertility", "diet",
        "nutrition", "fitness", "wellness", "sleep", "acne", "sunscreen",
        "allergy", "autism", "dementia", "alzheimer", "opioid", "overdose",
        "suicide", "mortality", "life expectancy", "public health",
        "health insurance", "medicare", "medicaid", "health minister",
        "syndrome", "disorder", "assisted living", "nursing home",
    ],
    "Energy": [
        "energy", "oil", "petroleum", "crude", "opec", "gasoline", "petrol",
        "diesel", "fuel", "electricity", "power grid", "utility", "utilities",
        "renewable", "renewables", "solar", "wind farm", "wind power",
        "hydropower", "hydroelectric", "nuclear power", "nuclear plant",
        "nuclear reactor", "nuclear energy", "coal", "oil pipeline",
        "gas pipeline", "lng", "refinery", "drilling", "fracking",
        "battery", "batteries", "hydrogen", "geothermal", "energy prices",
        "energy crisis", "power outage", "power cut", "power cuts", "blackout",
        "oil prices", "gas prices", "energy transition", "electric grid",
        "power plant",
        "energy company", "energy companies", "oil company", "oil companies",
    ],
    "Politics": [
        "politics", "political", "election", "elections", "vote", "votes",
        "voter", "voters", "voting", "campaign", "president", "presidential",
        "prime minister", "government", "parliament", "congress", "senate",
        "house of representatives", "lawmaker", "lawmakers", "legislator",
        "minister", "ministry", "democrat", "democrats", "republican",
        "republicans", "party", "parties", "policy", "policies", "legislation",
        "bill", "law", "supreme court", "constitution", "impeachment",
        "cabinet", "governor", "mayor", "mayors", "candidate", "primary",
        "midterm", "midterms", "greens", "ballot",
        "referendum", "poll", "polls", "approval rating", "white house",
        "kremlin", "downing street", "european union", "diplomacy",
        "diplomatic", "treaty", "summit", "geopolitics", "geopolitical",
        "trump", "biden", "harris", "obama", "putin", "zelensky", "netanyahu",
        "modi", "xi jinping", "macron", "scholz", "starmer", "gop",
        "journalist", "journalists", "press freedom", "budget",
        "orange order", "orangemen", "drumcree", "hardliners",
    ],
    "World": [
        "world", "international", "foreign", "global", "abroad", "overseas",
        "war", "conflict", "military", "troops", "soldiers", "army", "navy",
        "air force", "airstrike", "airstrikes", "missile", "missiles", "bomb",
        "bombing", "ceasefire", "hostage", "hostages", "gaza", "israel",
        "palestinian", "palestinians", "hamas", "hezbollah", "ukraine",
        "russia", "china", "beijing", "taiwan", "iran", "tehran", "iraq",
        "syria", "lebanon", "yemen", "houthi", "houthis", "saudi", "uae",
        "qatar", "egypt", "turkey", "jordan", "afghanistan", "pakistan", "india",
        "kashmir", "africa", "nigeria", "kenya", "sudan", "ethiopia",
        "europe", "britain", "france", "germany", "italy", "spain", "poland",
        "hungary", "mexico", "brazil", "venezuela", "colombia", "haiti",
        "canada", "australia", "japan", "south korea", "north korea",
        "philippines", "indonesia", "myanmar", "bangladesh", "sri lanka",
        "nepal", "cuba", "cubans", "cia", "rubio", "refugee", "refugees",
        "humanitarian", "embassy", "border", "russian", "russians",
        "detained", "detention", "sanctioned", "bahamas", "immigration",
        "protester", "protesters",
        "migrant", "migrants", "migration", "asylum", "united nations", "nato",
        "sanctions", "coup", "protest", "protests", "uprising", "revolution",
        "terrorism", "terrorist", "terrorists", "extremist", "militant",
        "militants", "insurgent", "rebels", "rebellion", "civil war",
        "genocide", "war crimes", "nuclear weapon", "nuclear weapons",
        "nuclear war", "nuclear deal", "nuclear program", "icc",
    ],
    "Sports": [
        "sport", "sports", "football", "soccer", "basketball", "baseball",
        "cricket", "tennis", "golf", "rugby", "hockey", "boxing", "ufc", "mma",
        "nfl", "nba", "mlb", "nhl", "fifa", "olympics", "olympic", "world cup",
        "match", "matches", "game", "games", "team", "teams", "player",
        "players", "coach", "manager", "championship", "tournament", "league",
        "season", "playoff", "playoffs", "final", "semifinal", "stadium",
        "athlete", "athletes", "runner", "marathon", "medal", "medals",
        "referee", "transfer", "goal", "goals", "score", "scores", "wicket",
        "innings", "batting", "bowling", "quarterback", "touchdown",
        "home run", "grand slam", "formula 1", "f1", "nascar", "cycling",
        "swimming", "gymnastics", "wrestling", "boxer", "champion", "champions",
        "club", "clubs", "derby", "rivalry", "cup", "asian cup", "fa cup",
        "world series", "opener", "win", "wins", "victory", "defeat",
        "defeated", "defeats", "beat", "beats", "comeback", "loss", "losses",
        "kickoff", "won", "loses", "lost", "knicks", "madison square garden",
    ],
    "Crime/Justice": [
        "crime", "criminal", "police", "cop", "cops", "court", "courts",
        "judge", "jury", "trial", "arrest", "arrested", "arrests", "charged",
        "charge", "charges", "indicted", "indictment", "sentence", "sentenced",
        "sentencing", "imprisonment", "prison", "jail", "inmate", "murder",
        "homicide", "shooting", "gun", "guns", "robbery", "theft", "fraud",
        "scam", "corruption", "bribery",         "investigation", "investigating", "investigators",
        "prosecutor", "prosecutors", "lawsuit", "attorney", "lawyer",
        "convicted", "conviction", "guilty", "plea", "suspect", "suspects",
        "victim", "victims", "killed", "death", "deaths", "body", "bodies",
        "abuse", "assault", "rape", "sexual assault", "domestic violence",
        "trafficking", "smuggling", "drug cartel", "gang", "gangs",
        "kidnapping", "kidnapped", "missing person", "missing persons",
        "manslaughter", "defraud", "defrauded", "defrauding", "scheme",
        "death penalty", "execution", "executions", "extradition", "warrant",
        "raid", "raids", "witness", "witnesses", "legal case", "prisoner",
        "prisoners", "escaped", "escape", "manhunt", "gunmen", "gunman",
        "open fire", "gunshot", "gunshots", "wounded", "wounding", "stabbed",
        "stabbing", "shooter", "shooters", "school shooting", "school shooter",
        "speeding", "caught speeding", "garda", "patrol", "looting",
        "alleged attack", "knife attack", "drive-by", "drive-by shooting",
        "shot in head", "struck", "fatal accident", "pedestrian",
        "allegation", "allegations", "vandal", "vandalism",
    ],
    "Science/Environment": [
        "science", "scientific", "research", "study", "studies", "scientists",
        "researchers", "experiment", "discovery", "climate", "climate change",
        "global warming", "environment", "environmental", "emissions", "carbon",
        "greenhouse gas", "pollution", "air quality", "water quality",
        "wildlife", "species", "endangered", "extinction", "ecosystem",
        "biodiversity", "conservation", "deforestation", "rainforest", "ocean",
        "sea level", "glacier", "iceberg", "arctic", "antarctic", "drought",
        "flood", "floods", "flooding", "wildfire", "wildfires", "hurricane",
        "typhoon", "storm", "storms", "earthquake", "volcano", "tsunami",
        "weather", "heatwave", "heat wave", "cold snap", "nasa", "space",
        "astronomy", "telescope", "planet", "mars", "moon", "asteroid",
        "comet", "solar system", "galaxy", "universe", "physics", "chemistry",
        "biology", "genetics", "dna", "fossil", "archaeology", "archaeologist",
        "dinosaurs", "evolution", "quantum", "nuclear fusion", "research team",
        "new study", "scientific study", "owl", "owls", "bird", "birds",
        "forecast", "temperature", "temperatures", "chilly", "snow", "rain",
        "tree", "trees", "fallen trees", "dream", "dreams",
    ],
    "Culture/Entertainment": [
        "entertainment", "celebrity", "celebrities", "actor", "actress", "film",
        "films", "movie", "movies", "cinema", "hollywood", "bollywood", "music",
        "musician", "singer", "band", "album", "song", "songs", "concert",
        "tour", "tv", "television", "tv series", "show", "shows", "episode",
        "award", "awards", "oscar", "oscars", "grammy", "grammys", "emmy",
        "emmys", "golden globe", "golden globes", "bafta", "art", "arts",
        "artist", "artists", "painting", "sculpture", "museum", "gallery",
        "exhibition", "book", "books", "author", "novel", "novelist", "writer",
        "poetry", "theater", "theatre", "musical", "opera", "festival",
        "gaming", "video game", "video games", "gamer", "disney", "hbo",
        "podcast", "podcasts", "radio", "magazine", "royal", "royals",
        "prince", "princess", "king", "queen", "james bond", "superstar",
        "singer-songwriter", "honour", "honor", "knighthood",
    ],
    "Lifestyle": [
        "lifestyle", "life style", "food", "foods", "recipe", "recipes",
        "cooking", "chef", "restaurant", "restaurants", "cuisine", "dining",
        "travel", "travelling", "traveling", "tourism", "tourist", "tourists",
        "hotel", "hotels", "destination", "vacation", "holiday", "holidays",
        "fashion", "style", "beauty", "makeup", "skincare", "wellness",
        "fitness", "workout", "exercise", "parenting", "family", "relationship",
        "relationships", "dating", "marriage", "wedding", "home", "homes",
        "garden", "gardening", "interior", "decor",
        "shopping", "product", "products", "review", "reviews", "deals",
        "sale", "gift", "gifts", "clothing", "shoes", "jewelry", "watch",
        "watches", "pet", "pets", "dog", "dogs", "cat", "cats", "coffee",
        "wine", "beer", "alcohol", "meditation", "yoga", "spa", "recipe",
        "plane ticket", "airfare", "airfares", "product recall",
        "pulled from shelves", "retirement", "retiree", "retirees",
        "pub", "pubs", "inn", "inns",
    ],
    "Education": [
        "education", "school", "schools", "university", "universities",
        "college", "colleges", "student", "students", "teacher", "teachers",
        "classroom", "classrooms", "curriculum", "exam", "exams",
        "test scores", "college admissions", "university admissions",
        "school admissions", "campus", "scholarship",
        "scholarships", "tuition", "academic", "academy", "degree", "degrees",
        "graduation", "graduate", "graduates", "principal", "professor",
        "professors", "lecturer", "semester", "homework", "literacy",
        "school board", "education department", "education minister",
        "higher education", "public school", "private school",
    ],
}

PRIORITY = [
    "Sports",
    "Health",
    "Energy",
    "Market/Stocks",
    "Technology",
    "Crime/Justice",
    "Education",
    "Science/Environment",
    "Culture/Entertainment",
    "Lifestyle",
    "Politics",
    "World",
    "Business/Economy",
    "Other",
]

SECTION_BOOST = {
    "sport": ("Sports", 12),
    "sports": ("Sports", 12),
    "football": ("Sports", 12),
    "business": ("Business/Economy", 4),
    "economy": ("Business/Economy", 10),
    "finance": ("Business/Economy", 8),
    "money": ("Business/Economy", 6),
    "personal-finance": ("Business/Economy", 6),
    "property": ("Business/Economy", 6),
    "real-estate": ("Business/Economy", 6),
    "markets": ("Market/Stocks", 10),
    "stock-market": ("Market/Stocks", 10),
    "investing": ("Market/Stocks", 10),
    "crypto": ("Market/Stocks", 10),
    "gold-prices": ("Market/Stocks", 12),
    "silver-prices": ("Market/Stocks", 12),
    "politics": ("Politics", 12),
    "news-and-politics": ("Politics", 12),
    "policy": ("Politics", 8),
    "2020-elections": ("Politics", 12),
    "ulster": ("Politics", 6),
    "tech": ("Technology", 12),
    "technology": ("Technology", 12),
    "gadgets": ("Technology", 12),
    "ai-artificial-intelligence": ("Technology", 12),
    "entertainment": ("Culture/Entertainment", 12),
    "showbiz": ("Culture/Entertainment", 12),
    "celebrity": ("Culture/Entertainment", 10),
    "royals": ("Culture/Entertainment", 10),
    "arts": ("Culture/Entertainment", 12),
    "arts-and-culture": ("Culture/Entertainment", 12),
    "arts-entertainment": ("Culture/Entertainment", 12),
    "arts-culture": ("Culture/Entertainment", 12),
    "music": ("Culture/Entertainment", 12),
    "film": ("Culture/Entertainment", 12),
    "tv": ("Culture/Entertainment", 12),
    "books": ("Culture/Entertainment", 12),
    "culture": ("Culture/Entertainment", 12),
    "games": ("Culture/Entertainment", 10),
    "gaming": ("Culture/Entertainment", 10),
    "podcast": ("Culture/Entertainment", 8),
    "lifestyle": ("Lifestyle", 12),
    "life": ("Lifestyle", 8),
    "life-style": ("Lifestyle", 8),
    "travel": ("Lifestyle", 10),
    "shopping": ("Lifestyle", 10),
    "food": ("Lifestyle", 10),
    "recipes": ("Lifestyle", 10),
    "wellness": ("Lifestyle", 8),
    "select": ("Lifestyle", 8),
    "health": ("Health", 12),
    "environment": ("Science/Environment", 12),
    "climate": ("Science/Environment", 12),
    "science": ("Science/Environment", 12),
    "weather": ("Science/Environment", 10),
    "space": ("Science/Environment", 8),
    "energy": ("Energy", 10),
    "world": ("World", 8),
    "nation-world": ("World", 8),
    "uk": ("World", 6),
    "us": ("World", 8),
    "us-news": ("World", 8),
    "europe": ("World", 8),
    "middle-east": ("World", 10),
    "asia": ("World", 8),
    "africa": ("World", 8),
    "americas": ("World", 8),
    "israel-news": ("World", 10),
    "careers-education": ("Education", 12),
    "education": ("Education", 12),
    "crime": ("Crime/Justice", 12),
    "courts": ("Crime/Justice", 12),
    "obituaries": ("Other", 5),
}

DOMAIN_BOOST = {
    "theverge.com": ("Technology", 4),
    "reason.com": ("Politics", 4),
    "motherjones.com": ("Politics", 4),
    "propublica.org": ("Crime/Justice", 2),
    "al-monitor.com": ("World", 4),
    "middleeasteye.net": ("World", 4),
    "jpost.com": ("World", 4),
    "brecorder.com": ("Business/Economy", 3),
    "livemint.com": ("Business/Economy", 3),
    "dawn.com": ("World", 2),
    "thenews.com.pk": ("World", 2),
}


def normalize(text):
    text = text.lower().replace("\u2019", "'").replace("\u2018", "'")
    text = text.replace("-", " ")
    text = re.sub(r"[^a-z0-9&+']+", " ", text)
    return " " + re.sub(r"\s+", " ", text).strip() + " "


def build_patterns():
    patterns = {}
    for category, words in KEYWORDS.items():
        variants = set()
        for word in words:
            word = word.lower()
            variants.add(word)
            if " " not in word and not word.endswith("s"):
                variants.add(word + "s")
                if word.endswith("y"):
                    variants.add(word[:-1] + "ies")
                if word.endswith(("ch", "sh", "x", "z")):
                    variants.add(word + "es")
            elif " " in word:
                parts = word.split()
                last = parts[-1]
                if not last.endswith("s"):
                    variants.add(" ".join(parts[:-1] + [last + "s"]))
        normalized_words = []
        for word in sorted(variants, key=len, reverse=True):
            normalized_words.append(re.escape(normalize(word).strip()))
        patterns[category] = re.compile(
            r"(?<![a-z0-9])(?:" + "|".join(normalized_words) + r")(?![a-z0-9])"
        )
    return patterns


def get_sections(url):
    try:
        path = urlparse(str(url)).path.strip("/")
    except Exception:
        return []
    if not path:
        return []
    return [part.lower() for part in path.split("/") if part]


def classify(title, content, url, domain, patterns):
    title_norm = normalize(str(title))
    content_norm = normalize(str(content)[:6000])
    scores = {category: 0 for category in CATEGORIES}
    matches = {}

    for category, pattern in patterns.items():
        title_hits = set(pattern.findall(title_norm))
        content_hits = set(pattern.findall(content_norm))
        matches[category] = (title_hits, content_hits)
        scores[category] += 8 * len(title_hits) + min(len(content_hits), 8)

    for section in get_sections(url):
        if section in SECTION_BOOST:
            category, boost = SECTION_BOOST[section]
            scores[category] += boost

    domain = str(domain).lower()
    if domain in DOMAIN_BOOST:
        category, boost = DOMAIN_BOOST[domain]
        scores[category] += boost

    best_score = max(scores.values())
    if best_score < 6:
        return "Other", scores, matches

    candidates = [c for c in CATEGORIES if scores[c] == best_score]
    for category in PRIORITY:
        if category in candidates:
            return category, scores, matches
    return candidates[0], scores, matches


def main():
    patterns = build_patterns()
    counts = Counter()
    sample_rows = defaultdict(list)
    total = 0

    with open(INPUT, "r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        fieldnames = list(reader.fieldnames or []) + ["category"]
        with open(OUTPUT, "w", encoding="utf-8-sig", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in reader:
                category, scores, matches = classify(
                    row.get("title_clean", ""),
                    row.get("content_clean", ""),
                    row.get("news_url_clean", ""),
                    row.get("domain", ""),
                    patterns,
                )
                row["category"] = category
                writer.writerow(row)
                counts[category] += 1
                total += 1

                if len(sample_rows[category]) < 15:
                    runner_up = sorted(
                        ((score, c) for c, score in scores.items() if c != category),
                        reverse=True,
                    )[:1]
                    sample_rows[category].append(
                        {
                            "source_row_index": row.get("source_row_index", ""),
                            "category": category,
                            "score": scores[category],
                            "runner_up": runner_up[0][1] if runner_up else "",
                            "runner_up_score": runner_up[0][0] if runner_up else "",
                            "title_clean": row.get("title_clean", ""),
                            "domain": row.get("domain", ""),
                            "news_url_clean": row.get("news_url_clean", ""),
                        }
                    )

                if total % 10000 == 0:
                    print(f"processed {total} rows", file=sys.stderr, flush=True)

    with open(SAMPLES, "w", encoding="utf-8-sig", newline="") as target:
        fields = [
            "source_row_index",
            "category",
            "score",
            "runner_up",
            "runner_up_score",
            "title_clean",
            "domain",
            "news_url_clean",
        ]
        writer = csv.DictWriter(target, fieldnames=fields)
        writer.writeheader()
        for category in CATEGORIES:
            for row in sample_rows[category]:
                writer.writerow(row)

    print(f"rows: {total}")
    for category in CATEGORIES:
        print(f"{category}: {counts[category]}")


if __name__ == "__main__":
    main()
