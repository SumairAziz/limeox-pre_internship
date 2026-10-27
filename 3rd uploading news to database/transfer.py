import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

load_dotenv()

url = URL.create(
    "postgresql+psycopg2",
    username=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT")),
    database=os.getenv("DB_NAME"),
)
engine = create_engine(url)

FILE = "news_articles3.csv"  

df = pd.read_csv(FILE, encoding="utf-8")

df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)

# Check for dates that failed to parse (they become NaT)
print("Rows:", len(df), "| Unparseable dates:", df["date"].isna().sum())

df.to_sql("articles", engine, if_exists="append", index=False, chunksize=1000)
print("Done")