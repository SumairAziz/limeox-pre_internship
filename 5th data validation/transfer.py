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

FILE = "validation_output/news_validation_all.csv"  

df = pd.read_csv(FILE, encoding="utf-8")

df["published_at_utc"] = pd.to_datetime(df["published_at_utc"], errors="coerce", utc=True, format="ISO8601")
df["published_at_utc_parsed"] = pd.to_datetime(df["published_at_utc_parsed"], errors="coerce", utc=True, format="ISO8601")

print("Rows:", len(df), "| Unparseable dates:", df["published_at_utc"].isna().sum())

df.to_sql("articles", engine, if_exists="append", index=False, chunksize=1000)
print("Done")