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

FILE = "news_validation_all_categorized.csv"

df = pd.read_csv(FILE, encoding="utf-8-sig")


df.to_sql("labeled_data", engine, if_exists="append", index=False, chunksize=1000)
print("Done")