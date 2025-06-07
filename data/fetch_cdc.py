import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
import re
import feedparser

# === STEP 1: Fetch CDC Socrata datasets ===
def fetch_cdc_socrata(start_year=2010):
    url = "https://data.cdc.gov/api/views/metadata/v1"
    r = requests.get(url)
    raw_data = r.json()
    df = pd.DataFrame(raw_data)
    df["createdAt"] = pd.to_datetime(df["createdAt"], errors="coerce")
    df = df.dropna(subset=["createdAt"])
    df = df[df["createdAt"].dt.year >= start_year]
    df["created_date"] = df["createdAt"].dt.date
    counts = df.groupby("created_date").size().reset_index(name="datasets_created")
    counts["Agency"] = "CDC"
    print("✅ CDC Socrata fetch complete ✔️")
    return counts

# === STEP 2: Fetch MMWR release dates from RSS ===
def fetch_mmwr_rss():
    feed_url = "https://tools.cdc.gov/api/v2/resources/media/403372.rss"
    feed = feedparser.parse(feed_url)
    dates = []

    for entry in feed.entries:
        try:
            published = entry.published_parsed
            dt = datetime(published.tm_year, published.tm_mon, published.tm_mday).date()
            dates.append(dt)
        except Exception:
            continue

    df = pd.DataFrame(dates, columns=["created_date"])
    df["datasets_created"] = 1
    df = df.groupby("created_date").size().reset_index(name="datasets_created")
    df["Agency"] = "CDC"
    print(f"✅ MMWR RSS pulled {len(df)} entries ✔️")
    return df

# === STEP 3: Scrape VAERS release date from CDC Wonder ===
def fetch_vaers_release():
    url = "https://wonder.cdc.gov/vaers.html"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")

    text_blocks = soup.find_all(string=re.compile(r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}, \d{4}\b'))
    for text in text_blocks:
        try:
            date_match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}, \d{4}', text)
            if date_match:
                date = datetime.strptime(date_match.group(), "%B %d, %Y").date()
                df = pd.DataFrame([{"created_date": date, "datasets_created": 1, "Agency": "CDC"}])
                print(f"✅ VAERS update found: {date}")
                return df
        except Exception:
            continue

    print("⚠️ No VAERS release date found.")
    return pd.DataFrame(columns=["created_date", "datasets_created", "Agency"])

# === STEP 4: Scrape VSRR release dates from NCHS ===
def fetch_vsrr_release_dates():
    url = "https://www.cdc.gov/nchs/nvss/vsrr.htm"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")

    links = soup.select("section.card-body a")
    dates = []

    for link in links:
        href = link.get("href", "")
        text = link.get_text()
        match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}, \d{4}', text)
        if match:
            try:
                date = datetime.strptime(match.group(), "%B %d, %Y").date()
                dates.append(date)
            except Exception:
                continue

    df = pd.DataFrame(dates, columns=["created_date"])
    df["datasets_created"] = 1
    df = df.groupby("created_date").size().reset_index(name="datasets_created")
    df["Agency"] = "CDC"
    print("✅ VSRR scrape complete ✔️")
    return df

# === STEP 5: Combine, save daily, and generate monthly summary ===
def save_combined_cdc_data(output_path_daily="data/cdc_dataset_counts.csv", output_path_monthly="data/cdc_monthly.csv"):
    socrata_df = fetch_cdc_socrata()
    mmwr_df = fetch_mmwr_rss()
    vaers_df = fetch_vaers_release()
    vsrr_df = fetch_vsrr_release_dates()

    combined = pd.concat([socrata_df, mmwr_df, vaers_df, vsrr_df], ignore_index=True)
    combined = combined.groupby(["created_date", "Agency"]).agg({"datasets_created": "sum"}).reset_index()
    os.makedirs(os.path.dirname(output_path_daily), exist_ok=True)
    combined.to_csv(output_path_daily, index=False)
    print(f"✅ Combined CDC daily data saved to {output_path_daily}")

    # Convert to monthly summary
    combined["month"] = pd.to_datetime(combined["created_date"]).values.astype("datetime64[M]")
    monthly = combined.groupby(["month", "Agency"]).agg({"datasets_created": "sum"}).reset_index()
    monthly.to_csv(output_path_monthly, index=False)
    print(f"✅ CDC monthly summary saved to {output_path_monthly}")

# === Run when called directly ===
if __name__ == "__main__":
    save_combined_cdc_data()