import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from datetime import date
import os
import re
import feedparser
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from collections import defaultdict

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
def fetch_vaers_dataset_counts():
    url = "https://vaers.hhs.gov/data/datasets.html"
    r = requests.get(url, verify = False)
    soup = BeautifulSoup(r.text, "html.parser")

    # Match things like 2023VAERSDATA.csv, 2024VAERSVAX.csv, etc.
    pattern = re.compile(r'(20\d{2})VAERS(?:DATA|VAX|SYMPTOMS)\.csv', re.IGNORECASE)
    year_counts = defaultdict(int)

    for link in soup.find_all("a", href=True):
        match = pattern.search(link["href"])
        if match:
            year = match.group(1)
            year_counts[year] += 1

    if year_counts:
        df = pd.DataFrame([
            {"created_date": date.today(), "year": int(year), "datasets_created": count, "Agency": "CDC"}
            for year, count in sorted(year_counts.items())
        ])
        print("✅ VAERS dataset counts by year:")
        print(df)
        return df
    else:
        print("⚠️ No VAERS datasets found on page.")
        return pd.DataFrame(columns=["created_date", "year", "datasets_created", "Agency"])

# === STEP 4: Scrape VSRR release dates from NCHS ===
def fetch_vsrr_release_dates():
    url = "https://www.cdc.gov/nchs/nvss/vsrr.htm"
    r = requests.get(url, verify=False)
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
def fetch_cdc_datasets_counts(output_path_daily="data/cdc_dataset_counts.csv", output_path_monthly="data/cdc_monthly.csv"):
    socrata_df = fetch_cdc_socrata()
    mmwr_df = fetch_mmwr_rss()
    vaers_df = fetch_vaers_dataset_counts()
    vsrr_df = fetch_vsrr_release_dates()

    combined = pd.concat([socrata_df, mmwr_df, vaers_df, vsrr_df], ignore_index=True)
    combined = combined.groupby(["created_date", "Agency"]).agg({"datasets_created": "sum"}).reset_index()
    os.makedirs(os.path.dirname(output_path_daily), exist_ok=True)
    combined.to_csv(output_path_daily, index=False)
    print(f"✅ Combined CDC daily data saved to {output_path_daily}")

# === Run when called directly ===
if __name__ == "__main__":
    fetch_cdc_datasets_counts()