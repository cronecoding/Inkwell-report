import pandas as pd
import requests
import dash
from dash import dcc, html, Input, Output
import plotly.express as px

# === STEP 1: FETCH & CLEAN METADATA===
# ==============CDC==============
def fetch_cdc_dataset_counts(start_year=2010):
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
    counts.to_csv("cdc_dataset_counts.csv", index=False)
    print("✅ CDC fetch complete ✔️")

# ===============epa===============
def fetch_epa_dataset_counts(start_year=2010):
    base_url = "https://catalog.data.gov"
    org_name = "epa-gov"
    search_url = f"{base_url}/api/3/action/package_search"
    
    params = {"fq": f"organization:{org_name}", "rows": 1000, "start": 0}
    all_results = []

    while True:
        r = requests.get(search_url, params=params)
        results = r.json()["result"]["results"]
        if not results:
            break
        all_results.extend(results)
        params["start"] += len(results)
        if len(results) < 1000:
            break
            
    created_dates = [r.get("metadata_created") for r in all_results if r.get("metadata_created")]
    df = pd.DataFrame({"createdAt": pd.to_datetime(created_dates, errors="coerce")})
    df = df[df["createdAt"].dt.year >= start_year]
    df["created_date"] = df["createdAt"].dt.date
    counts = df.groupby("created_date").size().reset_index(name="datasets_created")
    counts["Agency"] = "EPA"
    counts.to_csv("epa_dataset_counts.csv", index=False)
    print("✅ EPA fetch complete ✔️")

# ========usda=============
def fetch_usda_dataset_counts(start_year=2010):
    base_url = "https://catalog.data.gov"
    org_name = "usda-gov"
    search_url = f"{base_url}/api/3/action/package_search"
    
    params = {
        "fq": f"organization:{org_name}",
        "sort": "metadata_created asc",  # optional, to see earliest dates
        "rows": 1000,
        "start": 0
    }
    all_results = []

    while True:
        r = requests.get(search_url, params=params)
        results = r.json()["result"]["results"]
        if not results:
            break
        all_results.extend(results)
        params["start"] += len(results)
        if len(results) < 1000:
            break

    created_dates = [r.get("metadata_created") for r in all_results if r.get("metadata_created")]
    df = pd.DataFrame({"createdAt": pd.to_datetime(created_dates, errors="coerce")})
    df = df.dropna()
    df = df[df["createdAt"].dt.year >= start_year]
    df["created_date"] = df["createdAt"].dt.date

    counts = df.groupby("created_date").size().reset_index(name="datasets_created")
    counts["Agency"] = "USDA"
    counts.to_csv("usda_dataset_counts.csv", index=False)

    print("✅ USDA fetch complete ✔️")

# ==== noaa === #
import time

def fetch_noaa_created_timestamps(start_year=2010):
    base_url = "https://catalog.data.gov"
    org_name = "noaa-gov"
    search_url = f"{base_url}/api/3/action/package_search"

    # Get total count of datasets
    init = requests.get(search_url, params={"fq": f"organization:{org_name}", "rows": 0})
    total = init.json()["result"]["count"]
    print(f"Total NOAA datasets available: {total}")

    start = 0
    batch_size = 1000
    created_dates = []

    while start < total:
        params = {"fq": f"organization:{org_name}", "rows": batch_size, "start": start}

        for attempt in range(3):
            try:
                r = requests.get(search_url, params=params, timeout=30)
                r.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                print(f"Attempt {attempt + 1} failed at start={start}: {e}")
                time.sleep(5 * (attempt + 1))
        else:
            print(f"Skipping batch at start={start} after 3 failed attempts.")
            start += batch_size
            continue

        batch = r.json()["result"]["results"]
        if not batch:
            break

        # Just get timestamps
        created_dates.extend([item["metadata_created"] for item in batch if "metadata_created" in item])

        print(f"Fetched {start + len(batch)} / {total}")
        start += batch_size
        time.sleep(1)  # Be nice to the server

    # Convert to DataFrame and group by month
    df = pd.DataFrame({"createdAt": pd.to_datetime(created_dates, errors="coerce")})
    df = df[df["createdAt"].dt.year >= start_year]
    df["month"] = df["createdAt"].dt.to_period("M").dt.to_timestamp()
    monthly = df.groupby("month").size().reset_index(name="datasets_created")
    monthly["Agency"] = "NOAA"
    monthly.to_csv("noaa_monthly_dataset_counts.csv", index=False)

    return monthly
    print("✅ NOAA fetch complete ✔️")

# ======= ckan data (doj, nsf) started in 2019 ========
def fetch_ckan_dataset_counts(agency_key, output_csv, start_year=2010):
    import requests
    import pandas as pd
    import os

    base_url = "https://catalog.data.gov"
    search_url = f"{base_url}/api/3/action/package_search"

    params = {
        "fq": f"organization:{agency_key}",
        "rows": 1000,
        "start": 0
    }

    seen_ids = set()
    created_dates = []

    print(f"🔍 Fetching CKAN data for: {agency_key}")
    
    while True:
        r = requests.get(search_url, params=params)
        results = r.json().get("result", {}).get("results", [])

        if not results:
            break

        for r in results:
            dataset_id = r.get("id")
            created_str = r.get("metadata_created")
            if dataset_id and created_str and dataset_id not in seen_ids:
                seen_ids.add(dataset_id)
                created_dates.append(created_str)

        params["start"] += len(results)
        if len(results) < 1000:
            break

    if not created_dates:
        print(f"⚠️ No new records found for {agency_key}")
        return

    df = pd.DataFrame({"createdAt": pd.to_datetime(created_dates, errors="coerce")})
    df = df.dropna()
    df = df[df["createdAt"].dt.year >= start_year]
    df["created_date"] = df["createdAt"].dt.date

    counts = df.groupby("created_date").size().reset_index(name="datasets_created")
    counts["Agency"] = agency_key.upper()

    counts.to_csv(output_csv, index=False)
    print(f"✅ {agency_key.upper()} fetch complete — {len(df)} records processed")

# ======clean it up nice =======
def clean_agency_file_by_month(filepath, agency_name):
    df = pd.read_csv(filepath, parse_dates=["created_date"])
    df["Agency"] = agency_name
    df["month"] = df["created_date"].values.astype('datetime64[M]')
    monthly_counts = df.groupby("month").agg({
        "datasets_created": "sum"
    }).reset_index()
    monthly_counts["Agency"] = agency_name
    return monthly_counts

# === RUN FETCH + CLEAN + SAVE (ALL AGENCIES, 2010–present) ===

# CDC (Socrata)
fetch_cdc_dataset_counts()
cdc_monthly = clean_agency_file_by_month("cdc_dataset_counts.csv", "CDC")
cdc_monthly.to_csv("cdc_monthly.csv", index=False)
print("✅ CDC monthly summary saved to cdc_monthly.csv")

# EPA
fetch_epa_dataset_counts()
epa_monthly = clean_agency_file_by_month("epa_dataset_counts.csv", "EPA")
epa_monthly.to_csv("epa_monthly.csv", index=False)
print("✅ EPA monthly summary saved to epa_monthly.csv")

# HHS
fetch_ckan_dataset_counts("hhs-gov", "HHS")
hhs_monthly = clean_agency_file_by_month("hhs_dataset_counts.csv", "HHS")
hhs_monthly.to_csv("hhs_monthly.csv", index=False)
print("✅ HHS monthly summary saved to hhs_monthly.csv")

# DOJ
fetch_ckan_dataset_counts("doj-gov", "DOJ")
doj_monthly = clean_agency_file_by_month("doj_dataset_counts.csv", "DOJ")
doj_monthly.to_csv("doj_monthly.csv", index=False)
print("✅ DOJ monthly summary saved to doj_monthly.csv")

# USDA
fetch_ckan_dataset_counts("usda-gov", "USDA")
usda_monthly = clean_agency_file_by_month("usda_dataset_counts.csv", "USDA")
usda_monthly.to_csv("usda_monthly.csv", index=False)
print("✅ USDA monthly summary saved to usda_monthly.csv")

# NSF
fetch_ckan_dataset_counts("nsf-gov", "NSF")
nsf_monthly = clean_agency_file_by_month("nsf_dataset_counts.csv", "NSF")
nsf_monthly.to_csv("nsf_monthly.csv", index=False)
print("✅ NSF monthly summary saved to nsf_monthly.csv")

#NOAA
noaa_data = fetch_noaa_created_timestamps()
noaa_data.to_csv("noaa_monthly.csv", index=False)
print("✅ NOAA monthly summary saved to noaa_monthly.csv")

import os

# Helper to safely load a CSV
def safe_read(path):
    if os.path.exists(path):
        df = pd.read_csv(path, parse_dates=["month"])
        if not df.empty and "datasets_created" in df.columns:
            return df
    return None

# Only include valid, non-empty files
dataframes = [
    safe_read("cdc_monthly.csv"),
    safe_read("epa_monthly.csv"),
    safe_read("hhs_monthly.csv"),
    safe_read("doj_monthly.csv"),
    safe_read("usda_monthly.csv"),
    safe_read("nsf_monthly.csv"),
    safe_read("noaa_monthly.csv")
]

# Filter out None entries
dataframes = [df for df in dataframes if df is not None]

# Combine safely
combined_df = pd.concat(dataframes, ignore_index=True)
combined_df["normalized"] = combined_df.groupby("Agency")["datasets_created"].transform(
    lambda x: x / x.max() if x.max() > 0 else 0
)
combined_df.to_csv("combined_monthly.csv", index=False)
print("✅ combined_monthly complete ✔️")
