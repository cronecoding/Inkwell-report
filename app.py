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
#=============hhs===========
def fetch_hhs_dataset_counts(start_year=2010):
    url = "https://healthdata.gov/api/views/metadata/v1"
    r = requests.get(url)
    raw_data = r.json()
    df = pd.DataFrame(raw_data)

    # Parse and filter by year
    df["createdAt"] = pd.to_datetime(df["createdAt"], errors="coerce")
    df = df.dropna(subset=["createdAt"])
    df = df[df["createdAt"].dt.year >= start_year]
    df["created_date"] = df["createdAt"].dt.date

    # Group by date
    counts = df.groupby("created_date").size().reset_index(name="datasets_created")
    counts["Agency"] = "HHS"
    counts.to_csv("hhs_dataset_counts.csv", index=False)

    print("✅ HHS fetch complete ✔️")
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
    safe_read("nsf_monthly.csv")
]

# Filter out None entries
dataframes = [df for df in dataframes if df is not None]

# Combine safely
combined_df = pd.concat(dataframes, ignore_index=True)
# ===== STEP 3: DASHBOARD IT======
app = dash.Dash(__name__)
app.title = "Dataset Transparency Dashboard"

# Update your layout
app.layout = html.Div(
    style={
        'backgroundColor': '#31363A',
        'color': '#FFFFFF',
        'fontFamily': 'Arial',
        'padding': '20px'
    },
    children=[
        html.H1("🧭 Dataset Transparency Trends", style={'textAlign': 'center'}),

        html.Label("Select Time Window:", style={"marginTop": "10px"}),
        dcc.Dropdown(
            id='month-window',
            options=[
                {"label": "15 years", "value": 180},
                {"label": "10 years", "value": 120},
                {'label': '5 years', 'value': 60},
                {'label': '1 year', 'value': 12},
                {"label": "6 months", "value": 6},
                {'label': '3 months', 'value': 3}
            ],
            value=6,
            clearable=False,
            style={
                "width": "200px",
                "marginBottom": "20px",
                "backgroundColor": "#1e1e1e",
                "color": "#ffffff",
                "border": "1px solid #555555",
                "borderRadius": "5px",
                "padding": "5px"
            }
        ),

        dcc.Graph(id='line-graph'),
        dcc.Graph(id='bar-graph'),

        html.Label("Compare Change In Number of Datasets Released (Month Over Year):", style={"marginTop": "20px"}),
        dcc.Dropdown(
            id='slope-month',
            options=[{"label": month, "value": i} for i, month in enumerate([
                "January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"
            ], 1)],
            value=pd.Timestamp.today().month,  # defaults to current month
            clearable=False,
            style={
                "width": "200px",
                "marginBottom": "20px",
                "backgroundColor": "#1e1e1e",
                "color": "#ffffff",
                "border": "1px solid #555555"
            }
        ),

        dcc.Graph(id='slope-graph'),

        html.Div(
            "Data from data.cdc.gov & catalog.data.gov",
            style={'textAlign': 'center', 'fontStyle': 'italic'}
        ),
                
        html.H2("📌 Single Agency View", style={"marginTop": "40px"}),

        html.Label("Select Agency:"),
        dcc.Dropdown(
            id='single-agency',
            options=[{"label": agency, "value": agency} for agency in [
                "CDC", "EPA", "HHS", "DOJ", "USDA", "NSF"
            ]],
            value="CDC",
            clearable=False,
            style={
                "width": "200px",
                "marginBottom": "20px",
                "backgroundColor": "#1e1e1e",
                "color": "#ffffff",
                "border": "1px solid #555555",
                "padding": "5px"
            }
        ),

        html.Label("Select Range (Months):"),
        dcc.Input(
            id='month-range',
            type='number',
            min=1,
            max=180,
            step=1,
            value=12,
            style={
                "width": "200px",
                "marginBottom": "20px",
                "backgroundColor": "#1e1e1e",
                "color": "#ffffff",
                "border": "1px solid #555555",
                "padding": "5px"
            }
        ),

        dcc.Graph(id='monthly-agency-bar'),
    ]
)
# === CALLBACK: Update Graphs Based on Month Range ===

@app.callback(
    Output('line-graph', 'figure'),
    Output('bar-graph', 'figure'),
    Output('slope-graph', 'figure'),
    Input('month-window', 'value'),
    Input('slope-month', 'value')
)
def update_graphs(months_back, slope_window):
    # Load all monthly data files
    data_paths = {
        "CDC": "cdc_monthly.csv",
        "EPA": "epa_monthly.csv",
        "HHS": "hhs_monthly.csv",
        "DOJ": "doj_monthly.csv",
        "USDA": "usda_monthly.csv",
        "NSF": "nsf_monthly.csv"
    }

    valid_dfs = []

    print("📊 Checking monthly data files:")
    for agency, path in data_paths.items():
        if os.path.exists(path):
            df = pd.read_csv(path, parse_dates=["month"])
            if df.empty or df["datasets_created"].isna().all():
                print(f"⚠️ {agency} data is EMPTY or all NA — skipping")
            else:
                print(f"✅ {agency} loaded with {len(df)} rows")
                valid_dfs.append(df)
        else:
            print(f"🚫 {agency} file not found: {path}")

    # Combine valid dataframes
    combined_df = pd.concat(valid_dfs, ignore_index=True)

    # Filter for selected number of months
    cutoff = pd.Timestamp.today().replace(day=1) - pd.DateOffset(months=months_back)
    recent_df = combined_df[combined_df["month"] >= cutoff]

    # === Graph 1: Line Chart with Drop-off Detection
    fig_line = px.line(
        recent_df,
        x="month",
        y="datasets_created",
        color="Agency",
        title=f"🕵️‍♂️ Dataset Releases (Last {months_back} Months)",
        labels={"month": "Month", "datasets_created": "Datasets Published"},
        markers=True
    )
    fig_line.update_layout(
        plot_bgcolor="#31363A",
        paper_bgcolor="#31363A",
        font_color="#FFFFFF"
    )

    dropoff_lines = []
    for agency in recent_df["Agency"].unique():
        agency_df = recent_df[recent_df["Agency"] == agency].sort_values("month")
        previous = None
        for _, row in agency_df.iterrows():
            if previous and previous > 0 and row["datasets_created"] == 0:
                dropoff_lines.append({"agency": agency, "date": row["month"]})
            previous = row["datasets_created"]
    for event in dropoff_lines:
        fig_line.add_vline(
            x=event["date"],
            line_dash="dash",
            line_color="red",
            annotation_text=f"{event['agency']} drop-off",
            annotation_position="top left"
        )

    # === Graph 2: Bar Chart of Totals
    total_recent = recent_df.groupby("Agency")["datasets_created"].sum().reset_index()
    fig_bar = px.bar(
        total_recent,
        x="Agency",
        y="datasets_created",
        title=f"📊 Total Datasets Published (Last {months_back} Months)",
        labels={"datasets_created": "Total Datasets"},
        color="Agency"
    )
    fig_bar.update_layout(
        plot_bgcolor="#31363A",
        paper_bgcolor="#31363A",
        font_color="#FFFFFF"
    )

    # === Graph 3: Slope Chart: Year-over-Year for Selected Month ===
    target_month = int(slope_window)
    slope_data = combined_df[combined_df["month"].dt.month == target_month].copy()

    # Get the most recent two years that have data for this month
    available_years = sorted(slope_data["month"].dt.year.unique(), reverse=True)[:2]

    if len(available_years) < 2:
        # Not enough data — fallback empty chart with message
        fig_slope = px.line(
            pd.DataFrame(columns=["MonthLabel", "datasets_created", "Agency"]),
            x="MonthLabel",
            y="datasets_created",
            title=f"📈 Change in Output for {pd.to_datetime(target_month, format='%m').strftime('%B')} (Year-over-Year)",
            labels={"datasets_created": "Datasets Published"}
        )
        fig_slope.add_annotation(
            text="⚠️ Not enough data to compare year-over-year.",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=14, color="red"),
            align="center"
        )
    else:
        # Filter for only the selected years
        slope_data = slope_data[slope_data["month"].dt.year.isin(available_years)]
        slope_data["MonthLabel"] = slope_data["month"].dt.strftime("%b %Y")
        slope_df = slope_data.groupby(["Agency", "MonthLabel"])["datasets_created"].sum().reset_index()

        fig_slope = px.line(
            slope_df,
            x="MonthLabel",
            y="datasets_created",
            color="Agency",
            line_group="Agency",
            markers=True,
            title=f"📈 Change in Output for {pd.to_datetime(target_month, format='%m').strftime('%B')} (Year-over-Year)",
            labels={"datasets_created": "Datasets Published"}
        )

    fig_slope.update_layout(
        plot_bgcolor="#31363A",
        paper_bgcolor="#31363A",
        font_color="#FFFFFF",
    )
    fig_slope.update_traces(marker=dict(size=10)  # Adjust size as needed
    )
    return fig_line, fig_bar, fig_slope
    
# === graph 4: Update Single-Agency Monthly Upload Chart ===
@app.callback(
    Output("monthly-agency-bar", "figure"),
    Input("single-agency", "value"),
    Input("month-range", "value")
)
def update_agency_bar(agency, months_back):
    import pandas as pd
    import plotly.express as px
    import os

    if not agency:
        return px.bar(title="No agency selected")

    file = f"{agency.lower()}_monthly.csv"
    if not os.path.exists(file):
        return px.bar(title=f"No data file found for {agency}")

    df = pd.read_csv(file, parse_dates=["month"])
    cutoff = pd.Timestamp.today().replace(day=1) - pd.DateOffset(months=months_back)
    df = df[df["month"] >= cutoff]

    fig = px.bar(
        df,
        x="month",
        y="datasets_created",
        title=f"{agency} - Monthly Dataset Uploads (Last {months_back} Months)",
        labels={"month": "Month", "datasets_created": "Datasets Published"},
        color_discrete_sequence=["#1f77b4"]
    )
    fig.update_layout(
        plot_bgcolor="#31363A",
        paper_bgcolor="#31363A",
        font_color="#FFFFFF",
        xaxis_tickformat="%b\n%Y"
    )
    return fig
# === RUN THE APP ===
if __name__ == "__main__":
    app.run_server(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 8050)))
server = app.server

