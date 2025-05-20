import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.express as px
import os

# ===== STEP 3: DASHBOARD IT======
app = dash.Dash(__name__)
server = app.server 
app.title = "Dataset Transparency Dashboard"

# Update your layout
app.layout = html.Div(
    style={
        'backgroundColor': 'black',
        'color': 'white',
        'fontFamily': 'Arial',
        'padding': '20px'
    },
    children=[
        html.H1("Inkwell Global Report: Dataset Transparency Trends", style={'textAlign': 'center'}),

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
                "backgroundColor": #1a1a1a,
                "color": "#ffffff",
            }
        ),

        dcc.Graph(id='line-graph'),
        html.Div(id='line-graph-explanation'),
        dcc.Graph(id='bar-graph'),

        html.Label("Compare Change In Number of Datasets Released (Month Over Year):", style={"marginTop": "20px"}),
        dcc.Dropdown(
            id='slope-month',
            options=[{"label": month, "value": i} for i, month in enumerate([
                "January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"
            ], 1)],
            value=(pd.Timestamp.today().month - 1) or 12,  # defaults to last month
            clearable=False,
            style={
                "width": "200px",
                "marginBottom": "20px",
                "backgroundColor": "#1e1e1e",
                "color": "#ffffff",
            }
        ),

        dcc.Graph(id='slope-graph'),


                
        html.H2("Single Agency View", style={"marginTop": "40px"}),

        html.Label("Select Agency:"),
        dcc.Dropdown(
            id='single-agency',
            options=[{"label": agency, "value": agency} for agency in [
                "CDC", "DOJ", "EPA", "HHS", "NSF", "NOAA", "USDA"
            ]],
            value="CDC",
            clearable=False,
            style={
                "width": "40px",
                "marginBottom": "30px",
                "backgroundColor": "#1e1e1e",
                "color": "#ffffff",
            }
        ),
        html.Div(
            "Data from data.cdc.gov & catalog.data.gov",
            style={'textAlign': 'center', 'fontStyle': 'italic'}
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
    Output('line-graph-explanation', 'children'),
    Input('month-window', 'value'),
    Input('slope-month', 'value')
)
def update_graphs(months_back, slope_window):
    # Load all monthly data files
    combined_df = pd.read_csv("data/combined_monthly.csv", parse_dates=['month'])

    # Filter for selected number of months
    cutoff = pd.Timestamp.today().replace(day=1) - pd.DateOffset(months=months_back)
    recent_df = combined_df[combined_df["month"] >= cutoff]

    # === Graph 1: Line Chart with Drop-off Detection
    fig_line = px.line(
        recent_df,
        x="month",
        y="normalized",
        color="Agency",
        title=f"Dataset Releases (Normalized, Last {months_back} Months)",
        labels={"month": "Month", "normalized": "Relative Activity"},
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
            if previous and previous > 0 and row["normalized"] == 0:
                dropoff_lines.append({"agency": agency, "date": row["month"]})
            previous = row["normalized"]
    for event in dropoff_lines:
        fig_line.add_vline(
            x=event["date"],
            line_dash="dash",
            line_color="red",
            annotation_text=f"{event['agency']} drop-off",
            annotation_position="top left"
        )
    # === explaination of normalized graphs ===
    explanation = html.Div(
    children=f"""
        📈 This graph shows dataset publication trends over the last {months_back} months for each agency. 
        The values are normalized: each point represents the number of datasets published in that period 
        as a proportion of the busiest month on record for that agency (since 2010).

        A value of 1.0 means the agency matched or exceeded its historical peak. 
        Lower values indicate less activity relative to its own past—not compared to other agencies.

        This normalization allows fair trend comparisons without larger publishers like NOAA flattening the scale.
    """,
    style={
        "marginTop": "10px",
        "marginBottom": "20px",
        "fontSize": "14px",
        "lineHeight": "1.6",
        "color": "#CCCCCC",
        "maxWidth": "800px",
        "display": "flex", 
        "justifyContent": "center"},)
        
    # === Graph 2: Bar Chart of Totals
    total_recent = recent_df.groupby("Agency")["datasets_created"].sum().reset_index()
    fig_bar = px.bar(
        total_recent,
        x="Agency",
        y="datasets_created",
        title=f"Total Datasets Published (Last {months_back} Months)",
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
    available_years = sorted(slope_data["month"].dt.year.unique(), reverse=True)[:3]

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
    return fig_line, fig_bar, fig_slope, explanation
    
# === graph 4: Update Single-Agency Monthly Upload Chart ===
@app.callback(
    Output("monthly-agency-bar", "figure"),
    Input("single-agency", "value"),
    Input("month-range", "value")
)
def update_agency_bar(agency, months_back):

    if not agency:
        return px.bar(title="No agency selected")

    file = f"data/{agency.lower()}_monthly.csv"
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
if __name__ == '__main__':
    app.run_server(debug=True, port=8051)
