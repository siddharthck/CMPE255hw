import dash
from dash import dcc, html
import plotly.express as px
import pandas as pd
from google.cloud import bigquery
from dash.dependencies import Input, Output
import folium
from folium.plugins import HeatMap
import streamlit as st
from streamlit_folium import folium_static
from google.oauth2 import service_account
import toml
import os

# Load the TOML file
config = toml.load(".streamlit/secrets.toml")

# Set the credentials using the content of the TOML file
credentials_info = config["google_cloud"]
credentials = service_account.Credentials.from_service_account_info(credentials_info)

# Initialize Dash app
app = dash.Dash(__name__)

# BigQuery client setup
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Define BigQuery dataset and table
PROJECT_ID = credentials.project_id  # Replace with your GCP project ID
DATASET_ID = 'SanJoseServiceRequest'  # Replace with your dataset name
TABLE_ID = 'SJSR'  # Replace with your table name

# Load data from BigQuery
query = f"""
SELECT * FROM {PROJECT_ID}.{DATASET_ID}.{TABLE_ID} """
df = client.query(query).to_dataframe()

# Convert date columns
df["Date_Created"] = pd.to_datetime(df["Date_Created"])

# Dash Layout
app.layout = html.Div([
    html.H1("BeautifySJ Incident Dashboard", style={'textAlign': 'center'}),

    html.Label("Select Category:"),
    dcc.Dropdown(
        id='category-dropdown',
        options=[{"label": cat, "value": cat} for cat in df["Category"].dropna().unique()],
        value=None,
        placeholder="All Categories"
    ),

    html.Label("Select Service Type:"),
    dcc.Dropdown(
        id='service-dropdown',
        options=[{"label": svc, "value": svc} for svc in df["Service_Type"].dropna().unique()],
        value=None,
        placeholder="All Service Types"
    ),

    dcc.Graph(id="category-bar-chart"),
    dcc.Graph(id="incident-trend-chart"),
    html.Div(id="incident-heatmap")
])

# Callbacks for interactive filtering
@app.callback(
    [Output("category-bar-chart", "figure"),
     Output("incident-trend-chart", "figure"),
     Output("incident-heatmap", "children")],
    [Input("category-dropdown", "value"),
     Input("service-dropdown", "value")]
)
def update_charts(selected_category, selected_service):
    filtered_df = df.copy()

    if selected_category:
        filtered_df = filtered_df[filtered_df["Category"] == selected_category]
    
    if selected_service:
        filtered_df = filtered_df[filtered_df["Service_Type"] == selected_service]

    # Bar Chart
    category_counts = filtered_df["Category"].value_counts().reset_index()
    bar_fig = px.bar(
        category_counts,
        x=category_counts.index, y="Category",
        labels={"index": "Category", "Category": "Count"},
        title="Incident Distribution by Category"
    )

    # Time Series Chart
    df_trend = filtered_df.set_index("Date_Created").resample("M").size().reset_index(name="Count")
    trend_fig = px.line(
        df_trend, x="Date_Created", y="Count",
        markers=True, title="Incident Trends Over Time"
    )

    # Geolocation visualization in Streamlit
    m = folium.Map(location=[df['Latitude'].mean(), df['Longitude'].mean()], zoom_start=12)
    heat_data = list(zip(df['Latitude'], df['Longitude']))
    HeatMap(heat_data).add_to(m)
    map_html = m._repr_html_()

    # Save processed data back to BigQuery
    destination_table = f"{PROJECT_ID}.{DATASET_ID}.processed_data"
    df.to_gbq(destination_table, project_id=PROJECT_ID, if_exists='replace', credentials=credentials)

    return bar_fig, trend_fig, html.Iframe(srcDoc=map_html, width='100%', height='600')

# Run Dash app and save to HTML
if __name__ == "__main__":
    print("Running Dash app")
    app.run_server(debug=False)
