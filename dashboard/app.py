import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

st.set_page_config(page_title="Flight Performance Dashboard", layout="wide")

DB_URL = "postgresql://airflow:airflow@localhost:5432/pipeline_db"
engine = create_engine(DB_URL)

@st.cache_data
def load_metrics():
    return pd.read_sql("SELECT COUNT(*) as total_flights, COUNT(DISTINCT reporting_airline) as airlines, COUNT(DISTINCT origin) as airports FROM staging.flight_raw", engine)

@st.cache_data
def load_carriers():
    return pd.read_sql("SELECT carrier_name, reliability_score, total_flights, cancellation_rate FROM marts.mart_carrier_performance ORDER BY reliability_score DESC", engine)

@st.cache_data
def load_delays():
    return pd.read_sql("SELECT primary_delay_cause, SUM(flight_count) as flights FROM marts.mart_delay_analysis WHERE primary_delay_cause != 'None' GROUP BY primary_delay_cause ORDER BY flights DESC", engine)

@st.cache_data
def load_time_of_day():
    return pd.read_sql("SELECT dep_time_period, ROUND(AVG(avg_arr_delay_minutes)::numeric, 1) as avg_delay FROM marts.mart_delay_analysis GROUP BY dep_time_period ORDER BY avg_delay DESC", engine)

@st.cache_data
def load_routes():
    return pd.read_sql("SELECT route, carrier_name, total_flights, on_time_rate, cancellation_rate FROM marts.mart_route_analysis WHERE total_flights >= 100 ORDER BY on_time_rate ASC LIMIT 10", engine)

@st.cache_data
def load_weekend():
    return pd.read_sql("SELECT is_weekend, ROUND(AVG(avg_arr_delay_minutes)::numeric, 1) as avg_delay, SUM(flight_count) as total_flights FROM marts.mart_delay_analysis GROUP BY is_weekend", engine)

@st.cache_data
def load_busiest_airports():
    return pd.read_sql("SELECT iata_code, airport_name, total_departures, avg_departure_delay FROM marts.mart_airport_performance ORDER BY total_departures DESC LIMIT 10", engine)

@st.cache_data
def load_delayed_airports():
    return pd.read_sql("SELECT iata_code, airport_name, total_departures, avg_departure_delay FROM marts.mart_airport_performance WHERE total_departures >= 500 ORDER BY avg_departure_delay DESC NULLS LAST LIMIT 10", engine)

@st.cache_data
def load_distance():
    return pd.read_sql("SELECT distance_bucket, ROUND(AVG(avg_arr_delay_minutes)::numeric, 1) as avg_delay, SUM(flight_count) as total_flights FROM marts.mart_delay_analysis WHERE distance_bucket IS NOT NULL GROUP BY distance_bucket ORDER BY avg_delay DESC", engine)

# Title
st.title("✈️ Flight Performance Dashboard")
st.caption("Source: BTS On-Time Data | January 2024")

# Section 1 - Key Metrics
metrics = load_metrics().iloc[0]
col1, col2, col3 = st.columns(3)
col1.metric("Total Flights", f"{int(metrics['total_flights']):,}")
col2.metric("Airlines", int(metrics['airlines']))
col3.metric("Airports", int(metrics['airports']))

st.divider()

# Section 2 - Carrier Reliability
st.subheader("Carrier Reliability Score")
carriers = load_carriers()
fig1 = px.bar(carriers, x="reliability_score", y="carrier_name",
              orientation="h", color="reliability_score",
              color_continuous_scale="RdYlGn",
              labels={"reliability_score": "Reliability Score", "carrier_name": "Airline"})
fig1.update_layout(yaxis={"categoryorder": "total ascending"})
st.plotly_chart(fig1, use_container_width=True)

st.divider()

# Section 3 - Delay Causes
st.subheader("Delay Causes Breakdown")
delays = load_delays()
fig2 = px.pie(delays, values="flights", names="primary_delay_cause",
              title="What causes most delays?")
st.plotly_chart(fig2, use_container_width=True)

st.divider()

# Section 4 - Time of Day
st.subheader("Delays by Time of Day")
tod = load_time_of_day()
fig3 = px.bar(tod, x="dep_time_period", y="avg_delay",
              color="avg_delay", color_continuous_scale="RdYlGn_r",
              labels={"dep_time_period": "Time of Day", "avg_delay": "Avg Delay (mins)"})
st.plotly_chart(fig3, use_container_width=True)

st.divider()

# Section 5 - Worst Routes
st.subheader("Top 10 Most Unreliable Routes")
routes = load_routes()
st.dataframe(routes, use_container_width=True)

st.divider()

# Section 6 - On Time Rate by Airline
st.subheader("On Time Arrival Rate by Airline (%)")
carriers_sorted = carriers.sort_values("reliability_score", ascending=True)
fig4 = px.bar(carriers_sorted, x="reliability_score", y="carrier_name",
              orientation="h", color="reliability_score",
              color_continuous_scale="RdYlGn",
              labels={"reliability_score": "On Time Rate (%)", 
                      "carrier_name": "Airline"},
              title="Which airline gets you there on time?")
st.plotly_chart(fig4, use_container_width=True)

st.divider()

# Section 7 - Best vs Worst Airports
st.subheader("Airport Performance")
col1, col2 = st.columns(2)
with col1:
    st.write("Busiest Airports by Departures")
    st.dataframe(load_busiest_airports(), use_container_width=True)
with col2:
    st.write("Most Delayed Airports")
    st.dataframe(load_delayed_airports(), use_container_width=True)

st.divider()

# Section 8 - Cancellation Rate
st.subheader("Cancellation Rate by Airline (%)")
fig5 = px.bar(carriers, x="cancellation_rate", y="carrier_name",
              orientation="h", color="cancellation_rate",
              color_continuous_scale="Reds",
              labels={"cancellation_rate": "Cancellation Rate (%)", "carrier_name": "Airline"})
fig5.update_layout(yaxis={"categoryorder": "total ascending"})
st.plotly_chart(fig5, use_container_width=True)

st.divider()

# Section 9 - Distance vs Delay
st.subheader("Avg Delay by Flight Distance")
distance = load_distance()
fig6 = px.bar(distance, x="distance_bucket", y="avg_delay",
              color="avg_delay", color_continuous_scale="RdYlGn_r",
              labels={"distance_bucket": "Flight Distance", "avg_delay": "Avg Delay (mins)"})
st.plotly_chart(fig6, use_container_width=True)
