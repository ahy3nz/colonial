import time
import threading
import streamlit as st
import altair as alt
import pandas as pd

import colonial
from colonial.scraper import state_scraper, us_scraper, station_scraper

st.title("Gas buddy")

df = pd.read_csv("state_data.csv")
df['Timestamp'] = pd.to_datetime(df['Timestamp'])

chart = alt.Chart(df).mark_line(point=True).encode(
        x='Timestamp:T',
        y='Price:Q',
        color='Location:N',
        tooltip=['Location', 'Price', 'Timestamp']
).configure_point(
    size=200
)

st.altair_chart(chart, use_container_width=True)

def state_scrape_thread():
    state_thread = threading.Thread(target=state_scraper)
    state_thread.start()
    state_thread.join()
    time.sleep(30)

def us_scrape_thread():
    us_thread = threading.Thread(target=us_scraper)
    us_thread.start()
    us_thread.join()
    time.sleep(30)

def station_scrape_thread():
    station_thread = threading.Thread(target=station_scraper)
    station_thread.start()
    station_thread.join()
    time.sleep(30)

