import time
import threading
import streamlit as st
import altair as alt
import pandas as pd

from colonial.data import get_s3filesystem, GASBUDDY_BUCKET
from colonial.scraper import state_scraper, us_scraper, station_scraper

def main():
    st.title("Gas buddy interactive chart")
    
    df = load_data()
    
    chart = alt.Chart(df).mark_line(point=True).encode(
            x='Timestamp:T',
            y=alt.Y('Price:Q', scale=alt.Scale(zero=False)),
            color='Location:N',
            tooltip=['Location', 'Price', 'Timestamp']
    ).configure_point(
        size=200
    ).properties(
        height=500
    ).interactive()
    
    st.altair_chart(chart, use_container_width=True)


def load_data():
    csvfiles = ['station_data.csv', 'state_data.csv', 'us_data.csv']
    s3 = get_s3filesystem()
    all_dfs = []
    for csv in csvfiles:
        with s3.open(f'{GASBUDDY_BUCKET}/{csv}', 'r') as f:
            all_dfs.append(pd.read_csv(f))
    df = pd.concat(all_dfs)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])

    return df


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


if __name__ == "__main__":
    main()
