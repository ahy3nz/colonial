import logging
import time
import threading
import streamlit as st
import altair as alt
import pandas as pd

from colonial.data import get_s3filesystem, GASBUDDY_BUCKET
from colonial.scraper import state_scraper, us_scraper, station_scraper

logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(levelname)s:%(asctime)s:%(message)s',
    level=logging.DEBUG
)

def main():
    logger.debug("Launching main app")
    st.title("Gas buddy interactive chart")
    st.text("Costco gas stations are in VA")
    
    df = load_data()
    
    logger.debug("Creating chart")
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
    
    mychart = st.altair_chart(chart, use_container_width=True)
    
    while True:
        logger.debug("Launching scraping threads")
        t1 = state_scrape_thread()
        t2 = us_scrape_thread()
        t3 = station_scrape_thread()

        t1.join()
        t2.join()
        t3.join()
        logger.debug("Adding rows to chart")
        mychart.add_rows(load_data())
        time.sleep(10 * 60)


def load_data():
    logger.debug("Loading data from S3")
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
    st.report_thread.add_report_ctx(state_thread)
    state_thread.start()

    return state_thread


def us_scrape_thread():
    us_thread = threading.Thread(target=us_scraper)
    st.report_thread.add_report_ctx(us_thread)
    us_thread.start()

    return us_thread

def station_scrape_thread():
    station_thread = threading.Thread(target=station_scraper)
    st.report_thread.add_report_ctx(station_thread)
    station_thread.start()

    return station_thread

if __name__ == "__main__":
    main()
