import random
from pathlib import Path
from datetime import datetime
import logging
import time
import pytz
import requests
import pandas as pd

from colonial.data import upload_s3, pull_secret

logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(levelname)s:%(asctime)s:%(message)s',
     level=logging.DEBUG
)

STATES = ['GA', "TN", "VA", "MD", "PA"]

FUELS_ENDPT = "https://www.gasbuddy.com/assets-v2/api/fuels"
TRENDS_ENDPT = "https://www.gasbuddy.com/assets-v2/api/trends"
HEADER = {"User-Agent": 'foobar'}

def station_scraper():
    results = []
    stations = {
        "Costco1": pull_secret("COSTCO1"),
        "Costco2": pull_secret("COSTCO2")
        "Exxon1": pull_secret("EXXON1")
    }
    for name, station in stations.items():
        logger.debug(f"Scraping station {station}")
        time.sleep(15*random.random() + 5)
        resp = requests.get(
            FUELS_ENDPT,
            params={"stationIds": station},
            headers=HEADER
        )
        regular = [*filter(lambda x: x['id']==1, resp.json()['fuels'])]
        if len(regular) > 0:
            results.append({
                'Location': name,
                'Price': regular[0]['prices'][0]['price'],
                'Timestamp': datetime.strptime(
                    regular[0]['prices'][0]['postedTime'],
                    '%Y-%m-%dT%H:%M:%S.%f%z'
                ).astimezone(
                    pytz.timezone("US/Eastern")
                )
            })

    new_df = pd.DataFrame(results)
    upload_s3(new_df, 'station_data.csv')

    #filename = Path("staticdata/station_data.csv")
    #if filename.exists():
    #    use_header = False
    #else:
    #    use_header = True

    #new_df.to_csv('staticdata/station_data.csv', mode='a', index=False, header=use_header)


def state_scraper():
    results = []
    for state in STATES:
        logger.debug(f"Scraping state {state}")
        time.sleep(15*random.random() + 5)
        timestamp = datetime.now(pytz.timezone("US/Eastern"))
        resp = requests.get(
            TRENDS_ENDPT,
            params={"search": state},
            headers=HEADER
        )
        if resp.status_code == 200:
            price = resp.json()['trends']['body'][0]['Today']
            results.append({
                "Location": state,
                "Price": price,
                "Timestamp": timestamp
            })
    new_df = pd.DataFrame(results)
    upload_s3(new_df, 'state_data.csv')

    #filename = Path("staticdata/state_data.csv")
    #if filename.exists():
    #    use_header = False
    #else:
    #    use_header = True

    #new_df.to_csv('staticdata/state_data.csv', mode='a', index=False, header=use_header)

def us_scraper():
    results = []
    logger.debug(f"Scraping US")
    time.sleep(15*random.random() + 5)
    timestamp = datetime.now(pytz.timezone("US/Eastern"))
    resp = requests.get(
        TRENDS_ENDPT,
        params={"search": "USA"},
        headers=HEADER
    )
    if resp.status_code == 200:
        price = resp.json()['trends']['body'][-1]['Today']
        results.append({
            "Location": "USA",
            "Price": price,
            "Timestamp": timestamp
        })
        new_df = pd.DataFrame(results)
        upload_s3(new_df, 'us_data.csv')

    #filename = Path("staticdata/us_data.csv")
    #if filename.exists():
    #    use_header = False
    #else:
    #    use_header = True

    #new_df.to_csv('staticdata/us_data.csv', mode='a', index=False, header=use_header)


if __name__ == "__main__":
    station_scraper()

    state_scraper()

    us_scraper()

