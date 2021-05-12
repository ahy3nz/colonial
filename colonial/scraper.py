from pathlib import Path
from datetime import datetime
import pytz
import requests
import pandas as pd

STATES = ['GA', "TN", "VA", "MD", "PA"]

FUELS_ENDPT = "https://www.gasbuddy.com/assets-v2/api/fuels"
TRENDS_ENDPT = "https://www.gasbuddy.com/assets-v2/api/trends"
HEADER = {"User-Agent": 'foobar'}

def state_scraper():
    results = []
    for state in STATES:
        timestamp = datetime.now(pytz.timezone("US/Eastern"))
        resp = requests.get(
            TRENDS_ENDPT,
            params = {"search": state},
            headers=HEADER
        )
        price = resp.json()['trends']['body'][0]['Today']
        results.append({
            "Location": state,
            "Price": price,
            "Timestamp": timestamp
        })
    new_df = pd.DataFrame(results)

    filename = Path("state_data.csv")
    if filename.exists():
        use_header = False
    else:
        use_header = True

    new_df.to_csv('state_data.csv', mode='a', index=False, header=use_header)

def us_scraper():
    results = []
    timestamp = datetime.now(pytz.timezone("US/Eastern"))
    resp = requests.get(
        TRENDS_ENDPT,
        params = {"search": "USA"},
        headers=HEADER
    )
    price = resp.json()['trends']['body'][-1]['Today']
    results.append({
        "Location": "USA",
        "Price": price,
        "Timestamp": timestamp
    })
    new_df = pd.DataFrame(results)

    filename = Path("us_data.csv")
    if filename.exists():
        use_header = False
    else:
        use_header = True

    new_df.to_csv('us_data.csv', mode='a', index=False, header=use_header)


if __name__ == "__main__":
    #state_scraper()
    us_scraper()

