""" @bruin
name: raw.exchange_rate
type: python
materialization:
    type: table
connection: duckdb-default
@bruin """

import requests
import pandas as pd

def materialize():
    records = []
    for year in range(2015, 2026):
        for month in [1, 7]:
            date = f"{year}-{month:02d}-01"
            url = f"https://api.frankfurter.app/{date}"
            response = requests.get(url, params={"from": "MYR", "to": "SGD"})
            if response.status_code == 200:
                data = response.json()
                if "rates" in data and "SGD" in data["rates"]:
                    records.append({
                        "date": data["date"],
                        "year": year,
                        "month": month,
                        "myr_to_sgd": data["rates"]["SGD"]
                    })

    df = pd.DataFrame(records)
    return df
