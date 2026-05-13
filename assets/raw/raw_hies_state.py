""" @bruin
name: raw.hies_state
type: python
materialization:
    type: table
connection: duckdb-default
@bruin """

import requests
import pandas as pd

def materialize():
    url = "https://api.data.gov.my/opendosm"
    params = {"id": "hies_state", "limit": 1000}
    response = requests.get(url, params=params)
    data = response.json()

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    return df
