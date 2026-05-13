""" @bruin
name: raw.lfs_state
type: python
materialization:
    type: table
connection: duckdb-default
@bruin """

import requests
import pandas as pd

def materialize():
    url = "https://api.data.gov.my/opendosm"
    params = {"id": "lfs_qtr_state", "limit": 5000}
    response = requests.get(url, params=params)
    data = response.json()

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["quarter"] = df["date"].dt.quarter
    return df
