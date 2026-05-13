""" @bruin
name: raw.citizenship_renunciation
type: python
materialization:
    type: table
connection: duckdb-default
@bruin """

import pandas as pd

def materialize():
    df = pd.read_csv("/home/user/bruin/malaysia-talent-drain/seeds/citizenship_renunciation.csv")
    return df
