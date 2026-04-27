import pandas as pd

def load_csv(path):
    return pd.read_csv(path)

def load_json(path):
    return pd.read_json(path)

def load_parquet(path):
    return pd.read_parquet(path)