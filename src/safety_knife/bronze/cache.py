import json
from io import BytesIO, StringIO

import yfinance as yf
from pandas import DataFrame
import pandas as pd
from safety_knife.bronze.storage import get_bronze_raw_backend
from safety_knife.config import period, minute_period


def _csv_key(ticker_symbol: str, *, kind: str) -> str:
    # Keep a stable object layout across local and Azure.
    # Example: VET.TO/historical/daily.csv
    if kind == "daily":
        return f"{ticker_symbol}/historical/daily.csv"
    if kind == "minute":
        return f"{ticker_symbol}/historical_minute/minute.csv"
    raise ValueError(f"Unknown csv kind: {kind!r}")


def _info_key(ticker_symbol: str) -> str:
    return f"{ticker_symbol}/info/meta.json"


def _store_historicals(ticker_symbol: str, *, period: str, interval: str, key: str) -> DataFrame:
    backend = get_bronze_raw_backend()

    if not backend.exists(key):
        dat = yf.Ticker(ticker_symbol)
        df = dat.history(period=period, interval=interval)

        df = df.reset_index()  # moves index into a column named 'Date'
        print(df.columns.tolist())  # now includes 'Date'
        print(df)

        buf = StringIO()
        df.to_csv(buf, index=False)
        backend.write_bytes(key, buf.getvalue().encode("utf-8"), content_type="text/csv")
        print(f"Data fetched and saved to {key}")
        return df
    else:
        raw = backend.read_bytes(key)
        df = DataFrame(pd.read_csv(BytesIO(raw)))
        return df


def store_daily_historicals(ticker_symbol: str) -> DataFrame:
    interval = '1d'
    key = _csv_key(ticker_symbol, kind="daily")
    return _store_historicals(ticker_symbol, period=period, interval=interval, key=key)

def store_minute_historicals(ticker_symbol: str) -> DataFrame:
    interval = '1m'
    key = _csv_key(ticker_symbol, kind="minute")
    return _store_historicals(ticker_symbol, period=minute_period, interval=interval, key=key)
                              
def store_info(ticker_symbol: str) -> dict:
    backend = get_bronze_raw_backend()
    key = _info_key(ticker_symbol)

    if not backend.exists(key):
        dat = yf.Ticker(ticker_symbol)
        info = dat.info
        print(info)
        backend.write_bytes(key, json.dumps(info).encode("utf-8"), content_type="application/json")
        print(f"Info data fetched and saved to {key}")

        return info
    else:
        info = json.loads(backend.read_bytes(key).decode("utf-8"))
        return info


# Move these functions to cache.py

# def delete_files_in_dir(directory):
#     for filename in os.listdir(directory):
#         file_path = os.path.join(directory, filename)
#         try:
#             if os.path.isfile(file_path) or os.path.islink(file_path):
#                 os.unlink(file_path)
#             elif os.path.isdir(file_path):
#                 shutil.rmtree(file_path)
#         except Exception as e:
#             print(f'Failed to delete {file_path}. Reason: {e}')