import os
import json
import yfinance as yf
from pandas import DataFrame
import pandas as pd
from safety_knife.config import DATA_DIR, period, output_path, output_path_minute, minute_period

def _store_historicals(ticker_symbol: str, output_path: str, period: str, interval: str) -> DataFrame:
    # Check if output directory exists, if not create it
    if not os.path.exists(output_path):
        dat = yf.Ticker(ticker_symbol)
        df = dat.history(period=period, interval=interval)

        df = df.reset_index()  # moves index into a column named 'Date'
        print(df.columns.tolist())  # now includes 'Date'
        print(df)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path)
        print(f"Data fetched and saved to {output_path}")
        return df
    else:
        df = DataFrame(pd.read_csv(output_path))
        return df

def store_daily_historicals(ticker_symbol: str) -> DataFrame:
    interval = '1d'
    daily_output_path = f"{output_path}/{ticker_symbol}/historical/daily.csv"
    return _store_historicals(ticker_symbol, daily_output_path, period, interval)

def store_minute_historicals(ticker_symbol: str) -> DataFrame:
    interval = '1m'
    minute_output_path = f"{output_path_minute}/{ticker_symbol}/historical_minute/minute.csv"
    return _store_historicals(ticker_symbol, minute_output_path, minute_period, interval)
                              
def store_info(ticker_symbol: str) -> dict:
    info_output_path = f"{DATA_DIR}/{ticker_symbol}/info/meta.json"
    if not os.path.exists(info_output_path):
        dat = yf.Ticker(ticker_symbol)
        info = dat.info
        print(info)
        os.makedirs(os.path.dirname(info_output_path), exist_ok=True)
        with open(info_output_path, 'w') as f:
            json.dump(info, f)
        print(f"Info data fetched and saved to {info_output_path}")

        return info
    else:
        info = json.load(open(info_output_path, 'r'))
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