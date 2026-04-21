from safety_knife.bronze.cache import store_daily_historicals, store_info, store_minute_historicals
from pandas import DataFrame
# # Save the raw data to a CSV file in the bronze layer
from safety_knife.config import ticker_symbol

def retrieve_daily_data(ticker_symbol) -> tuple[DataFrame, dict]:
    df = store_daily_historicals(ticker_symbol)
    info = store_info(ticker_symbol)
    return df, info

def retrieve_minute_data(ticker_symbol) -> DataFrame:
    df = store_minute_historicals(ticker_symbol)
    return df

# df = retrieve_minute_data(ticker_symbol=ticker_symbol)
# print(df)
# print(info)
