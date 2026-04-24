from safety_knife.config import ticker_symbol, DATA_DIR, output_path_historical, company_output_path
from safety_knife.bronze.fetch_data import retrieve_daily_data
from safety_knife.bronze.store_data import load_to_spark, save_to_delta, load_from_delta, get_or_create_spark_session, upsert_to_delta, upsert_historicals_by_path
from safety_knife.bronze.wrangle_data import rename_stock_splits_column, convert_date_column, add_symbol_column, prepare_data, add_price_to_earnings_column, rename_column, prepare_company_data
import pandas as pd

from dotenv import load_dotenv
load_dotenv()

# Create a single Spark session with Delta support for this entire script
spark = get_or_create_spark_session(app_name="bronze_etl_drive")

#fetch data from api if not in cache.
#cache acts like a historical record of the raw data retrieved.
print(ticker_symbol)
pd_df, info = retrieve_daily_data(ticker_symbol=ticker_symbol)
# print(pd_df)
# print(info)

def populate_yfinance_company_delta(spark, info):
    # Convert the info dictionary to a pandas DataFrame
    df_info = pd.DataFrame([info])
    print(df_info)
    
    company_columns = ["symbol", "longName", "marketCap", "currentPrice", "trailingEps", "dividendYield", "totalRevenue", "netIncomeToCommon", "bookValue", "priceToBook", "forwardEps"]
    df_info = df_info[company_columns]
    print(df_info)

    spark_df_info = spark.createDataFrame(df_info)
    spark_df_info.show()

    spark_df_info = add_price_to_earnings_column(spark_df_info)
    spark_df_info = rename_column(spark_df_info, "netIncomeToCommon", "netIncome")
    spark_df_info = rename_column(spark_df_info, "bookValue", "bookValuePerShare")
    spark_df_info = prepare_company_data(spark_df_info)
    spark_df_info.show()
    # Save the DataFrame to a Delta table
    print(company_output_path)
    # save_to_delta(spark_df=spark_df_info, path=company_output_path)
    upsert_to_delta(spark=spark, incoming_df=spark_df_info, delta_path=company_output_path)

def populate_yfinance_historicals_delta(spark, pd_df):
    print(pd_df)
    print(pd_df.index.name)
    spark_df = load_to_spark(pd_df, spark=spark)

    spark_df.show()   
    spark_df = rename_stock_splits_column(spark_df)
    spark_df.show()
    spark_df = convert_date_column(spark_df)
    spark_df.show()
    spark_df = add_symbol_column(spark_df, ticker_symbol)

    spark_df = prepare_data(spark_df, is_minute=False)
    spark_df.show()
    spark_df.printSchema()

    # save_to_delta(spark_df, path=output_path_historical, partition="Symbol")
    upsert_historicals_by_path(spark=spark, incoming_df=spark_df, delta_path=output_path_historical)

    df0 = load_from_delta(output_path_historical, spark=spark)
    df0.show()

# Call the function with the retrieved info
populate_yfinance_company_delta(spark, info)
print(company_output_path)
spark.sql("""
    SELECT * FROM yfinance.company
""").show()

# populate_yfinance_historicals_delta(spark, pd_df)
# spark.sql("""
#     SELECT * FROM yfinance.historicals
# """).show()