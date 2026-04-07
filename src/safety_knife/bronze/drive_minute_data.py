from safety_knife.config import ticker_symbol, DATA_DIR, output_path_minute
from safety_knife.bronze.fetch_data import retrieve_minute_data
from safety_knife.bronze.store_data import save_to_delta, load_from_delta, get_or_create_spark_session
from safety_knife.bronze.wrangle_data import prepare_bronze_output

# Create a single Spark session with Delta support for this entire script
spark = get_or_create_spark_session(app_name="bronze_etl_drive")

#fetch data from api if not in cache.
#cache acts like a historical record of the raw data retrieved.
pd_df = retrieve_minute_data(ticker_symbol=ticker_symbol)

spark_df = prepare_bronze_output(spark, pd_df, ticker_symbol, is_minute=True)
# spark_df.show()

save_to_delta(spark_df, path=output_path_minute, partition="Symbol")

df0 = load_from_delta(output_path_minute, spark=spark)
df0.show()

spark.sql("""
    SELECT * FROM yfinance.minute_historicals
""").show()