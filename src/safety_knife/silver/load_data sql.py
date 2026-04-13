from pyspark.sql import functions as F
from pyspark.sql import types as T
from safety_knife.spark_utils import get_or_create_spark_session

def main() -> None:
    spark = get_or_create_spark_session(app_name="silver_migration")
    # staged_data_query = f"""
    #  -- Stage DataFrames
    #     WITH staged AS (
    #         SELECT
    #             Date,
    #             Symbol,
    #             CAST(Open AS DOUBLE) AS Open,
    #             CAST(High AS DOUBLE) AS High,
    #             CAST(Low AS DOUBLE) AS Low,
    #             CAST(Close AS DOUBLE) AS Close,
    #             CAST(Volume AS BIGINT) AS Volume,
    #             CAST(Dividends AS DOUBLE) AS Dividends,
    #             CAST(Stock_Splits AS DOUBLE) AS Stock_Splits,
    #             MD5(Symbol) AS Instrument_HK,
    #             MD5(Date_FORMAT(Date, 'yyyy-MM-dd')) AS Calendar_HK,
    #             MD5(CONCAT(Symbol, '|', Date_FORMAT(Date, 'yyyy-MM-dd'))) AS Instrument_Day_LK,
    #             CURRENT_TIMESTAMP AS Load_DTS,
    #             'yfinance' AS Record_Source
    #         FROM yfinance.historicals
    #         WHERE Date IS NOT NULL AND Symbol IS NOT NULL AND LENGTH(Symbol) > 0
    #     )    
    # """

    # spark.sql(f"""
    #    {staged_data_query}
        
    #     -- MERGE loads (idempotent)
    #     MERGE INTO dv_yfinance.hub_instrument t
    #     USING (
    #         SELECT DISTINCT Instrument_HK, Symbol, Load_DTS, Record_Source
    #         FROM staged
    #     ) s
    #     ON t.Instrument_HK = s.Instrument_HK
    #     WHEN NOT MATCHED THEN INSERT (Instrument_HK, Symbol, Load_DTS, Record_Source)
    #     VALUES (s.Instrument_HK, s.Symbol, s.Load_DTS, s.Record_Source);
    # """)

    # spark.sql(f"""
    #    {staged_data_query}
    
    #     MERGE INTO dv_yfinance.hub_calendar t
    #     USING (
    #         SELECT DISTINCT Calendar_HK, Date, Load_DTS, Record_Source
    #         FROM staged
    #     ) s
    #     ON t.Calendar_HK = s.Calendar_HK
    #     WHEN NOT MATCHED THEN INSERT (Calendar_HK, Date, Load_DTS, Record_Source)
    #     VALUES (s.Calendar_HK, s.Date, s.Load_DTS, s.Record_Source);
    # """)

    # spark.sql(f"""
    #    {staged_data_query}    
    
    #     MERGE INTO dv_yfinance.link_instrument_day t
    #     USING (
    #         SELECT DISTINCT Instrument_Day_LK, Instrument_HK, Calendar_HK, Load_DTS, Record_Source
    #         FROM staged
    #     ) s
    #     ON t.Instrument_Day_LK = s.Instrument_Day_LK
    #     WHEN NOT MATCHED THEN INSERT (Instrument_Day_LK, Instrument_HK, Calendar_HK, Load_DTS, Record_Source)
    #     VALUES (s.Instrument_Day_LK, s.Instrument_HK, s.Calendar_HK, s.Load_DTS, s.Record_Source);
    # """)

    # spark.sql("""
    #     select * from dv_yfinance.link_instrument_day limit 10
    #     """).show()
    
    # spark.sql (f"""
    #      {staged_data_query}
    #      select md5(concat(cast(Open as string), cast(High as string), cast(Low as string), cast(Close as string), cast(Volume as string), cast(Dividends as string), cast(Stock_Splits as string)))
    #      from staged limit 10
    #            """).show()
    
    # spark.sql(f"""
    #    {staged_data_query}
    
    #     MERGE INTO dv_yfinance.sat_instrument_day_prices t
    #     USING (
    #         SELECT DISTINCT Instrument_Day_LK,
    #             Open,
    #             High,
    #             Low,
    #             Close,
    #             Volume,
    #             Dividends,
    #             Stock_Splits,
    #             Record_Source,
    #             MD5(CONCAT(CAST(Open AS STRING), CAST(High AS STRING), CAST(Low AS STRING), CAST(Close AS STRING), CAST(Volume AS STRING), CAST(Dividends AS STRING), CAST(Stock_Splits AS STRING), CAST(Record_Source AS STRING))) AS Hash_Diff
    #         FROM staged
    #     ) s
    #     ON t.Instrument_Day_LK = s.Instrument_Day_LK AND t.Hash_Diff = s.Hash_Diff
    #     WHEN NOT MATCHED THEN INSERT (Instrument_Day_LK, Open, High, Low, Close, Volume, Dividends, Stock_Splits, Hash_Diff, Load_DTS, Record_Source)
    #     VALUES (s.Instrument_Day_LK, s.Open, s.High, s.Low, s.Close, s.Volume, s.Dividends, s.Stock_Splits, s.Hash_Diff, CURRENT_TIMESTAMP(), s.Record_Source);
    # """)

    # spark.sql("""
    #     select * from dv_yfinance.sat_instrument_day_prices limit 10
    #     """).show()

    # Load company satellite data (SCD Type 2)
    # Single MERGE handles both closing old versions and inserting new ones
    # spark.sql(f"""
    #     WITH company_staged AS (
    #         SELECT
    #             MD5(c.symbol) AS Instrument_HK,
    #             c.longName,
    #             c.marketCap,
    #             CURRENT_TIMESTAMP AS Load_DTS,
    #             'yfinance' AS Record_Source,
    #             MD5(CONCAT(COALESCE(CAST(c.longName AS STRING), ''), '|', COALESCE(CAST(c.marketCap AS STRING), ''))) AS Hash_Diff
    #         FROM yfinance.company c
    #         WHERE c.symbol IS NOT NULL AND LENGTH(c.symbol) > 0
    #     )
    #     MERGE INTO dv_yfinance.sat_company_info t
    #     USING company_staged s
    #     ON t.Instrument_HK = s.Instrument_HK AND t.End_DTS IS NULL
    #     WHEN MATCHED AND t.Hash_Diff != s.Hash_Diff THEN 
    #         UPDATE SET End_DTS = CURRENT_TIMESTAMP()
    #     WHEN NOT MATCHED THEN 
    #         INSERT (Instrument_HK, longName, marketCap, Hash_Diff, Load_DTS, Effective_DTS, End_DTS, Record_Source)
    #         VALUES (s.Instrument_HK, s.longName, s.marketCap, s.Hash_Diff, s.Load_DTS, s.Load_DTS, NULL, s.Record_Source);
    # """)

    # spark.sql("""
    #     select * from dv_yfinance.sat_company_info limit 10
    #     """).show()

    # # Load minute-level data from bronze
    # # Stage data with all hash keys
    minute_staged_query = f"""
        WITH minute_staged AS (
            SELECT
                DateTime,
                Symbol,
                CAST(Open AS DOUBLE) AS Open,
                CAST(High AS DOUBLE) AS High,
                CAST(Low AS DOUBLE) AS Low,
                CAST(Close AS DOUBLE) AS Close,
                CAST(Volume AS BIGINT) AS Volume,
                CAST(Dividends AS DOUBLE) AS Dividends,
                CAST(Stock_Splits AS DOUBLE) AS Stock_Splits,
                MD5(Symbol) AS Instrument_HK,
                MD5(CAST(DateTime AS STRING)) AS Time_HK,
                MD5(CONCAT(Symbol, '|', CAST(DateTime AS STRING))) AS Instrument_Minute_LK,
                CURRENT_TIMESTAMP AS Load_DTS,
                'yfinance' AS Record_Source,
                MD5(CONCAT(CAST(Open AS STRING), CAST(High AS STRING), CAST(Low AS STRING), CAST(Close AS STRING), CAST(Volume AS STRING), CAST(Dividends AS STRING), CAST(Stock_Splits AS STRING))) AS Hash_Diff
            FROM yfinance.minute_historicals
            WHERE DateTime IS NOT NULL AND Symbol IS NOT NULL AND LENGTH(Symbol) > 0
        )
    """
    
    # MERGE hub_time (idempotent)
    spark.sql(f"""
       {minute_staged_query}
        
        MERGE INTO dv_yfinance.hub_time t
        USING (
            SELECT DISTINCT Time_HK, DateTime, Load_DTS, Record_Source
            FROM minute_staged
        ) s
        ON t.Time_HK = s.Time_HK
        WHEN NOT MATCHED THEN INSERT (Time_HK, DateTime, Load_DTS, Record_Source)
        VALUES (s.Time_HK, s.DateTime, s.Load_DTS, s.Record_Source);
    """)

    # # MERGE link_instrument_minute (idempotent)
    spark.sql(f"""
       {minute_staged_query}
    
        MERGE INTO dv_yfinance.link_instrument_minute t
        USING (
            SELECT DISTINCT Instrument_Minute_LK, Instrument_HK, Time_HK, Load_DTS, Record_Source
            FROM minute_staged
        ) s
        ON t.Instrument_Minute_LK = s.Instrument_Minute_LK
        WHEN NOT MATCHED THEN INSERT (Instrument_Minute_LK, Instrument_HK, Time_HK, Load_DTS, Record_Source)
        VALUES (s.Instrument_Minute_LK, s.Instrument_HK, s.Time_HK, s.Load_DTS, s.Record_Source);
    """)

    # # MERGE sat_instrument_minute_prices (SCD Type 2)
    spark.sql(f"""
       {minute_staged_query}
    
        MERGE INTO dv_yfinance.sat_instrument_minute_prices t
        USING (
            SELECT DISTINCT Instrument_Minute_LK,
                Open,
                High,
                Low,
                Close,
                Volume,
                Dividends,
                Stock_Splits,
                Hash_Diff,
                Load_DTS,
                Record_Source
            FROM minute_staged
        ) s
        ON t.Instrument_Minute_LK = s.Instrument_Minute_LK AND t.End_DTS IS NULL
        WHEN MATCHED AND t.Hash_Diff != s.Hash_Diff THEN 
            UPDATE SET End_DTS = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN 
            INSERT (Instrument_Minute_LK, Open, High, Low, Close, Volume, Dividends, Stock_Splits, Hash_Diff, Load_DTS, Effective_DTS, End_DTS, Record_Source)
            VALUES (s.Instrument_Minute_LK, s.Open, s.High, s.Low, s.Close, s.Volume, s.Dividends, s.Stock_Splits, s.Hash_Diff, s.Load_DTS, s.Load_DTS, NULL, s.Record_Source);
    """)

    spark.sql("""
        select * from dv_yfinance.link_instrument_minute limit 10
        """).show()

    spark.sql("""
        select * from dv_yfinance.sat_instrument_minute_prices limit 10
        """).show()



if __name__ == "__main__":
    main()
