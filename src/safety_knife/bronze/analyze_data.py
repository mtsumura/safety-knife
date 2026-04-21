from safety_knife.config import DATA_DIR, ticker_symbol, output_path_historical, company_output_path
from safety_knife.bronze.store_data import load_from_delta, show_table_history, get_or_create_spark_session

# df0 = load_from_delta(output_path_historical)
# print(df0.count())
# df0.show()



spark = get_or_create_spark_session(app_name="qa_bronze")

# spark.sql(""" 
# select * from yfinance.company
# """).show()
spark.sql("""
    SELECT * FROM yfinance.historicals where Symbol = 'CSU.TO' order by volume desc limit 10
    -- SELECT min(Close) FROM yfinance.historicals
    -- SELECT * FROM yfinance.historicals WHERE Symbol = 'CSU.TO' order by Date desc limit 10

    --SELECT
    --     COUNT(DISTINCT Symbol) AS n_symbols,
    --     MIN(Date) AS first_date,
    --     MAX(Date) AS last_date,
    --     COUNT(*) AS n_rows
    --     FROM yfinance.historicals;
""").show()

# spark.sql(
#     """
#     SELECT Symbol, MAX(Close) AS ath_close, MIN(Close) AS atl_close
#     FROM yfinance.historicals
#     GROUP BY Symbol;
# """).show()

# spark.sql(
#     """
#     WITH ordered AS (
#     SELECT
#         Symbol, Date, Close,
#         LAG(Close) OVER (PARTITION BY Symbol ORDER BY Date) AS prev_close
#     FROM yfinance.historicals
#    )
#    SELECT Symbol, Date, Close, prev_close,
#        (Close - prev_close) / prev_close AS daily_return
#    FROM ordered
#    WHERE prev_close IS NOT NULL
#    ORDER BY daily_return DESC
#    LIMIT 10;
#  """).show()

# spark.sql("""
# with prev_ex as (
#   select Date, Close, 
#   LAG(Close) over (partition by Symbol order by date) as prev_close
#    from yfinance.historicals
#   order by date
#   )
#   select Date, Close, prev_close, close - prev_close as diff, ((close - prev_close) / prev_close) * 100 as pct
#   from prev_ex
#   order by pct desc
# """).show()





# spark.sql("""
# with volume_ex as (
# select
# Symbol,
# AVG(Volume) as avg_volume
# from yfinance.historicals 
# where Symbol = 'CSU.TO'
# group by Symbol
# )
# select h.date, h.volume, v.avg_volume, (h.volume - v.avg_volume) as diff
#     from yfinance.historicals h
#     join volume_ex v on h.Symbol = v.Symbol
#     order by diff desc
#     limit 10
# """).show()

# spark.sql(
# """
#   WITH stats AS (
#   SELECT Symbol, Date, Volume,
#          AVG(Volume) OVER (PARTITION BY Symbol) AS avg_vol,
#          STDDEV(Volume) OVER (PARTITION BY Symbol) AS std_vol
#   FROM yfinance.historicals
# )
# SELECT Symbol, Date, Volume, avg_vol,
#        (Volume - avg_vol) / NULLIF(std_vol, 0) AS z_score
# FROM stats
# WHERE std_vol > 0
# ORDER BY z_score DESC
# LIMIT 20;
#  """).show()

# spark.sql(""" 
# select * from yfinance.historicals
# where dividends > 0 or stock_splits > 0
# order by Date asc
# """).show()