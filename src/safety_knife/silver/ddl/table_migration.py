from safety_knife.config import silver_output_path
from safety_knife.spark_utils import get_or_create_spark_session

spark = get_or_create_spark_session(app_name="silver_migration")
print(f"{silver_output_path=}")
silver_schema = "dv_yfinance"

print(spark.conf.get("spark.sql.catalogImplementation"))
print(spark.conf.get("spark.sql.warehouse.dir"))

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {silver_schema}
    """)

# spark.sql(f"""
#           DROP TABLE {silver_schema}.hub_instrument
#           """)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver_schema}.hub_instrument (
        Instrument_HK STRING NOT NULL,   -- hash(Symbol)
        Symbol        STRING NOT NULL,   -- business key
        Load_DTS      TIMESTAMP NOT NULL,
        Record_Source STRING NOT NULL
)
USING DELTA
LOCATION '{silver_output_path}/hub_instrument';          
    """)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_schema}.hub_calendar (
  Calendar_HK   STRING NOT NULL,   -- hash(Date)
  Date          DATE   NOT NULL,   -- business key
  Load_DTS      TIMESTAMP NOT NULL,
  Record_Source STRING NOT NULL
)
USING DELTA
LOCATION '{silver_output_path}/hub_calendar';     
    """)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_schema}.link_instrument_day (
  Instrument_Day_LK STRING NOT NULL,  -- hash(Symbol, Date)
  Instrument_HK     STRING NOT NULL,  -- → hub_instrument
  Calendar_HK       STRING NOT NULL,  -- → hub_calendar
  Load_DTS          TIMESTAMP NOT NULL,
  Record_Source     STRING NOT NULL
)
USING DELTA
LOCATION '{silver_output_path}/link_instrument_day';
    """)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_schema}.sat_instrument_day_prices (
  Instrument_Day_LK STRING   NOT NULL,  -- parent key
  Open              DOUBLE,
  High              DOUBLE,
  Low               DOUBLE,
  Close             DOUBLE,
  Volume            BIGINT,
  Dividends         DOUBLE,
  Stock_Splits      DOUBLE,
  -- optional: hash of all descriptive attributes for change detection
  Hash_Diff         STRING,
  Load_DTS          TIMESTAMP NOT NULL,
  Record_Source     STRING   NOT NULL
)
USING DELTA
LOCATION '{silver_output_path}/sat_instrument_day_prices';
    """)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS dv_yfinance.sat_company_info (
  Instrument_HK         STRING NOT NULL,
  longName              STRING,
  marketCap             BIGINT,
  Hash_Diff             STRING,
  Load_DTS              TIMESTAMP NOT NULL,
  Effective_DTS         TIMESTAMP NOT NULL,       -- when this version became active
  End_DTS               TIMESTAMP,                -- when this version ended (NULL = current)
  Record_Source         STRING NOT NULL
)
USING DELTA
LOCATION '{silver_output_path}/sat_company_info';
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS dv_yfinance.hub_time (
  Time_HK           STRING NOT NULL,           -- hash(DateTime)
  DateTime          TIMESTAMP NOT NULL,         -- business key
  Load_DTS          TIMESTAMP NOT NULL,
  Record_Source     STRING NOT NULL
)
USING DELTA
LOCATION '{silver_output_path}/hub_time';
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS dv_yfinance.link_instrument_minute (
  Instrument_Minute_LK STRING NOT NULL,    -- hash(Symbol, DateTime)
  Instrument_HK        STRING NOT NULL,    -- → hub_instrument
  Time_HK              STRING NOT NULL,    -- → hub_time
  Load_DTS             TIMESTAMP NOT NULL,
  Record_Source        STRING NOT NULL
)
USING DELTA
LOCATION '{silver_output_path}/link_instrument_minute';
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS dv_yfinance.sat_instrument_minute_prices (
  Instrument_Minute_LK STRING   NOT NULL,  -- parent key
  Open                 DOUBLE,
  High                 DOUBLE,
  Low                  DOUBLE,
  Close                DOUBLE,
  Volume               BIGINT,
  Dividends            DOUBLE,
  Stock_Splits         DOUBLE,
  Hash_Diff            STRING,
  Load_DTS             TIMESTAMP NOT NULL,
  Effective_DTS        TIMESTAMP NOT NULL,
  End_DTS              TIMESTAMP,
  Record_Source        STRING NOT NULL
)
USING DELTA
LOCATION '{silver_output_path}/sat_instrument_minute_prices';
""")

spark.sql("""
CREATE VIEW IF NOT EXISTS dv_yfinance.instrument_daily_view as
  SELECT 
  t.Date,
  i.Symbol,
  ci.longName,
  p.Open,
  p.Close,
  CASE WHEN p.Close > p.Open THEN 'up' ELSE 'down' END as Movement,
  p.High,
  p.Low,
  p.High - p.Low AS Intraday_Range,
  p.Volume
  FROM dv_yfinance.link_instrument_day lim
  JOIN dv_yfinance.sat_instrument_day_prices p 
      ON lim.Instrument_Day_LK = p.Instrument_Day_LK
  JOIN dv_yfinance.hub_calendar t 
      ON lim.Calendar_HK = t.Calendar_HK
  JOIN dv_yfinance.hub_instrument i 
      ON lim.Instrument_HK = i.Instrument_HK
  JOIN dv_yfinance.sat_company_info ci
      ON ci.Instrument_HK = i.Instrument_HK
  ORDER BY t.Date desc;
""")

spark.sql("""
CREATE VIEW IF NOT EXISTS dv_yfinance.instrument_minute_view as
SELECT 
  t.DateTime,
  i.Symbol,
  ci.longName,
  p.Open,
  p.Close,
  CASE WHEN p.Close > p.Open THEN 'up' ELSE 'down' END as Movement,
  p.High,
  p.Low,
  p.High - p.Low AS Intra_Minute_Range,
  p.Volume
  FROM dv_yfinance.link_instrument_minute lim
  JOIN dv_yfinance.sat_instrument_minute_prices p 
      ON lim.Instrument_Minute_LK = p.Instrument_Minute_LK
  JOIN dv_yfinance.hub_time t 
      ON lim.Time_HK = t.Time_HK
  JOIN dv_yfinance.hub_instrument i 
      ON lim.Instrument_HK = i.Instrument_HK
  JOIN dv_yfinance.sat_company_info ci
      ON ci.Instrument_HK = i.Instrument_HK
  ORDER BY t.DateTime desc;
""")
# spark.sql(f"""
#     SHOW CATALOGS
#     """).show()

# spark.sql(f"""
#     SHOW DATABASES
#     """).show()

# spark.sql(f"""
#     SHOW SCHEMAS
#     """).show()

spark.sql(f"""
    SHOW TABLES IN {silver_schema}
    """).show()

# spark.sql(f"""
#     DESCRIBE TABLE EXTENDED {silver_schema}.sat_company_info
#     """).show()

# spark.sql(f"""
#     SELECT * FROM {silver_schema}.sat_company_info limit 10
# """).show()

# spark.sql(f"""
#     SELECT * FROM {silver_schema}.link_instrument_minute limit 10
# """).show()

# spark.sql(f"""
#     SELECT * FROM {silver_schema}.sat_instrument_minute_prices limit 10
# """).show()

# from delta.tables import DeltaTable
# # show_table_history(output_path_historical)
# delta_table = DeltaTable.forPath(spark, f"{silver_output_path}/sat_company_info")
# delta_table.history().show(truncate=False)


# spark.sql(f"""
#     SELECT 
#     t.DateTime,
#     i.Symbol,
#     p.Open,
#     p.Close,
#     p.High - p.Low AS Intraday_Range
#     FROM dv_yfinance.link_instrument_minute lim
#     JOIN dv_yfinance.sat_instrument_minute_prices p 
#         ON lim.Instrument_Minute_LK = p.Instrument_Minute_LK AND p.End_DTS IS NULL
#     JOIN dv_yfinance.hub_time t ON lim.Time_HK = t.Time_HK
#     JOIN dv_yfinance.hub_instrument i ON lim.Instrument_HK = i.Instrument_HK
#     WHERE DATE(t.DateTime) > '2026-03-01'
#     ORDER BY t.DateTime;
# """).show() 