"""
Silver layer table definitions - Data Vault 2.0 schema.

Defines all silver layer tables (hubs, links, satellites) as reusable TableDefinition objects.
This approach allows sharing definitions across different initialization scripts and environments.
"""

import os
from safety_knife.ddl.table_factory import TableDefinition


# ============================================================================
# HUBS - Core business dimensions
# ============================================================================

HUB_INSTRUMENT = TableDefinition(
    name="hub_instrument",
    schema_sql="""
        Instrument_HK STRING NOT NULL,   -- hash(Symbol)
        Symbol        STRING NOT NULL,   -- business key
        Load_DTS      TIMESTAMP NOT NULL,
        Record_Source STRING NOT NULL
    """,
    location_subpath="hub_instrument",
)

HUB_CALENDAR = TableDefinition(
    name="hub_calendar",
    schema_sql="""
        Calendar_HK   STRING NOT NULL,   -- hash(Date)
        Date          DATE   NOT NULL,   -- business key
        Load_DTS      TIMESTAMP NOT NULL,
        Record_Source STRING NOT NULL
    """,
    location_subpath="hub_calendar",
)

HUB_TIME = TableDefinition(
    name="hub_time",
    schema_sql="""
        Time_HK       STRING NOT NULL,           -- hash(DateTime)
        DateTime      TIMESTAMP NOT NULL,        -- business key
        Load_DTS      TIMESTAMP NOT NULL,
        Record_Source STRING NOT NULL
    """,
    location_subpath="hub_time",
)


# ============================================================================
# LINKS - Relationships between hubs
# ============================================================================

LINK_INSTRUMENT_DAY = TableDefinition(
    name="link_instrument_day",
    schema_sql="""
        Instrument_Day_LK STRING NOT NULL,  -- hash(Symbol, Date)
        Instrument_HK     STRING NOT NULL,  -- → hub_instrument
        Calendar_HK       STRING NOT NULL,  -- → hub_calendar
        Load_DTS          TIMESTAMP NOT NULL,
        Record_Source     STRING NOT NULL
    """,
    location_subpath="link_instrument_day",
)

LINK_INSTRUMENT_MINUTE = TableDefinition(
    name="link_instrument_minute",
    schema_sql="""
        Instrument_Minute_LK STRING NOT NULL,    -- hash(Symbol, DateTime)
        Instrument_HK        STRING NOT NULL,    -- → hub_instrument
        Time_HK              STRING NOT NULL,    -- → hub_time
        Load_DTS             TIMESTAMP NOT NULL,
        Record_Source        STRING NOT NULL
    """,
    location_subpath="link_instrument_minute",
)


# ============================================================================
# SATELLITES - Descriptive attributes
# ============================================================================

SAT_INSTRUMENT_DAY_PRICES = TableDefinition(
    name="sat_instrument_day_prices",
    schema_sql="""
        Instrument_Day_LK STRING   NOT NULL,  -- parent key
        Open              DOUBLE,
        High              DOUBLE,
        Low               DOUBLE,
        Close             DOUBLE,
        Volume            BIGINT,
        Dividends         DOUBLE,
        Stock_Splits      DOUBLE,
        Hash_Diff         STRING,
        Load_DTS          TIMESTAMP NOT NULL,
        Record_Source     STRING   NOT NULL
    """,
    location_subpath="sat_instrument_day_prices",
)

SAT_INSTRUMENT_MINUTE_PRICES = TableDefinition(
    name="sat_instrument_minute_prices",
    schema_sql="""
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
        Record_Source        STRING   NOT NULL
    """,
    location_subpath="sat_instrument_minute_prices",
)

SAT_COMPANY_INFO = TableDefinition(
    name="sat_company_info",
    schema_sql="""
        Instrument_HK         STRING NOT NULL,
        longName              STRING,
        marketCap             BIGINT,
        Hash_Diff             STRING,
        Load_DTS              TIMESTAMP NOT NULL,
        Effective_DTS         TIMESTAMP NOT NULL,       -- when this version became active
        End_DTS               TIMESTAMP,                -- when this version ended (NULL = current)
        Record_Source         STRING NOT NULL
    """,
    location_subpath="sat_company_info",
)


# ============================================================================
# Table collections - Grouped by type
# ============================================================================

ALL_HUBS = [
    HUB_INSTRUMENT,
    HUB_CALENDAR,
    HUB_TIME,
]

ALL_LINKS = [
    LINK_INSTRUMENT_DAY,
    LINK_INSTRUMENT_MINUTE,
]

ALL_SATELLITES = [
    SAT_INSTRUMENT_DAY_PRICES,
    SAT_INSTRUMENT_MINUTE_PRICES,
    SAT_COMPANY_INFO,
]

ALL_TABLES = ALL_HUBS + ALL_LINKS + ALL_SATELLITES


# ============================================================================
# View definitions (SQL strings)
# ============================================================================

INSTRUMENT_DAILY_VIEW = """
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
  ORDER BY t.Date desc
"""

INSTRUMENT_MINUTE_VIEW = """
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
  ORDER BY t.DateTime desc
"""
