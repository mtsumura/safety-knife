from safety_knife.config import DATA_DIR, ticker_symbol, output_path_historical, company_output_path, output_path_minute
from safety_knife.spark_utils import get_or_create_spark_session


spark = get_or_create_spark_session(app_name="test_migration")
print(f"{output_path_minute=}")
spark.sql(
    """
    CREATE SCHEMA IF NOT EXISTS yfinance
    """)

# spark.sql("""
# DROP TABLE IF EXISTS yfinance.minute_historicals
# """)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS yfinance.historicals (
  Date         DATE,
  Symbol       STRING,
  Open         DOUBLE,
  High         DOUBLE,
  Low          DOUBLE,
  Close        DOUBLE,
  Volume       BIGINT,
  Dividends    DOUBLE,
  Stock_Splits DOUBLE
)
USING DELTA
PARTITIONED BY (Symbol)
LOCATION '{output_path_historical}';
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS yfinance.company (
    symbol STRING,
    longName STRING,
    marketCap BIGINT,
    priceToEarnings DOUBLE,
    dividendYield DOUBLE,
    totalRevenue BIGINT,
    netIncome BIGINT,
    bookValuePerShare DOUBLE,
    priceToBook DOUBLE,
    forwardEps DOUBLE
)
USING DELTA
LOCATION '{company_output_path}';
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS yfinance.minute_historicals (
  DateTime     TIMESTAMP,
  Symbol       STRING,
  Open         DOUBLE,
  High         DOUBLE,
  Low          DOUBLE,
  Close        DOUBLE,
  Volume       BIGINT,
  Dividends    DOUBLE,
  Stock_Splits DOUBLE
)
USING DELTA
PARTITIONED BY (Symbol)
LOCATION '{output_path_minute}';
""")

spark.sql(f"""
    SHOW CATALOGS
    """).show()

spark.sql(f"""
    SHOW DATABASES
    """).show()

spark.sql(f"""
    SHOW SCHEMAS
    """).show()

spark.sql(f"""
    SHOW TABLES IN yfinance
    """).show()

spark.sql("""
    ALTER TABLE yfinance.historicals ADD CONSTRAINT date_check CHECK (Date IS NOT NULL);
""")

spark.sql("""
    ALTER TABLE yfinance.historicals ADD CONSTRAINT symbol_check CHECK (Symbol IS NOT NULL);
""")

spark.sql("""
    ALTER TABLE yfinance.historicals ADD CONSTRAINT positive_open CHECK (Open IS NOT NULL AND Open > 0);
""")

spark.sql("""
    ALTER TABLE yfinance.historicals ADD CONSTRAINT positive_high CHECK (High IS NOT NULL AND High > 0);
""")

spark.sql("""
    ALTER TABLE yfinance.historicals ADD CONSTRAINT positive_low CHECK (Low IS NOT NULL AND Low > 0);
""")

spark.sql("""
    ALTER TABLE yfinance.historicals ADD CONSTRAINT positive_close CHECK (Close IS NOT NULL AND Close > 0);
""")


spark.sql("""
    ALTER TABLE yfinance.historicals ADD CONSTRAINT positive_volume CHECK (Volume IS NOT NULL AND Volume > 0);
""")

spark.sql(f"""
    DESCRIBE TABLE EXTENDED yfinance.minute_historicals
    """).show()

spark.sql(f"""
    SHOW TBLPROPERTIES yfinance.minute_historicals
    """).show()

spark.sql(f"""
    SELECT * FROM yfinance.minute_historicals
""").show()

spark.sql(f"""
    SELECT * FROM yfinance.historicals
""").show()

spark.sql(f"""
    SELECT * FROM yfinance.company
""").show()
