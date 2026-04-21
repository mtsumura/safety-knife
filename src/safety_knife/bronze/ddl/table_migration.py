import os

from safety_knife.config import (
    company_output_path,
    output_path_historical,
    output_path_minute,
 )
from safety_knife.spark_utils import get_or_create_spark_session


spark = get_or_create_spark_session(app_name="test_migration")
bronze_schema = os.environ.get("BRONZE_SCHEMA", "yfinance")
repoint = (os.environ.get("BRONZE_REPOINT_TABLES") or "1").strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
}

print(f"{bronze_schema=}")
print(f"{repoint=}")
print(f"{output_path_historical=}")
print(f"{output_path_minute=}")
print(f"{company_output_path=}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_schema}")

if repoint:
    # tables that point at the configured LOCATION. This does NOT delete data at LOCATION.
    spark.sql(f"DROP TABLE IF EXISTS {bronze_schema}.historicals")
    spark.sql(f"DROP TABLE IF EXISTS {bronze_schema}.minute_historicals")
    spark.sql(f"DROP TABLE IF EXISTS {bronze_schema}.company")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {bronze_schema}.historicals (
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
CREATE TABLE IF NOT EXISTS {bronze_schema}.company (
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
CREATE TABLE IF NOT EXISTS {bronze_schema}.minute_historicals (
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
    SHOW TABLES IN {bronze_schema}
    """).show()

# spark.sql(f"""
#     ALTER TABLE {bronze_schema}.historicals ADD CONSTRAINT date_check CHECK (Date IS NOT NULL);
# """)

# spark.sql(f"""
#     ALTER TABLE {bronze_schema}.historicals ADD CONSTRAINT symbol_check CHECK (Symbol IS NOT NULL);
# """)

# spark.sql(f"""
#     ALTER TABLE {bronze_schema}.historicals ADD CONSTRAINT positive_open CHECK (Open IS NOT NULL AND Open > 0);
# """)

# spark.sql(f"""
#     ALTER TABLE {bronze_schema}.historicals ADD CONSTRAINT positive_high CHECK (High IS NOT NULL AND High > 0);
# """)

# spark.sql(f"""
#     ALTER TABLE {bronze_schema}.historicals ADD CONSTRAINT positive_low CHECK (Low IS NOT NULL AND Low > 0);
# """)

# spark.sql(f"""
#     ALTER TABLE {bronze_schema}.historicals ADD CONSTRAINT positive_close CHECK (Close IS NOT NULL AND Close > 0);
# """)

# spark.sql(f"""
#     ALTER TABLE {bronze_schema}.historicals ADD CONSTRAINT positive_volume CHECK (Volume IS NOT NULL AND Volume > 0);
# """)

spark.sql(f"DESCRIBE TABLE EXTENDED {bronze_schema}.minute_historicals").show()
spark.sql(f"SHOW TBLPROPERTIES {bronze_schema}.minute_historicals").show()

spark.sql(f"SELECT * FROM {bronze_schema}.historicals").show()
spark.sql(f"SELECT * FROM {bronze_schema}.company").show()

spark.sql(f"SELECT * FROM {bronze_schema}.minute_historicals LIMIT 5").show()
