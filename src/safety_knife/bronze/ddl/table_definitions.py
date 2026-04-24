"""
Bronze layer table definitions - Raw data ingestion.

Defines all bronze layer tables as reusable TableDefinition objects.
"""

from safety_knife.ddl.table_factory import TableDefinition


# ============================================================================
# Bronze raw tables - Direct copy of source data
# ============================================================================

HISTORICALS = TableDefinition(
    name="historicals",
    schema_sql="""
        Date         DATE,
        Symbol       STRING,
        Open         DOUBLE,
        High         DOUBLE,
        Low          DOUBLE,
        Close        DOUBLE,
        Volume       BIGINT,
        Dividends    DOUBLE,
        Stock_Splits DOUBLE
    """,
    location_subpath="historicals",
    partition_by="Symbol",
)

COMPANY = TableDefinition(
    name="company",
    schema_sql="""
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
    """,
    location_subpath="company",
)

MINUTE_HISTORICALS = TableDefinition(
    name="minute_historicals",
    schema_sql="""
        DateTime     TIMESTAMP,
        Symbol       STRING,
        Open         DOUBLE,
        High         DOUBLE,
        Low          DOUBLE,
        Close        DOUBLE,
        Volume       BIGINT,
        Dividends    DOUBLE,
        Stock_Splits DOUBLE
    """,
    location_subpath="historicals_minute",
    partition_by="Symbol",
)


# ============================================================================
# Table collections
# ============================================================================

ALL_TABLES = [
    HISTORICALS,
    COMPANY,
    MINUTE_HISTORICALS,
]
