"""
Silver layer table initialization script.

Uses the unified DeltaTableFactory to create Data Vault 2.0 tables.
Supports environment variable configuration for local and Azure (ADLS Gen2) paths.

Environment variables:
  SILVER_DELTA_BASE: Base path for silver tables (default: local data/silver/vault)
                     Examples: 
                       - /path/to/data/silver/vault
                       - abfss://container@account.dfs.core.windows.net/silver
  SILVER_SCHEMA: Schema/database name (default: dv_yfinance)
  SILVER_REPOINT_TABLES: If 1/true/yes, drop and recreate tables to repoint to new location
                         (default: 1)
"""

import os
from safety_knife.config import silver_output_path
from safety_knife.ddl.table_factory import DeltaTableFactory
from safety_knife.silver.ddl.table_definitions import (
    ALL_TABLES,
    INSTRUMENT_DAILY_VIEW,
    INSTRUMENT_MINUTE_VIEW,
)

# Configuration from environment or defaults
silver_schema = os.environ.get("SILVER_SCHEMA", "dv_yfinance")
repoint = (os.environ.get("SILVER_REPOINT_TABLES") or "1").strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
}

print(f"{silver_schema=}")
print(f"{repoint=}")
print(f"{silver_output_path=}")

# Create factory and initialize tables
factory = DeltaTableFactory(
    spark_app_name="silver_migration",
    delta_base_path=silver_output_path,
    schema_name=silver_schema,
    repoint=repoint,
)

# Create schema and all tables
factory.create_tables(ALL_TABLES)

# Create views
factory.create_view(
    view_name="instrument_daily_view",
    sql=INSTRUMENT_DAILY_VIEW,
    replace=True,
)

factory.create_view(
    view_name="instrument_minute_view",
    sql=INSTRUMENT_MINUTE_VIEW,
    replace=True,
)

# Display results
print("\n=== Catalogs ===")
factory.show_catalogs()

print("\n=== Schemas ===")
factory.show_schemas()

print("\n=== Silver Tables ===")
factory.show_tables()

print("\n=== Sample Table Description ===")
factory.describe_table("hub_instrument")
