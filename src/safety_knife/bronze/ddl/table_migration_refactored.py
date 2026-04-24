"""
Bronze layer table initialization script.

Uses the unified DeltaTableFactory to create raw data tables.
Supports environment variable configuration for local and Azure (ADLS Gen2) paths.

Environment variables:
  BRONZE_DELTA_BASE: Base path for bronze tables (default: local data/bronze)
                     Examples:
                       - /Users/.../Workspace/safety-knife/data/bronze
                       - abfss://container@account.dfs.core.windows.net/bronze
  BRONZE_SCHEMA: Schema/database name (default: yfinance)
  BRONZE_REPOINT_TABLES: If 1/true/yes, drop and recreate tables to repoint to new location
                         (default: 1)
"""

import os
from safety_knife.config import BRONZE_DELTA_BASE
from safety_knife.ddl.table_factory import DeltaTableFactory
from safety_knife.bronze.ddl.table_definitions import ALL_TABLES

# Configuration from environment or defaults
bronze_schema = os.environ.get("BRONZE_SCHEMA", "yfinance")
repoint = (os.environ.get("BRONZE_REPOINT_TABLES") or "1").strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
}

print(f"{bronze_schema=}")
print(f"{repoint=}")
print(f"{BRONZE_DELTA_BASE=}")

# Create factory and initialize tables
factory = DeltaTableFactory(
    spark_app_name="bronze_migration",
    delta_base_path=BRONZE_DELTA_BASE,
    schema_name=bronze_schema,
    repoint=repoint,
)

# Create schema and all tables
factory.create_tables(ALL_TABLES)

# Display results
print("\n=== Catalogs ===")
factory.show_catalogs()

print("\n=== Schemas ===")
factory.show_schemas()

print("\n=== Bronze Tables ===")
factory.show_tables()

print("\n=== Sample Table Description ===")
factory.describe_table("historicals")
