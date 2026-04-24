"""
Unified table factory for creating Delta Lake tables with support for local and Azure storage.

Supports:
- Local paths: /path/to/data
- Azure ADLS Gen2: abfss://container@account.dfs.core.windows.net/path
"""

from typing import Optional
from dataclasses import dataclass
from safety_knife.spark_utils import get_or_create_spark_session


@dataclass
class TableDefinition:
    """Defines a single Delta Lake table."""
    name: str
    schema_sql: str  # e.g., "id STRING NOT NULL, name STRING"
    location_subpath: str  # e.g., "hub_instrument" or "sat_prices"
    partition_by: Optional[str] = None  # e.g., "Symbol"
    using: str = "DELTA"


class DeltaTableFactory:
    """
    Factory for creating Delta Lake tables with support for local and Azure storage backends.
    
    Usage:
        factory = DeltaTableFactory(
            spark_app_name="my_app",
            delta_base_path="/path/to/data",  # or abfss://...
            schema_name="my_schema"
        )
        factory.create_tables(tables=[...], repoint=True)
    """
    
    def __init__(
        self,
        spark_app_name: str,
        delta_base_path: str,
        schema_name: str,
        repoint: bool = False,
    ):
        """
        Args:
            spark_app_name: Spark application name
            delta_base_path: Base path for Delta tables (local or ADLS Gen2)
            schema_name: Hive schema/database name
            repoint: If True, drop tables before recreating (allows repointing to new locations)
        """
        self.spark = get_or_create_spark_session(app_name=spark_app_name)
        self.delta_base_path = delta_base_path.rstrip("/")
        self.schema_name = schema_name
        self.repoint = repoint
        
    def create_schema(self) -> None:
        """Create the schema if it doesn't exist."""
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")
        
    def _drop_table_if_repointing(self, table_name: str) -> None:
        """Drop table if repointing is enabled."""
        if self.repoint:
            self.spark.sql(f"DROP TABLE IF EXISTS {self.schema_name}.{table_name}")
    
    def _build_create_table_sql(
        self,
        table_name: str,
        schema_sql: str,
        location: str,
        partition_by: Optional[str] = None,
        using: str = "DELTA",
    ) -> str:
        """Build CREATE TABLE statement."""
        partition_clause = f"\nPARTITIONED BY ({partition_by})" if partition_by else ""
        
        sql = f"""
CREATE TABLE IF NOT EXISTS {self.schema_name}.{table_name} (
  {schema_sql}
)
USING {using}{partition_clause}
LOCATION '{location}';
        """.strip()
        
        return sql
    
    def create_table(
        self,
        table_def: TableDefinition,
    ) -> None:
        """Create a single table from a TableDefinition."""
        location = f"{self.delta_base_path}/{table_def.location_subpath}"
        self._drop_table_if_repointing(table_def.name)
        
        sql = self._build_create_table_sql(
            table_name=table_def.name,
            schema_sql=table_def.schema_sql,
            location=location,
            partition_by=table_def.partition_by,
            using=table_def.using,
        )
        
        self.spark.sql(sql)
        
    def create_tables(self, tables: list[TableDefinition]) -> None:
        """Create multiple tables."""
        self.create_schema()
        for table in tables:
            self.create_table(table)
    
    def show_tables(self) -> None:
        """Display all tables in the schema."""
        self.spark.sql(f"SHOW TABLES IN {self.schema_name}").show()
    
    def show_catalogs(self) -> None:
        """Display all catalogs."""
        self.spark.sql("SHOW CATALOGS").show()
    
    def show_schemas(self) -> None:
        """Display all schemas."""
        self.spark.sql("SHOW SCHEMAS").show()
    
    def show_databases(self) -> None:
        """Alias for show_schemas (databases and schemas are the same in Spark SQL)."""
        self.show_schemas()
    
    def describe_table(self, table_name: str) -> None:
        """Describe a table."""
        self.spark.sql(f"DESCRIBE TABLE EXTENDED {self.schema_name}.{table_name}").show()
    
    def create_view(
        self,
        view_name: str,
        sql: str,
        replace: bool = True,
    ) -> None:
        """Create a view. If replace=True, uses CREATE OR REPLACE VIEW."""
        create_clause = "CREATE OR REPLACE VIEW" if replace else "CREATE VIEW IF NOT EXISTS"
        full_sql = f"{create_clause} {self.schema_name}.{view_name} AS\n{sql}"
        self.spark.sql(full_sql)
