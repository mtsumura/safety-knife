"""
Centralized Spark session utilities for Delta Lake integration.
"""

from pyspark.sql import SparkSession
from safety_knife.config import hive_warehouse_dir


def _create_spark_session(app_name: str) -> SparkSession:
    """
    Create a Spark session with Delta support.
    
    Args:
        app_name (str): Name for the Spark application.
        
    Returns:
        SparkSession: Configured Spark session with Delta Lake support.
    """
    from delta import configure_spark_with_delta_pip
    
    builder = (
        SparkSession.builder.appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .enableHiveSupport()
            .config("spark.sql.warehouse.dir", hive_warehouse_dir)
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def get_or_create_spark_session(app_name: str = "safety_knife_etl") -> SparkSession:
    """
    Get or create a Spark session with Delta support.
    Returns the existing session if already created.
    
    Args:
        app_name (str): Name for the Spark application (used only if creating new session).
        
    Returns:
        SparkSession: Active or newly created Spark session with Delta support.
    """
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing
    return _create_spark_session(app_name)
