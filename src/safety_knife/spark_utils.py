"""
Centralized Spark session utilities for Delta Lake integration.
"""

import os

from pyspark.sql import SparkSession
from safety_knife.config import hive_warehouse_dir


def _with_azure_abfs(builder):
    """
    If you store Delta tables in ADLS Gen2 (abfss://), Spark needs:
    - Hadoop Azure filesystem implementation (hadoop-azure)
    - OAuth (service principal) configs

    This is a no-op unless SPARK_AZURE_ACCOUNT_NAME is set.
    """

    account_name = os.environ.get("SPARK_AZURE_ACCOUNT_NAME")
    if not account_name:
        return builder, []

    # Match Hadoop client bundled with PySpark 4.1.x (hadoop-client-*-3.4.2.jar).
    hadoop_azure_pkg = os.environ.get(
        "SPARK_HADOOP_AZURE_PACKAGE",
        "org.apache.hadoop:hadoop-azure:3.4.2",
    )
    # Note: don't set spark.jars.packages here; Delta helper also configures it.
    # We'll pass this as an "extra package" to configure_spark_with_delta_pip below.

    tenant_id = os.environ.get("AZURE_TENANT_ID")
    client_id = os.environ.get("AZURE_CLIENT_ID")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET")
    if not (tenant_id and client_id and client_secret):
        raise RuntimeError(
            "SPARK_AZURE_ACCOUNT_NAME is set, but AZURE_TENANT_ID/AZURE_CLIENT_ID/AZURE_CLIENT_SECRET "
            "are not all set. These are required for Spark ABFS OAuth."
        )

    suffix = f"{account_name}.dfs.core.windows.net"

    # Use spark.hadoop.* so these land in the underlying Hadoop Configuration.
    builder = (
        builder.config(f"spark.hadoop.fs.azure.account.auth.type.{suffix}", "OAuth")
        .config(
            f"spark.hadoop.fs.azure.account.oauth.provider.type.{suffix}",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        )
        .config(f"spark.hadoop.fs.azure.account.oauth2.client.id.{suffix}", client_id)
        .config(f"spark.hadoop.fs.azure.account.oauth2.client.secret.{suffix}", client_secret)
        .config(
            f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{suffix}",
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
        )
    )

    return builder, [hadoop_azure_pkg]


def _create_spark_session(app_name: str) -> SparkSession:
    """
    Create a Spark session with Delta support.
    
    Args:
        app_name (str): Name for the Spark application.
        
    Returns:
        SparkSession: Configured Spark session with Delta Lake support.
    """
    from delta import configure_spark_with_delta_pip
    
    builder = SparkSession.builder.appName(app_name)
    builder, extra_packages = _with_azure_abfs(builder)
    builder = (
        builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .enableHiveSupport()
        .config("spark.sql.warehouse.dir", hive_warehouse_dir)
    )
    # Ensure Delta jars + any optional extra jars (e.g. hadoop-azure for abfss://)
    builder = configure_spark_with_delta_pip(builder, extra_packages=extra_packages or None)
    spark = builder.getOrCreate()
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
