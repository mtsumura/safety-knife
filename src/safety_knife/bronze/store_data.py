from safety_knife.spark_utils import get_or_create_spark_session
    
def load_to_spark(df, spark=None):
    """
    Takes a pandas dataframe and loads it into a spark dataframe.

    Args:
        df (pandas.DataFrame): The input pandas dataframe to be loaded.
        spark (SparkSession, optional): Spark session to use. If None, creates/gets one.

    Returns:
        pyspark.sql.DataFrame: A spark dataframe containing the same data as the input pandas dataframe.
    """
    if spark is None:
        spark = get_or_create_spark_session()
    # Convert the pandas dataframe to a spark dataframe
    spark_df = spark.createDataFrame(df)
    return spark_df


def save_to_delta(spark_df, path, partition=None):  
    """
    Takes a spark dataframe and saves it to a delta lake table.

    Args:
        spark_df (pyspark.sql.DataFrame): The input spark dataframe to be saved.
        path (str): The path where the delta lake table should be saved.
        partition (str, optional): Column to partition by.

    Returns:
        None
    """
    # Save the spark dataframe as a delta lake table
    s =  spark_df.write.format("delta").mode("append").option("overwriteSchema", "true")
    s.partitionBy(partition).save(path) if partition is not None else s.save(path)


def load_from_delta(path, version=None, spark=None):
    """
    Takes a path and loads stored data to a spark dataframe
    
    Args:
        path (str): Path to the Delta table.
        version (int, optional): Version of the table to load.
        spark (SparkSession, optional): Spark session to use. If None, creates/gets one.
    """
    if spark is None:
        spark = get_or_create_spark_session()
    s = spark.read.format("delta")
    df = s.option("versionAsOf", version).load(path) if version is not None else s.load(path)
    df.printSchema()
    return df

def show_table_history(output_path, spark=None):
    from delta.tables import DeltaTable
    if spark is None:
        spark = get_or_create_spark_session()
    delta_table = DeltaTable.forPath(spark, output_path)
    delta_table.history().show(truncate=False)
    print(delta_table.detail())

from delta.tables import DeltaTable
from pyspark.sql import functions as F

def upsert_to_delta(spark, incoming_df, delta_path: str):
    target = DeltaTable.forPath(spark, delta_path)

    (target.alias("t")
        .merge(
            source=incoming_df.alias("s"),
            condition="t.symbol = s.symbol",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

def upsert_historicals_by_path(spark, incoming_df, delta_path: str):
    target = DeltaTable.forPath(spark, delta_path)

    (target.alias("t")
        .merge(
            source=incoming_df.alias("s"),
            condition="t.Symbol = s.Symbol AND t.Date = s.Date",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )