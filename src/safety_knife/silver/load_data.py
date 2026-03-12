import argparse

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def _is_delta_table(spark: SparkSession, path: str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False


def _ensure_delta_table(spark: SparkSession, path: str, schema: T.StructType) -> None:
    if _is_delta_table(spark, path):
        return
    spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(path)


def _register_external_table(spark: SparkSession, table_name: str, path: str) -> None:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{path}'")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load Data Vault (daily) tables from yfinance.historicals (Delta)."
    )
    parser.add_argument(
        "--bronze-table",
        default="yfinance.historicals",
        help="Source table name (Spark catalog). Default: yfinance.historicals",
    )
    parser.add_argument(
        "--bronze-path",
        default=None,
        help="Optional source Delta path. If set, used instead of --bronze-table.",
    )
    parser.add_argument(
        "--symbol",
        default=None,
        help="If the source has no Symbol column, add this as a constant (e.g. CSU.TO).",
    )
    parser.add_argument(
        "--dv-db",
        default="dv_yfinance",
        help="Database/schema name for vault tables. Default: dv_yfinance",
    )
    parser.add_argument(
        "--dv-base-path",
        required=True,
        help="Base directory to store vault Delta tables (e.g. /data/vault/yfinance).",
    )
    parser.add_argument(
        "--record-source",
        default="yfinance.historicals",
        help="Value to store in Record_Source. Default: yfinance.historicals",
    )
    parser.add_argument(
        "--register-catalog",
        action="store_true",
        help="If set, CREATE TABLE ... LOCATION for the vault tables in spark catalog.",
    )
    parser.add_argument(
        "--enable-hive",
        action="store_true",
        help="If set, enable Hive support (needed for persistent catalogs).",
    )

    args = parser.parse_args()

    builder = SparkSession.builder.appName("LoadYFinanceDataVaultDaily")
    if args.enable_hive:
        builder = builder.enableHiveSupport()
    spark = builder.getOrCreate()

    if args.bronze_path:
        src = spark.read.format("delta").load(args.bronze_path)
    else:
        src = spark.table(args.bronze_table)

    cols = set(src.columns)
    if "Symbol" not in cols:
        if not args.symbol:
            raise ValueError(
                "Source has no Symbol column. Provide --symbol to load single-instrument data."
            )
        src = src.withColumn("Symbol", F.lit(args.symbol))

    # Canonicalize business keys (keeps HK stable across casing/whitespace differences).
    src = src.withColumn("Symbol", F.upper(F.trim(F.col("Symbol"))))

    # Basic data quality gates for business keys.
    src = src.filter(F.col("Date").isNotNull() & F.col("Symbol").isNotNull() & (F.length("Symbol") > 0))

    load_dts = F.current_timestamp()
    record_source = F.lit(args.record_source)

    # Hash key expressions (md5 is stable and portable).
    date_str = F.date_format(F.col("Date"), "yyyy-MM-dd")
    instrument_hk = F.md5(F.col("Symbol"))
    calendar_hk = F.md5(date_str)
    instrument_day_lk = F.md5(F.concat_ws("|", F.col("Symbol"), date_str))

    staged = (
        src.select(
            F.col("Date").alias("Date"),
            F.col("Symbol").alias("Symbol"),
            F.col("Open").cast("double").alias("Open"),
            F.col("High").cast("double").alias("High"),
            F.col("Low").cast("double").alias("Low"),
            F.col("Close").cast("double").alias("Close"),
            F.col("Volume").cast("bigint").alias("Volume"),
            F.col("Dividends").cast("double").alias("Dividends"),
            F.col("Stock_Splits").cast("double").alias("Stock_Splits"),
        )
        .withColumn("Instrument_HK", instrument_hk)
        .withColumn("Calendar_HK", calendar_hk)
        .withColumn("Instrument_Day_LK", instrument_day_lk)
        .withColumn("Load_DTS", load_dts)
        .withColumn("Record_Source", record_source)
    )

    # --- Target paths ---
    dv_base = args.dv_base_path.rstrip("/")
    hub_instrument_path = f"{dv_base}/hub_instrument"
    hub_calendar_path = f"{dv_base}/hub_calendar"
    link_instrument_day_path = f"{dv_base}/link_instrument_day"
    sat_instrument_day_prices_path = f"{dv_base}/sat_instrument_day_prices"

    # --- Target schemas (explicit) ---
    hub_instrument_schema = T.StructType(
        [
            T.StructField("Instrument_HK", T.StringType(), nullable=False),
            T.StructField("Symbol", T.StringType(), nullable=False),
            T.StructField("Load_DTS", T.TimestampType(), nullable=False),
            T.StructField("Record_Source", T.StringType(), nullable=False),
        ]
    )
    hub_calendar_schema = T.StructType(
        [
            T.StructField("Calendar_HK", T.StringType(), nullable=False),
            T.StructField("Date", T.DateType(), nullable=False),
            T.StructField("Load_DTS", T.TimestampType(), nullable=False),
            T.StructField("Record_Source", T.StringType(), nullable=False),
        ]
    )
    link_instrument_day_schema = T.StructType(
        [
            T.StructField("Instrument_Day_LK", T.StringType(), nullable=False),
            T.StructField("Instrument_HK", T.StringType(), nullable=False),
            T.StructField("Calendar_HK", T.StringType(), nullable=False),
            T.StructField("Load_DTS", T.TimestampType(), nullable=False),
            T.StructField("Record_Source", T.StringType(), nullable=False),
        ]
    )
    sat_prices_schema = T.StructType(
        [
            T.StructField("Instrument_Day_LK", T.StringType(), nullable=False),
            T.StructField("Open", T.DoubleType(), nullable=True),
            T.StructField("High", T.DoubleType(), nullable=True),
            T.StructField("Low", T.DoubleType(), nullable=True),
            T.StructField("Close", T.DoubleType(), nullable=True),
            T.StructField("Volume", T.LongType(), nullable=True),
            T.StructField("Dividends", T.DoubleType(), nullable=True),
            T.StructField("Stock_Splits", T.DoubleType(), nullable=True),
            T.StructField("Hash_Diff", T.StringType(), nullable=False),
            T.StructField("Load_DTS", T.TimestampType(), nullable=False),
            T.StructField("Record_Source", T.StringType(), nullable=False),
        ]
    )

    # Create tables (as Delta paths) if missing.
    _ensure_delta_table(spark, hub_instrument_path, hub_instrument_schema)
    _ensure_delta_table(spark, hub_calendar_path, hub_calendar_schema)
    _ensure_delta_table(spark, link_instrument_day_path, link_instrument_day_schema)
    _ensure_delta_table(spark, sat_instrument_day_prices_path, sat_prices_schema)

    # Optionally register as external tables for convenient Spark SQL access.
    if args.register_catalog:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.dv_db}")
        _register_external_table(spark, f"{args.dv_db}.hub_instrument", hub_instrument_path)
        _register_external_table(spark, f"{args.dv_db}.hub_calendar", hub_calendar_path)
        _register_external_table(spark, f"{args.dv_db}.link_instrument_day", link_instrument_day_path)
        _register_external_table(spark, f"{args.dv_db}.sat_instrument_day_prices", sat_instrument_day_prices_path)

    # --- Stage DataFrames ---
    stg_hub_instrument = (
        staged.select("Instrument_HK", "Symbol", "Load_DTS", "Record_Source")
        .dropDuplicates(["Instrument_HK"])
    )
    stg_hub_calendar = (
        staged.select("Calendar_HK", "Date", "Load_DTS", "Record_Source")
        .dropDuplicates(["Calendar_HK"])
    )
    stg_link_instrument_day = (
        staged.select(
            "Instrument_Day_LK",
            "Instrument_HK",
            "Calendar_HK",
            "Load_DTS",
            "Record_Source",
        ).dropDuplicates(["Instrument_Day_LK"])
    )

    # HashDiff for satellite: stable serialization via JSON struct.
    attr_struct = F.struct(
        F.col("Open"),
        F.col("High"),
        F.col("Low"),
        F.col("Close"),
        F.col("Volume"),
        F.col("Dividends"),
        F.col("Stock_Splits"),
    )
    stg_sat_prices = (
        staged.withColumn("Hash_Diff", F.md5(F.to_json(attr_struct)))
        .select(
            "Instrument_Day_LK",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "Dividends",
            "Stock_Splits",
            "Hash_Diff",
            "Load_DTS",
            "Record_Source",
        )
        # Idempotency within a batch and across reruns:
        # only one row per (Instrument_Day_LK, Hash_Diff).
        .dropDuplicates(["Instrument_Day_LK", "Hash_Diff"])
    )

    # --- MERGE loads (idempotent) ---
    DeltaTable.forPath(spark, hub_instrument_path).alias("t").merge(
        stg_hub_instrument.alias("s"),
        "t.Instrument_HK = s.Instrument_HK",
    ).whenNotMatchedInsertAll().execute()

    DeltaTable.forPath(spark, hub_calendar_path).alias("t").merge(
        stg_hub_calendar.alias("s"),
        "t.Calendar_HK = s.Calendar_HK",
    ).whenNotMatchedInsertAll().execute()

    DeltaTable.forPath(spark, link_instrument_day_path).alias("t").merge(
        stg_link_instrument_day.alias("s"),
        "t.Instrument_Day_LK = s.Instrument_Day_LK",
    ).whenNotMatchedInsertAll().execute()

    # Satellite is insert-only with (LK, Hash_Diff) uniqueness for idempotent reruns.
    DeltaTable.forPath(spark, sat_instrument_day_prices_path).alias("t").merge(
        stg_sat_prices.alias("s"),
        "t.Instrument_Day_LK = s.Instrument_Day_LK AND t.Hash_Diff = s.Hash_Diff",
    ).whenNotMatchedInsertAll().execute()


if __name__ == "__main__":
    main()
