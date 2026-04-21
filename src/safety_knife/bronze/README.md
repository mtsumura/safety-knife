# Bronze pipeline configuration (Local vs Azure)

This directory contains the **Bronze** ingestion pipeline:
- **Raw cache**: CSV + JSON written by `cache.py` (yfinance API pulls)
- **Bronze Delta tables**: Delta Lake tables used by Spark SQL / downstream Silver loads

You can run everything on your laptop and switch between **local** and **Azure** storage by setting environment variables.

---

## 1) Azure authentication (Service Principal)

The project is set up to use a **service principal** for Azure access (preferred for debugging + RBAC).

Set these in your shell:

```bash
export AZURE_TENANT_ID="..."
export AZURE_CLIENT_ID="..."
export AZURE_CLIENT_SECRET="..."
```

Permissions you typically need on the target storage account/container:
- **Raw cache (Blob SDK)**: “Storage Blob Data Contributor” (or narrower)
- **Spark ABFS (ADLS Gen2)**: same RBAC scope, but ensure the account has **Hierarchical Namespace** enabled (ADLS Gen2)

---

## 2) Raw cache (CSV + JSON) storage backend

Raw cache is used by `src/safety_knife/bronze/cache.py`.

### Local raw cache

```bash
export BRONZE_RAW_BACKEND="local"
export BRONZE_RAW_LOCAL_BASE="/Users/michaeltsumura/Workspace/safety-knife/data/bronze"
```

### Azure raw cache (Azure Blob Storage)

```bash
export BRONZE_RAW_BACKEND="azure"
export BRONZE_RAW_AZURE_ACCOUNT_URL="https://<account>.blob.core.windows.net"
export BRONZE_RAW_AZURE_CONTAINER="<container>"

# optional (recommended): keep all raw blobs under a prefix
export BRONZE_RAW_AZURE_PREFIX="bronze-raw"
```

Notes:
- Auth uses the service principal vars (`AZURE_*`) above.
- Raw objects are written under stable keys like:
  - `<SYMBOL>/historical/daily.csv`
  - `<SYMBOL>/historical_minute/minute.csv`
  - `<SYMBOL>/info/meta.json`

---

## 3) Bronze Delta tables (Delta Lake) storage location

Bronze Delta tables are created/registered by:
- `src/safety_knife/bronze/ddl/table_migration.py`

And written by Spark via:
- `src/safety_knife/bronze/store_data.py` (`save_to_delta(...)`)

### Local Bronze Delta

```bash
export BRONZE_DELTA_BASE="/Users/michaeltsumura/Workspace/safety-knife/data/bronze"
```

### Azure Bronze Delta (ADLS Gen2 via ABFS)

```bash
export BRONZE_DELTA_BASE="abfss://<container>@<account>.dfs.core.windows.net/bronze"
```

This controls where these Delta tables live (directories):
- `${BRONZE_DELTA_BASE}/historicals`
- `${BRONZE_DELTA_BASE}/historicals_minute`
- `${BRONZE_DELTA_BASE}/company`

---

## 4) Spark configuration for `abfss://` (required for Azure Delta)

When writing/reading Delta on ADLS Gen2 (`abfss://...`), Spark must load the Azure filesystem implementation and OAuth configuration.

Set:

```bash
export SPARK_AZURE_ACCOUNT_NAME="<account>"   # e.g. mtsumurasafety

# Optional override (default matches PySpark 4.1.x Hadoop 3.4.2)
export SPARK_HADOOP_AZURE_PACKAGE="org.apache.hadoop:hadoop-azure:3.4.2"
```

Spark uses the same service principal vars:

```bash
export AZURE_TENANT_ID="..."
export AZURE_CLIENT_ID="..."
export AZURE_CLIENT_SECRET="..."
```

If you don’t set `SPARK_AZURE_ACCOUNT_NAME`, Spark will not attempt to configure ABFS.

---

## 5) Metastore schema + “transparent switching” (Option A)

`src/safety_knife/bronze/ddl/table_migration.py` supports **Option A** switching:
drop the metastore table entries and recreate them pointing at the currently configured `LOCATION`.

```bash
# Optional schema name for the bronze tables (default: yfinance)
export BRONZE_SCHEMA="yfinance"

# If true (default), DROP TABLE and recreate to re-point LOCATION
export BRONZE_REPOINT_TABLES="1"
```

Run the migration script after switching `BRONZE_DELTA_BASE`:

```bash
poetry run python src/safety_knife/bronze/ddl/table_migration.py
```

Important:
- `DROP TABLE` removes the **catalog entry**, not the files at `LOCATION`.
- Don’t run two writers against the same Delta table path concurrently.

---

## 6) Quick “Azure Bronze” checklist

1. Export `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`
2. Export `BRONZE_DELTA_BASE=abfss://...`
3. Export `SPARK_AZURE_ACCOUNT_NAME=<account>`
4. Run `poetry run python src/safety_knife/bronze/ddl/table_migration.py`
5. Run your Bronze daily pipeline (e.g. `drive_daily_data.py`) and confirm tables read back from the `yfinance` schema.

