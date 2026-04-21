"""
Smoke test: Spark writes/reads Parquet on ADLS Gen2 (abfss://).

Requires JVM-side Azure Hadoop libs (not provided by the Python azure-* SDKs):
  org.apache.hadoop:hadoop-azure — provides SecureAzureBlobFileSystem

Auth: set AZURE_STORAGE_ACCOUNT_KEY for the storage account (or switch to OAuth later).
If this key was ever committed or pasted into chat, rotate it in Azure Portal.
"""
import os

from azure.identity._credentials import client_secret
from pyspark.sql import SparkSession

account_name = "mtsumurasafety"
# account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")
# if not account_key:
#     raise RuntimeError(
#         "Set AZURE_STORAGE_ACCOUNT_KEY to the storage account access key "
#         "(or change this script to use OAuth / SAS for Spark)."
#     )

# Match Hadoop client bundled with PySpark 4.1.x (see pyspark/jars/hadoop-client-api-*.jar).
hadoop_azure_pkg = "org.apache.hadoop:hadoop-azure:3.4.2"

spark = (
    SparkSession.builder.appName("ADLS Test")
    .config("spark.jars.packages", hadoop_azure_pkg)
    .getOrCreate()
)

# Account key auth (SharedKey) for abfss
# spark.conf.set(
#     f"fs.azure.account.auth.type.{account_name}.dfs.core.windows.net",
#     "SharedKey",
# )
# spark.conf.set(
#     f"fs.azure.account.key.{account_name}.dfs.core.windows.net",
#     account_key,
# )

# service principal implementation

tenant_id="a65e0f8e-36fb-4683-ae3b-364611bdab10"
client_id="048fb0e7-82c6-4ab8-82c8-b006988c5acb"
client_secret = os.environ.get("AZURE_CLIENT_SECRET")


spark.conf.set(
    f"fs.azure.account.auth.type.{account_name}.dfs.core.windows.net",
    "OAuth"
)

spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{account_name}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{account_name}.dfs.core.windows.net",
    f"{client_id}"
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{account_name}.dfs.core.windows.net",
    f"{client_secret}"
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.mtsumurasafety.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
)

path = "abfss://safety-knife-bronze@mtsumurasafety.dfs.core.windows.net/test-folder4/"

df = spark.createDataFrame(
    [("11", "knife"), ("22", "safety")],
    ["id", "name"],
)

df.write.mode("overwrite").parquet(path)
print("Write successful")

df_read = spark.read.parquet(path)
df_read.show()
