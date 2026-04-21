from azure.storage.filedatalake import DataLakeServiceClient
import os

account_name = "mtsumurasafety"
account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")

# service_client = DataLakeServiceClient(
#     account_url=f"https://{account_name}.dfs.core.windows.net",
#     credential=account_key
# )

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

credential = ClientSecretCredential(
    tenant_id="",
    client_id="",
    client_secret= os.environ.get("AZURE_CLIENT_SECRET")
)

service_client = DataLakeServiceClient(
    account_url="https://mtsumurasafety.dfs.core.windows.net",
    credential=credential
)

file_system = service_client.get_file_system_client("safety-knife-bronze")

# Create a test file
file_client = file_system.get_file_client("test-folder/hello-app-subscription.txt")

data = "Hello ADLS Gen2 from Python!"
file_client.create_file()
file_client.append_data(data=data, offset=0, length=len(data))
file_client.flush_data(len(data))

print("Write successful")

# Read it back
download = file_client.download_file()
content = download.readall()

print("Read content:", content.decode())