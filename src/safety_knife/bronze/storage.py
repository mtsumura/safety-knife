from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Optional


class StorageBackend(Protocol):
    def exists(self, key: str) -> bool: ...

    def read_bytes(self, key: str) -> bytes: ...

    def write_bytes(self, key: str, data: bytes, *, content_type: Optional[str] = None) -> None: ...


def _normalize_key(key: str) -> str:
    # Keys are always POSIX-like (/) regardless of OS.
    return key.lstrip("/").replace("\\", "/")


@dataclass(frozen=True)
class LocalStorageBackend:
    base_dir: Path

    def _path_for(self, key: str) -> Path:
        return self.base_dir / _normalize_key(key)

    def exists(self, key: str) -> bool:
        return self._path_for(key).exists()

    def read_bytes(self, key: str) -> bytes:
        return self._path_for(key).read_bytes()

    def write_bytes(self, key: str, data: bytes, *, content_type: Optional[str] = None) -> None:
        path = self._path_for(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)


@dataclass(frozen=True)
class AzureBlobStorageBackend:
    """
    Azure Blob Storage backend (works for standard Blob accounts and ADLS Gen2 accounts).

    Uses the Blob endpoint, not the dfs endpoint, because the bronze cache is simple blob IO
    (CSV/JSON). Spark/ABFS integration is separate.
    """

    account_url: str  # e.g. "https://<account>.blob.core.windows.net"
    container: str
    prefix: str = ""
    credential: object | None = None  # account key, SAS token, or TokenCredential

    def _blob_name(self, key: str) -> str:
        key = _normalize_key(key)
        if not self.prefix:
            return key
        return f"{self.prefix.rstrip('/')}/{key}"

    def _client(self):
        from azure.storage.blob import BlobServiceClient

        return BlobServiceClient(account_url=self.account_url, credential=self.credential)

    def exists(self, key: str) -> bool:
        blob = self._client().get_blob_client(container=self.container, blob=self._blob_name(key))
        return blob.exists()

    def read_bytes(self, key: str) -> bytes:
        blob = self._client().get_blob_client(container=self.container, blob=self._blob_name(key))
        return blob.download_blob().readall()

    def write_bytes(self, key: str, data: bytes, *, content_type: Optional[str] = None) -> None:
        blob = self._client().get_blob_client(container=self.container, blob=self._blob_name(key))
        kwargs = {}
        if content_type:
            from azure.storage.blob import ContentSettings

            kwargs["content_settings"] = ContentSettings(content_type=content_type)
        blob.upload_blob(data, overwrite=True, **kwargs)


def get_bronze_raw_backend() -> StorageBackend:
    """
    Configure the raw bronze cache backend (CSV/JSON) via environment variables.

    - Local (default):
        BRONZE_RAW_BACKEND=local
        BRONZE_RAW_LOCAL_BASE=/abs/path/to/data/bronze

    - Azure Blob:
        BRONZE_RAW_BACKEND=azure
        BRONZE_RAW_AZURE_ACCOUNT_URL=https://<account>.blob.core.windows.net
        BRONZE_RAW_AZURE_CONTAINER=<container>
        BRONZE_RAW_AZURE_PREFIX=bronze   (optional)

      Credential options:
        - BRONZE_RAW_AZURE_CONNECTION_STRING=...
        - BRONZE_RAW_AZURE_ACCOUNT_KEY=...
        - or rely on DefaultAzureCredential (requires azure-identity and `az login`)
    """

    backend = (os.environ.get("BRONZE_RAW_BACKEND") or "local").strip().lower()

    if backend == "local":
        base = os.environ.get("BRONZE_RAW_LOCAL_BASE")
        if not base:
            raise RuntimeError("Set BRONZE_RAW_LOCAL_BASE when using BRONZE_RAW_BACKEND=local.")
        return LocalStorageBackend(base_dir=Path(base))

    if backend in {"azure", "blob", "azureblob"}:
        account_url = os.environ.get("BRONZE_RAW_AZURE_ACCOUNT_URL")
        container = os.environ.get("BRONZE_RAW_AZURE_CONTAINER")
        prefix = os.environ.get("BRONZE_RAW_AZURE_PREFIX", "").strip()
        if not account_url or not container:
            raise RuntimeError(
                "Set BRONZE_RAW_AZURE_ACCOUNT_URL and BRONZE_RAW_AZURE_CONTAINER "
                "when using BRONZE_RAW_BACKEND=azure."
            )

        # Prefer explicit secrets if provided, otherwise use DefaultAzureCredential.
        # conn_str = os.environ.get("BRONZE_RAW_AZURE_CONNECTION_STRING")
        # if conn_str:
        #     from azure.storage.blob import BlobServiceClient

        #     svc = BlobServiceClient.from_connection_string(conn_str)
        #     return AzureBlobStorageBackend(
        #         account_url=str(svc.url),
        #         container=container,
        #         prefix=prefix,
        #         credential=None,  # already baked into the client, but we reconstruct via URL; keep None
        #     )

        # Service principal (explicit) — easiest to debug and matches our tests.
        tenant_id = os.environ.get("AZURE_TENANT_ID")
        client_id = os.environ.get("AZURE_CLIENT_ID")
        client_secret = os.environ.get("AZURE_CLIENT_SECRET")
        if tenant_id and client_id and client_secret:
            from azure.identity import ClientSecretCredential

            return AzureBlobStorageBackend(
                account_url=account_url,
                container=container,
                prefix=prefix,
                credential=ClientSecretCredential(
                    tenant_id=tenant_id,
                    client_id=client_id,
                    client_secret=client_secret,
                ),
            )
        else:
            raise RuntimeError("tenant_id, client_id, client_secret is not set")

        # account_key = os.environ.get("BRONZE_RAW_AZURE_ACCOUNT_KEY")
        # if account_key:
        #     return AzureBlobStorageBackend(
        #         account_url=account_url,
        #         container=container,
        #         prefix=prefix,
        #         credential=account_key,
        #     )

        # from azure.identity import DefaultAzureCredential

        # return AzureBlobStorageBackend(
        #     account_url=account_url,
        #     container=container,
        #     prefix=prefix,
        #     credential=DefaultAzureCredential(),
        # )

    raise RuntimeError(f"Unknown BRONZE_RAW_BACKEND={backend!r}. Use 'local' or 'azure'.")

