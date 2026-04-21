import os
from fastmcp.server.auth.providers.azure import AzureProvider
from key_value.aio.stores.redis import RedisStore
from key_value.aio.wrappers.encryption import FernetEncryptionWrapper
from cryptography.fernet import Fernet

def create_auth_provider(redis_client):
    return AzureProvider(
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
        tenant_id=os.environ["AZURE_TENANT_ID"],
        base_url=os.environ["BASE_URL"],
        required_scopes=["mcp-access"],
        jwt_signing_key=os.environ["JWT_SIGNING_KEY"],
        client_storage=FernetEncryptionWrapper(
            key_value=RedisStore(client=redis_client),
            fernet=Fernet(os.environ["STORAGE_ENCRYPTION_KEY"])
        )
    )
