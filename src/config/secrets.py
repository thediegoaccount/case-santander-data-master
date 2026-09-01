"""
Secrets Helper - Recuperação Dinâmica de Secrets

Centraliza recuperação de secrets do Key Vault usando configuração dinâmica do ambiente.
"""

from databricks.sdk.runtime import dbutils
from .environment import get_config


def get_secret(key: str):
    """
    Recupera secret do Key Vault dinamicamente

    Usa key_vault do ambiente configurado (HK ou PROD)
    via src/config/environment.py

    Args:
        key: Nome da secret (ex: client-id, client-secret)

    Returns:
        Valor da secret

    Exemplo:
        client_id = get_secret("client-id")
        storage_account = get_secret("storage-account")
    """
    config = get_config()
    key_vault = config["key_vault"]

    return dbutils.secrets.get(scope=key_vault, key=key)


def get_client_id():
    """Recupera client-id do Key Vault"""
    return get_secret("client-id")


def get_client_secret():
    """Recupera client-secret do Key Vault"""
    return get_secret("client-secret")


def get_tenant_id():
    """Recupera tenant-id do Key Vault"""
    return get_secret("tenant-id")


def get_storage_account():
    """Recupera storage-account do Key Vault"""
    return get_secret("storage-account")


def get_kaggle_username():
    """Recupera kaggle-username do Key Vault"""
    return get_secret("kaggle-username")


def get_kaggle_key():
    """Recupera kaggle-key do Key Vault"""
    return get_secret("kaggle-key")


def get_salt():
    """Recupera salt do Key Vault (para anonimização)"""
    return get_secret("salt")
