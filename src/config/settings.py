# Acoes monitoradas
ACOES = [
    "PETR4.SA",  # Petrobras
    "VALE3.SA",  # Vale
    "ITUB4.SA",  # Itau
    "BBDC4.SA",  # Bradesco
    "ABEV3.SA",  # Ambev
    "MGLU3.SA",  # Magazine Luiza
    "WEGE3.SA",  # WEG
    "BBAS3.SA",  # Banco do Brasil
    "SANB11.SA",  # Santander
]


def get_paths(storage_account: str) -> dict:
    return {
        "bronze_acoes": f"abfss://bronze@{storage_account}.dfs.core.windows.net/acoes/",
        "bronze_bcb": f"abfss://bronze@{storage_account}.dfs.core.windows.net/bcb/",
        "bronze_world_bank": f"abfss://bronze@{storage_account}.dfs.core.windows.net/world_bank/",
        "bronze_kafka": f"abfss://bronze@{storage_account}.dfs.core.windows.net/kafka/",
        "bronze_clientes": f"abfss://bronze@{storage_account}.dfs.core.windows.net/clientes/",
        "bronze_ordens": f"abfss://bronze@{storage_account}.dfs.core.windows.net/ordens/",
        "silver_acoes": f"abfss://silver@{storage_account}.dfs.core.windows.net/acoes/",
        "silver_bcb": f"abfss://silver@{storage_account}.dfs.core.windows.net/bcb/",
        "silver_world_bank": f"abfss://silver@{storage_account}.dfs.core.windows.net/world_bank/",
        "silver_streaming": f"abfss://silver@{storage_account}.dfs.core.windows.net/streaming/",
        "silver_clientes": f"abfss://silver@{storage_account}.dfs.core.windows.net/clientes/",
        "silver_ordens": f"abfss://silver@{storage_account}.dfs.core.windows.net/ordens/",
        "gold_anomalias": f"abfss://gold@{storage_account}.dfs.core.windows.net/anomalias/",
        "gold_performance": f"abfss://gold@{storage_account}.dfs.core.windows.net/performance_acoes/",
        "gold_cambio": f"abfss://gold@{storage_account}.dfs.core.windows.net/acoes_vs_cambio/",
        "gold_observ": f"abfss://gold@{storage_account}.dfs.core.windows.net/observabilidade/",
    }


def configure_adls(spark, storage_account, client_id, tenant_id, client_secret):
    """Configura autenticacao OAuth para o ADLS"""
    spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    )


def get_spark():
    """Retorna sessao Spark via Databricks Connect"""
    from databricks.connect import DatabricksSession

    return DatabricksSession.builder.getOrCreate()


def get_credentials(dbutils):
    """Busca credenciais do Key Vault via get_secret (dinâmico)"""
    from .secrets import get_secret

    return {
        "client_id": get_secret("client-id"),
        "tenant_id": get_secret("tenant-id"),
        "client_secret": get_secret("client-secret"),
        "storage_account": get_secret("storage-account"),
        "sql_conn": get_secret("sql-connection-string"),
        "kaggle_username": get_secret("kaggle-username"),
        "kaggle_key": get_secret("kaggle-key"),
    }
