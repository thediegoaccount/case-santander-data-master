from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils

def get_spark():
    """Retorna sessão Spark via Databricks Connect"""
    return DatabricksSession.builder.getOrCreate()

def get_credentials():
    """Busca credenciais do Key Vault via dbutils.secrets"""
    return {
        "client_id":       dbutils.secrets.get(scope="kv-case-santander", key="client-id"),
        "tenant_id":       dbutils.secrets.get(scope="kv-case-santander", key="tenant-id"),
        "client_secret":   dbutils.secrets.get(scope="kv-case-santander", key="client-secret"),
        "storage_account": dbutils.secrets.get(scope="kv-case-santander", key="storage-account"),
        "sql_conn":        dbutils.secrets.get(scope="kv-case-santander", key="sql-connection-string"),
        "kaggle_username": dbutils.secrets.get(scope="kv-case-santander", key="kaggle-username"),
        "kaggle_key":      dbutils.secrets.get(scope="kv-case-santander", key="kaggle-key"),
    }

def configure_adls(spark, storage_account, client_id, tenant_id, client_secret):
    """Configura autenticação OAuth para o ADLS"""
    spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# Ações monitoradas
ACOES = [
    "PETR4.SA", "VALE3.SA", "ITUB4.SA", "BBDC4.SA",
    "ABEV3.SA", "MGLU3.SA", "WEGE3.SA", "BBAS3.SA", "SANB11.SA"
]

# Paths ADLS
def get_paths(storage_account):
    return {
        "bronze_acoes":      f"abfss://bronze@{storage_account}.dfs.core.windows.net/acoes/",
        "bronze_bcb":        f"abfss://bronze@{storage_account}.dfs.core.windows.net/bcb/",
        "bronze_world_bank": f"abfss://bronze@{storage_account}.dfs.core.windows.net/world_bank/",
        "bronze_kafka":      f"abfss://bronze@{storage_account}.dfs.core.windows.net/kafka/",
        "bronze_clientes":   f"abfss://bronze@{storage_account}.dfs.core.windows.net/clientes/",
        "bronze_ordens":     f"abfss://bronze@{storage_account}.dfs.core.windows.net/ordens/",
        "silver_acoes":      f"abfss://silver@{storage_account}.dfs.core.windows.net/acoes/",
        "silver_bcb":        f"abfss://silver@{storage_account}.dfs.core.windows.net/bcb/",
        "silver_world_bank": f"abfss://silver@{storage_account}.dfs.core.windows.net/world_bank/",
        "silver_streaming":  f"abfss://silver@{storage_account}.dfs.core.windows.net/streaming/",
        "silver_clientes":   f"abfss://silver@{storage_account}.dfs.core.windows.net/clientes/",
        "silver_ordens":     f"abfss://silver@{storage_account}.dfs.core.windows.net/ordens/",
        "gold_anomalias":    f"abfss://gold@{storage_account}.dfs.core.windows.net/anomalias/",
        "gold_performance":  f"abfss://gold@{storage_account}.dfs.core.windows.net/performance_acoes/",
        "gold_cambio":       f"abfss://gold@{storage_account}.dfs.core.windows.net/acoes_vs_cambio/",
        "gold_observ":       f"abfss://gold@{storage_account}.dfs.core.windows.net/observabilidade/",
    }
