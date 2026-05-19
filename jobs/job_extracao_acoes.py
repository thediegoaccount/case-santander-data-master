"""
Job: Extracao de Ações
"""

import sys

sys.path.insert(0, "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")

from datetime import datetime

from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils

from src.config.settings import configure_adls
from src.ingestion.yahoo_finance import extrair_acoes


def main():
    inicio = datetime.now()
    print(f"=== JOB EXTRACAO_ACOES INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id = dbutils.secrets.get(scope="kv-case-santander", key="client-id")
    tenant_id = dbutils.secrets.get(scope="kv-case-santander", key="tenant-id")
    client_secret = dbutils.secrets.get(scope="kv-case-santander", key="client-secret")
    storage_account = dbutils.secrets.get(scope="kv-case-santander", key="storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    total_acoes = extrair_acoes(spark, storage_account)

    fim = datetime.now()
    print("\n=== JOB EXTRACAO_ACOES CONCLUIDO ===")
    print("Acoes gravadas")
    print(f"Duracao:    {(fim - inicio).total_seconds():.2f}s")


main()
