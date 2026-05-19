"""
Job: Extracao World Bank
"""

import sys

sys.path.insert(0, "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")

from datetime import datetime

from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils

from src.config.settings import configure_adls
from src.ingestion.world_bank import extrair_world_bank


def main():
    inicio = datetime.now()
    print(f"=== JOB EXTRACAO_WORLD_BANK INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id = dbutils.secrets.get(scope="kv-case-santander", key="client-id")
    tenant_id = dbutils.secrets.get(scope="kv-case-santander", key="tenant-id")
    client_secret = dbutils.secrets.get(scope="kv-case-santander", key="client-secret")
    storage_account = dbutils.secrets.get(scope="kv-case-santander", key="storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    total_wb = extrair_world_bank(spark, storage_account)

    fim = datetime.now()
    print("\n=== JOB EXTRACAO_WORLD_BANK CONCLUIDO ===")
    print("World Bank gravado")
    print(f"Duracao:    {(fim - inicio).total_seconds():.2f}s")


main()
