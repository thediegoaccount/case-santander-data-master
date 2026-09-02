"""
Job: Gold BCB Indicadores
Analisa Selic, Câmbio USD/BRL e IPCA do Banco Central.

Depende de:
  - <catalog>.<env>_silver.bcb

Produz:
  - <catalog>.<env>_gold.indicadores_bcb

Ou via Databricks Workflow:
    Task: t3_gold_bcb
"""

import sys
from src.config.environment import setup_python_path

setup_python_path()
from src.config.logging import info, error, warning

from datetime import datetime

from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils
from src.config.secrets import get_secret

from src.config.settings import configure_adls
from src.gold.bcb_analise import analisar_indicadores_bcb
from src.config.tables import SCHEMA_GOLD, SCHEMA_SILVER


def main():
    inicio = datetime.now()
    info("job_gold_bcb", f"=== JOB GOLD BCB INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id = get_secret("client-id")
    tenant_id = get_secret("tenant-id")
    client_secret = get_secret("client-secret")
    storage_account = get_secret("storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    analisar_indicadores_bcb(spark)

    fim = datetime.now()
    info("job_gold_bcb", "\n=== JOB GOLD BCB CONCLUIDO ===")
    info("job_gold_bcb", f"Duracao: {(fim - inicio).total_seconds():.2f}s")

if __name__ == "__main__":
    main()
