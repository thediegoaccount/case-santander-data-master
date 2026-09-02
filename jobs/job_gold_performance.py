"""
Job: Gold Performance
Calcula performance das ações por setor e período.

Depende de:
  - <catalog>.<env>_silver.acoes

Produz:
  - <catalog>.<env>_gold.performance_acoes

Ou via Databricks Workflow:
    Task: t3_performance
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
from src.gold.performance import calcular_performance
from src.config.tables import SCHEMA_GOLD, SCHEMA_SILVER


def main():
    inicio = datetime.now()
    info("job_gold_performance", f"=== JOB GOLD PERFORMANCE INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id = get_secret("client-id")
    tenant_id = get_secret("tenant-id")
    client_secret = get_secret("client-secret")
    storage_account = get_secret("storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    calcular_performance(spark, storage_account)

    fim = datetime.now()
    info("job_gold_performance", "\n=== JOB GOLD PERFORMANCE CONCLUIDO ===")
    info("job_gold_performance", f"Duracao: {(fim - inicio).total_seconds():.2f}s")

if __name__ == "__main__":
    main()
