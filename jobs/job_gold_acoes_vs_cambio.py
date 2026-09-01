"""
Job: Gold Ações vs Câmbio
Correlaciona performance de ações com variações do câmbio USD/BRL.

Depende de:
  - case_santander.silver.acoes
  - case_santander.silver.bcb

Produz:
  - case_santander.gold.acoes_vs_cambio

Ou via Databricks Workflow:
    Task: t3_gold_acoes_vs_cambio
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
from src.gold.correlacao_acoes_cambio import correlacionar_acoes_cambio


def main():
    inicio = datetime.now()
    info("job_gold_acoes_vs_cambio", f"=== JOB GOLD ACOES VS CAMBIO INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id = get_secret("client-id")
    tenant_id = get_secret("tenant-id")
    client_secret = get_secret("client-secret")
    storage_account = get_secret("storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    correlacionar_acoes_cambio(spark)

    fim = datetime.now()
    info("job_gold_acoes_vs_cambio", "\n=== JOB GOLD ACOES VS CAMBIO CONCLUIDO ===")
    info("job_gold_acoes_vs_cambio", f"Duracao: {(fim - inicio).total_seconds():.2f}s")


main()
