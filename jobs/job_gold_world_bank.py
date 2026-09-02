"""
Job: Gold World Bank Contexto Macroeconômico
Analisa PIB e Desemprego para contexto global.

Depende de:
  - <catalog>.<env>_silver.world_bank

Produz:
  - <catalog>.<env>_gold.contexto_macroeconomico

Ou via Databricks Workflow:
    Task: t3_gold_world_bank
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
from src.gold.world_bank_analise import analisar_contexto_macro
from src.config.tables import SCHEMA_GOLD, SCHEMA_SILVER


def main():
    inicio = datetime.now()
    info("job_gold_world_bank", f"=== JOB GOLD WORLD BANK INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id = get_secret("client-id")
    tenant_id = get_secret("tenant-id")
    client_secret = get_secret("client-secret")
    storage_account = get_secret("storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    analisar_contexto_macro(spark)

    fim = datetime.now()
    info("job_gold_world_bank", "\n=== JOB GOLD WORLD BANK CONCLUIDO ===")
    info("job_gold_world_bank", f"Duracao: {(fim - inicio).total_seconds():.2f}s")

if __name__ == "__main__":
    main()
