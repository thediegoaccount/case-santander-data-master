"""
Job: Gold Fraude
Detecta fraudes em ordens de clientes cruzando dados de ordens com score de risco.

Depende de:
  - case_santander.silver.ordens
  - case_santander.gold.score_risco_clientes

Produz:
  - case_santander.gold.deteccao_fraude

Ou via Databricks Workflow:
    Task: t3_fraude
"""

import sys
from src.config.environment import setup_python_path

setup_python_path()
from src.config.logging import info, error, warning

from datetime import datetime

from databricks.connect import DatabricksSession

from src.gold.fraude import detectar_fraude


def main():
    inicio = datetime.now()
    info("job_gold_fraude", f"=== JOB GOLD FRAUDE INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    detectar_fraude(spark)

    fim = datetime.now()
    info("job_gold_fraude", "\n=== JOB GOLD FRAUDE CONCLUIDO ===")
    info("job_gold_fraude", f"Duracao: {(fim - inicio).total_seconds():.2f}s")


main()
