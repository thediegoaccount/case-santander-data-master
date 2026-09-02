"""
Job: SCD Type 2
"""

import sys
from src.config.environment import setup_python_path

setup_python_path()
from src.config.logging import info, error, warning

from datetime import datetime

from databricks.connect import DatabricksSession

from src.clients.scd import aplicar_scd_clientes, aplicar_scd_score_risco


def main():
    inicio = datetime.now()
    info("job_scd", f"=== JOB SCD INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    total_clientes = aplicar_scd_clientes(spark)
    total_score = aplicar_scd_score_risco(spark)

    fim = datetime.now()
    info("job_scd", "\n=== JOB SCD CONCLUIDO ===")
    info("job_scd", f"Clientes SCD:    {total_clientes} registros ativos")
    info("job_scd", f"Score Risco SCD: {total_score} registros ativos")
    info("job_scd", f"Duracao: {(fim - inicio).total_seconds():.2f}s")

if __name__ == "__main__":
    main()
