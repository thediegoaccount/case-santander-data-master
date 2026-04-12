"""
Job: SCD Type 2
"""

import sys

sys.path.insert(0, "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")

from databricks.connect import DatabricksSession
from datetime import datetime

from src.clients.scd import aplicar_scd_clientes, aplicar_scd_score_risco


def main():
    inicio = datetime.now()
    print(f"=== JOB SCD INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    total_clientes = aplicar_scd_clientes(spark)
    total_score = aplicar_scd_score_risco(spark)

    fim = datetime.now()
    print("\n=== JOB SCD CONCLUIDO ===")
    print(f"Clientes SCD:    {total_clientes} registros ativos")
    print(f"Score Risco SCD: {total_score} registros ativos")
    print(f"Duracao: {(fim - inicio).total_seconds():.2f}s")


main()
