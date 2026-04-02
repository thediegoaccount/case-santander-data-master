"""
Job: SDC Type 2
"""
import sys
sys.path.insert(0, "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")

from databricks.connect import DatabricksSession
from datetime import datetime

from src.clients.sdc import aplicar_sdc_clientes, aplicar_sdc_score_risco


def main():
    inicio = datetime.now()
    print(f"=== JOB SDC INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    total_clientes = aplicar_sdc_clientes(spark)
    total_score    = aplicar_sdc_score_risco(spark)

    fim = datetime.now()
    print(f"\n=== JOB SDC CONCLUIDO ===")
    print(f"Clientes SDC:    {total_clientes} registros ativos")
    print(f"Score Risco SDC: {total_score} registros ativos")
    print(f"Duracao: {(fim - inicio).total_seconds():.2f}s")


main()
