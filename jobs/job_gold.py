"""
Job: Analises Gold
Responsavel pelos calculos e analises da camada Gold.

Execucao:
    python jobs/job_gold.py

Ou via Databricks Workflow:
    Task: t3_gold
"""
from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.settings import configure_adls
from src.gold.anomalias import detectar_anomalias
from src.gold.performance import calcular_performance
from src.gold.fraude import detectar_fraude


def main():
    inicio = datetime.now()
    print(f"=== JOB GOLD INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id       = dbutils.secrets.get(scope="kv-case-santander", key="client-id")
    tenant_id       = dbutils.secrets.get(scope="kv-case-santander", key="tenant-id")
    client_secret   = dbutils.secrets.get(scope="kv-case-santander", key="client-secret")
    storage_account = dbutils.secrets.get(scope="kv-case-santander", key="storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    total_anomalias  = detectar_anomalias(spark, storage_account)
    total_performance = calcular_performance(spark, storage_account)
    total_fraudes    = detectar_fraude(spark)

    fim = datetime.now()
    duracao = (fim - inicio).total_seconds()

    print(f"\n=== JOB GOLD CONCLUIDO ===")
    print(f"Anomalias:   {total_anomalias}")
    print(f"Performance: {total_performance} registros")
    print(f"Fraudes:     {total_fraudes} criticos")
    print(f"Duracao:     {duracao:.2f}s")


if __name__ == "__main__":
    main()
