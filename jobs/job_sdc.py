"""
Job: SDC Type 2
Aplica Slowly Changing Dimensions nas tabelas de clientes
e score de risco, mantendo historico completo de mudancas.

Execucao:
    python jobs/job_sdc.py

Ou via Databricks Workflow:
    Task: t9_sdc
"""
from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.settings import configure_adls
from src.clients.sdc import aplicar_sdc_clientes, aplicar_sdc_score_risco


def main():
    inicio = datetime.now()
    print(f"=== JOB SDC INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id       = dbutils.secrets.get(scope="kv-case-santander", key="client-id")
    tenant_id       = dbutils.secrets.get(scope="kv-case-santander", key="tenant-id")
    client_secret   = dbutils.secrets.get(scope="kv-case-santander", key="client-secret")
    storage_account = dbutils.secrets.get(scope="kv-case-santander", key="storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    total_clientes = aplicar_sdc_clientes(spark, storage_account)
    total_score    = aplicar_sdc_score_risco(spark, storage_account)

    fim = datetime.now()
    duracao = (fim - inicio).total_seconds()

    print(f"\n=== JOB SDC CONCLUIDO ===")
    print(f"SDC Clientes:    {total_clientes} registros ativos")
    print(f"SDC Score Risco: {total_score} registros ativos")
    print(f"Duracao:         {duracao:.2f}s")


if __name__ == "__main__":
    main()
