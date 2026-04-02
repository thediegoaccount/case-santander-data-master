"""
Job: SDC Type 2
"""
import sys
sys.path.insert(0, "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")

from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils
from datetime import datetime

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
    print(f"\n=== JOB SDC CONCLUIDO ===")
    print(f"Clientes SDC:    {total_clientes} registros ativos")
    print(f"Score Risco SDC: {total_score} registros ativos")
    print(f"Duracao: {(fim - inicio).total_seconds():.2f}s")


main()
