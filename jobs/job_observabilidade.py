"""
Job: Observabilidade
"""
import sys
sys.path.insert(0, "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")

from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils
from datetime import datetime

from src.config.settings import configure_adls
from src.observability.monitoring import executar_monitoramento


def main():
    inicio = datetime.now()
    print(f"=== JOB OBSERVABILIDADE INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id       = dbutils.secrets.get(scope="kv-case-santander", key="client-id")
    tenant_id       = dbutils.secrets.get(scope="kv-case-santander", key="tenant-id")
    client_secret   = dbutils.secrets.get(scope="kv-case-santander", key="client-secret")
    storage_account = dbutils.secrets.get(scope="kv-case-santander", key="storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    resultados = executar_monitoramento(spark, storage_account)

    if resultados:
        df_metricas = spark.createDataFrame(resultados)
        df_metricas.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("case_santander.gold.observabilidade")

    fim = datetime.now()
    print(f"\n=== JOB OBSERVABILIDADE CONCLUIDO ===")
    print(f"Tabelas monitoradas: {len(resultados)}")
    print(f"Duracao: {(fim - inicio).total_seconds():.2f}s")


main()
