"""
Job: Exportar Amostra de Clientes para Streaming

Exporta uma amostra de hash_cliente de bronze.clientes para ADLS, consumida
pelos producers de streaming (scripts/eventhub_producer*.py), que rodam
fora do Databricks e nao tem acesso a Unity Catalog.

Depende de: <catalog>.<env>_bronze.clientes
Roda DEPOIS de job_bronze_clientes.
"""

from datetime import datetime

from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils

from src.config.environment import setup_python_path
from src.config.logging import info
from src.config.secrets import get_secret
from src.config.settings import configure_adls
from src.ingestion.exportar_amostra_streaming import exportar_amostra_clientes

setup_python_path()


def main():
    inicio = datetime.now()
    info("job_exportar_amostra_streaming", f"=== JOB EXPORTAR_AMOSTRA_STREAMING INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id = get_secret("client-id")
    tenant_id = get_secret("tenant-id")
    client_secret = get_secret("client-secret")
    storage_account = get_secret("storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    total = exportar_amostra_clientes(spark, dbutils, storage_account)

    fim = datetime.now()
    info("job_exportar_amostra_streaming", "\n=== JOB EXPORTAR_AMOSTRA_STREAMING CONCLUIDO ===")
    info("job_exportar_amostra_streaming", f"Clientes exportados: {total}")
    info("job_exportar_amostra_streaming", f"Duracao:    {(fim - inicio).total_seconds():.2f}s")


if __name__ == "__main__":
    main()
