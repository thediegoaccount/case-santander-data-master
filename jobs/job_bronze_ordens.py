"""
Job: Bronze Ordens

Carga independente de UMA tabela: <catalog>.<env>_bronze.ordens
Depende de: <catalog>.<env>_bronze.clientes (amostra de clientes
para simular as ordens). Rodar DEPOIS de job_bronze_clientes.
"""

from datetime import datetime

from databricks.connect import DatabricksSession

from src.config.environment import setup_python_path
from src.config.logging import info
from src.config.secrets import get_secret
from src.config.settings import configure_adls
from src.ingestion.ordens_simuladas import gerar_ordens

setup_python_path()


def main():
    inicio = datetime.now()
    info("job_bronze_ordens", f"=== JOB BRONZE_ORDENS INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id = get_secret("client-id")
    tenant_id = get_secret("tenant-id")
    client_secret = get_secret("client-secret")
    storage_account = get_secret("storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    total = gerar_ordens(spark)

    fim = datetime.now()
    info("job_bronze_ordens", "\n=== JOB BRONZE_ORDENS CONCLUIDO ===")
    info("job_bronze_ordens", f"Registros:  {total}")
    info("job_bronze_ordens", f"Duracao:    {(fim - inicio).total_seconds():.2f}s")


if __name__ == "__main__":
    main()
