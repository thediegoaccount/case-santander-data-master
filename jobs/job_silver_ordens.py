"""
Job: Silver Ordens

Carga independente de UMA tabela: <catalog>.<env>_silver.ordens
Depende de: <catalog>.<env>_bronze.ordens
"""

from datetime import datetime

from databricks.connect import DatabricksSession

from src.config.environment import setup_python_path
from src.config.logging import info
from src.config.secrets import get_secret
from src.config.settings import configure_adls
from src.transformation.silver_ordens import transformar_ordens

setup_python_path()


def main():
    inicio = datetime.now()
    info("job_silver_ordens", f"=== JOB SILVER_ORDENS INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id = get_secret("client-id")
    tenant_id = get_secret("tenant-id")
    client_secret = get_secret("client-secret")
    storage_account = get_secret("storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    total = transformar_ordens(spark)

    fim = datetime.now()
    info("job_silver_ordens", "\n=== JOB SILVER_ORDENS CONCLUIDO ===")
    info("job_silver_ordens", f"Registros:  {total}")
    info("job_silver_ordens", f"Duracao:    {(fim - inicio).total_seconds():.2f}s")


if __name__ == "__main__":
    main()
