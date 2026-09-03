"""
Job: Silver Clientes

Carga independente de UMA tabela: <catalog>.<env>_silver.clientes
Depende de: <catalog>.<env>_bronze.clientes
"""

from datetime import datetime

from databricks.connect import DatabricksSession

from src.config.environment import setup_python_path
from src.config.logging import info
from src.config.secrets import get_secret
from src.config.settings import configure_adls
from src.transformation.silver_clientes import transformar_clientes

setup_python_path()


def main():
    inicio = datetime.now()
    info("job_silver_clientes", f"=== JOB SILVER_CLIENTES INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id = get_secret("client-id")
    tenant_id = get_secret("tenant-id")
    client_secret = get_secret("client-secret")
    storage_account = get_secret("storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    total = transformar_clientes(spark)

    fim = datetime.now()
    info("job_silver_clientes", "\n=== JOB SILVER_CLIENTES CONCLUIDO ===")
    info("job_silver_clientes", f"Registros:  {total}")
    info("job_silver_clientes", f"Duracao:    {(fim - inicio).total_seconds():.2f}s")


if __name__ == "__main__":
    main()
