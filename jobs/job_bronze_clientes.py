"""
Job: Bronze Clientes

Carga independente de UMA tabela: <catalog>.<env>_bronze.clientes
Fonte: dataset publico de churn bancario (Kaggle).
"""

from datetime import datetime

from databricks.connect import DatabricksSession

from src.config.environment import setup_python_path
from src.config.logging import info
from src.config.secrets import get_secret
from src.config.settings import configure_adls
from src.ingestion.clientes_kaggle import extrair_clientes

setup_python_path()


def main():
    inicio = datetime.now()
    info("job_bronze_clientes", f"=== JOB BRONZE_CLIENTES INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id = get_secret("client-id")
    tenant_id = get_secret("tenant-id")
    client_secret = get_secret("client-secret")
    storage_account = get_secret("storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    total = extrair_clientes(spark)

    fim = datetime.now()
    info("job_bronze_clientes", "\n=== JOB BRONZE_CLIENTES CONCLUIDO ===")
    info("job_bronze_clientes", f"Registros:  {total}")
    info("job_bronze_clientes", f"Duracao:    {(fim - inicio).total_seconds():.2f}s")


if __name__ == "__main__":
    main()
