"""
Job: Silver Ações
"""

import sys
from src.config.environment import setup_python_path

setup_python_path()
from src.config.logging import info, error, warning

from datetime import datetime

from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils
from src.config.secrets import get_secret

from src.config.settings import configure_adls
from src.transformation.silver_acoes import transformar_acoes


def main():
    inicio = datetime.now()
    info("job_silver_acoes", f"=== JOB SILVER_ACOES INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id = get_secret("client-id")
    tenant_id = get_secret("tenant-id")
    client_secret = get_secret("client-secret")
    storage_account = get_secret("storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    total_acoes = transformar_acoes(spark, storage_account)

    fim = datetime.now()
    info("job_silver_acoes", "\n=== JOB SILVER_ACOES CONCLUIDO ===")
    info("job_silver_acoes", "Acoes gravadas")
    info("job_silver_acoes", f"Duracao:    {(fim - inicio).total_seconds():.2f}s")

if __name__ == "__main__":
    main()
