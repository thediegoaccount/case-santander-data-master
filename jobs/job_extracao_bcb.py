"""
Job: Extracao BCB
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
from src.ingestion.bcb import extrair_bcb


def main():
    inicio = datetime.now()
    info("job_extracao_bcb", f"=== JOB EXTRACAO_BCB INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id = get_secret("client-id")
    tenant_id = get_secret("tenant-id")
    client_secret = get_secret("client-secret")
    storage_account = get_secret("storage-account")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    total_bcb = extrair_bcb(spark, storage_account)

    fim = datetime.now()
    info("job_extracao_bcb", "\n=== JOB EXTRACAO_BCB CONCLUIDO ===")
    info("job_extracao_bcb", "BCB gravado")
    info("job_extracao_bcb", f"Duracao:    {(fim - inicio).total_seconds():.2f}s")


main()
