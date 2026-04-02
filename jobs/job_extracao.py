"""
Job: Extracao de Dados
Responsavel pela extracao de dados de todas as fontes
e gravacao na camada Bronze do ADLS.

Execucao:
    python jobs/job_extracao.py
    
Ou via Databricks Workflow:
    Task: t1_extracao
"""
from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.settings import configure_adls
from src.ingestion.yahoo_finance import extrair_acoes
from src.ingestion.bcb import extrair_bcb
from src.ingestion.world_bank import extrair_world_bank


def main():
    inicio = datetime.now()
    print(f"=== JOB EXTRACAO INICIADO: {inicio} ===")

    # Sessao Spark
    spark = DatabricksSession.builder.getOrCreate()

    # Credenciais
    client_id       = dbutils.secrets.get(scope="kv-case-santander", key="client-id")
    tenant_id       = dbutils.secrets.get(scope="kv-case-santander", key="tenant-id")
    client_secret   = dbutils.secrets.get(scope="kv-case-santander", key="client-secret")
    storage_account = dbutils.secrets.get(scope="kv-case-santander", key="storage-account")

    # Configuracao ADLS
    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    # Extracao
    total_acoes = extrair_acoes(spark, storage_account)
    total_bcb   = extrair_bcb(spark, storage_account)
    total_wb    = extrair_world_bank(spark, storage_account)

    fim = datetime.now()
    duracao = (fim - inicio).total_seconds()

    print(f"\n=== JOB EXTRACAO CONCLUIDO ===")
    print(f"Acoes:      {total_acoes} registros")
    print(f"BCB:        {total_bcb} registros")
    print(f"World Bank: {total_wb} registros")
    print(f"Duracao:    {duracao:.2f}s")
    print(f"Fim:        {fim}")


if __name__ == "__main__":
    main()
