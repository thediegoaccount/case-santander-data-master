"""
Job: Carga SQL - Streaming
Carrega tabelas de análise intraday em tempo real no Azure SQL Database.

Tabelas carregadas:
  - case_santander.gold.fraude_streaming
  - case_santander.gold.anomalias_intraday
  - case_santander.gold.volume_intraday
  - case_santander.gold.ranking_acoes_realtime

Ou via Databricks Workflow:
    Task: t_carga_sql_streaming
"""

import sys
from src.config.environment import setup_python_path

setup_python_path()
from src.config.logging import info, error, warning

from datetime import datetime

from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils
from src.config.secrets import get_secret


def main():
    inicio = datetime.now()
    info("job_carga_sql_streaming", f"=== JOB CARGA SQL STREAMING INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()
    sql_conn = get_secret("sql-connection-string")

    tabelas = [
        "case_santander.gold.fraude_streaming",
        "case_santander.gold.anomalias_intraday",
        "case_santander.gold.volume_intraday",
        "case_santander.gold.ranking_acoes_realtime",
    ]

    info("job_carga_sql_streaming", "Carregando tabelas de streaming no SQL Database...")
    sucesso = 0
    falha = 0

    for tabela_uc in tabelas:
        tabela_sql = f"dbo.{tabela_uc.split('.')[-1]}"
        try:
            spark.sql(f"SELECT * FROM {tabela_uc}") \
                .write \
                .format("jdbc") \
                .option("url", sql_conn) \
                .option("dbtable", tabela_sql) \
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                .mode("overwrite") \
                .save()
            sucesso += 1
        except Exception as e:
            info("job_carga_sql_streaming", f"   {tabela_sql}: {e}")
            falha += 1

    if falha == 0:
        info("job_carga_sql_streaming", f"   {sucesso} tabelas carregadas com sucesso")
    else:
        info("job_carga_sql_streaming", f"   {sucesso} tabelas OK, {falha} falharam")

    fim = datetime.now()
    info("job_carga_sql_streaming", "\n=== JOB CARGA SQL STREAMING CONCLUIDO ===")
    info("job_carga_sql_streaming", f"Duracao: {(fim - inicio).total_seconds():.2f}s")


main()
