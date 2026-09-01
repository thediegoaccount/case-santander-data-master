"""
Job: Carga SQL - Indicadores Macroeconômicos
Carrega tabelas de análise macroeconômica no Azure SQL Database.

Tabelas carregadas:
  - case_santander.gold.indicadores_bcb
  - case_santander.gold.contexto_macroeconomico
  - case_santander.gold.acoes_vs_cambio

Ou via Databricks Workflow:
    Task: t_carga_sql_macro
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
    info("job_carga_sql_macro", f"=== JOB CARGA SQL MACRO INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()
    sql_conn = get_secret("sql-connection-string")

    tabelas = [
        "case_santander.gold.indicadores_bcb",
        "case_santander.gold.contexto_macroeconomico",
        "case_santander.gold.acoes_vs_cambio",
    ]

    info("job_carga_sql_macro", "Carregando tabelas macroeconômicas no SQL Database...")
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
            info("job_carga_sql_macro", f"   {tabela_sql}: {e}")
            falha += 1

    if falha == 0:
        info("job_carga_sql_macro", f"   {sucesso} tabelas carregadas com sucesso")
    else:
        info("job_carga_sql_macro", f"   {sucesso} tabelas OK, {falha} falharam")

    fim = datetime.now()
    info("job_carga_sql_macro", "\n=== JOB CARGA SQL MACRO CONCLUIDO ===")
    info("job_carga_sql_macro", f"Duracao: {(fim - inicio).total_seconds():.2f}s")


main()
