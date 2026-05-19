"""
Job: Carga SQL Database
Carrega tabelas Gold no Azure SQL Database.

Ou via Databricks Workflow:
    Task: t_carga_sql
"""

import sys

sys.path.insert(0, "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")

from datetime import datetime

from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils


def main():
    inicio = datetime.now()
    print(f"=== JOB CARGA SQL INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()
    sql_conn = dbutils.secrets.get(scope="kv-case-santander", key="sql-connection-string")

    tabelas = [
        "case_santander.gold.performance_acoes",
        "case_santander.gold.anomalias",
        "case_santander.gold.acoes_vs_cambio",
        "case_santander.gold.perfil_clientes",
        "case_santander.gold.ordens_consolidadas",
        "case_santander.gold.ranking_acoes_perfil",
        "case_santander.gold.posicao_clientes",
        "case_santander.gold.score_risco_clientes",
        "case_santander.gold.deteccao_fraude",
        "case_santander.gold.fraude_streaming",
        "case_santander.gold.anomalias_intraday",
        "case_santander.gold.volume_intraday",
        "case_santander.gold.ranking_acoes_realtime",
    ]

    print("Carregando tabelas no SQL Database...")
    for tabela_uc in tabelas:
        tabela_sql = f"dbo.{tabela_uc.split('.')[-1]}"
        try:
            df = spark.sql(f"SELECT * FROM {tabela_uc}")
            # fmt: off
            df.write \
                .format("jdbc") \
                .option("url", sql_conn) \
                .option("dbtable", tabela_sql) \
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                .mode("overwrite") \
                .save()
            # fmt: on
            print(f"  ✅ {tabela_sql} carregado")
        except Exception as e:
            print(f"  ❌ {tabela_sql} → {e}")

    fim = datetime.now()
    print("\n=== JOB CARGA SQL CONCLUIDO ===")
    print(f"Duracao: {(fim - inicio).total_seconds():.2f}s")


main()
