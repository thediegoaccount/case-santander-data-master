"""
Job: Observabilidade
"""
import sys
sys.path.insert(0, "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")

from databricks.connect import DatabricksSession
from datetime import datetime

from src.observability.monitoring import executar_monitoramento


def main():
    inicio = datetime.now()
    print(f"=== JOB OBSERVABILIDADE INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    resultados = executar_monitoramento(spark)

    if resultados:
        # Drop e recria para evitar conflito de schema
        spark.sql("DROP TABLE IF EXISTS case_santander.gold.observabilidade")

        df_metricas = spark.createDataFrame(resultados)
        df_metricas.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable("case_santander.gold.observabilidade")

    fim = datetime.now()
    print(f"\n=== JOB OBSERVABILIDADE CONCLUIDO ===")
    print(f"Tabelas monitoradas: {len(resultados)}")
    print(f"Duracao: {(fim - inicio).total_seconds():.2f}s")


main()
