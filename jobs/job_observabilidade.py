"""
Job: Observabilidade
"""

import sys
from src.config.environment import setup_python_path

setup_python_path()
from src.config.logging import info, error, warning

from datetime import datetime

from databricks.connect import DatabricksSession

from src.observability.monitoring import executar_monitoramento
from src.config.tables import SCHEMA_GOLD, SCHEMA_SILVER


def main():
    inicio = datetime.now()
    info("job_observabilidade", f"=== JOB OBSERVABILIDADE INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    resultados = executar_monitoramento(spark)

    if resultados:
        # Sem DROP: os jobs de streaming leem MAX(versao_cdf) desta tabela
        # como marca d'agua do CDC, e o DROP diario destruia tanto o
        # historico quanto as propriedades da tabela.
        df_metricas = spark.createDataFrame(resultados)
        # fmt: off
        df_metricas.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(f"{SCHEMA_GOLD}.observabilidade")
        # fmt: on

    # Manutenção Delta: OPTIMIZE compacta small files e VACUUM remove
    # versões antigas além da janela de retenção (168h = 7 dias)
    info("job_observabilidade", "\nExecutando manutenção Delta (OPTIMIZE + VACUUM)...")
    # fmt: off
    tabelas_manutencao = [
        (f"{SCHEMA_SILVER}.acoes",              "ticker, ano, mes"),
        (f"{SCHEMA_SILVER}.ordens",             "hash_cliente, ticker"),
        (f"{SCHEMA_SILVER}.clientes",           "hash_cliente"),
        (f"{SCHEMA_SILVER}.streaming",          "ticker, hora"),
        (f"{SCHEMA_GOLD}.anomalias",            "ticker"),
        (f"{SCHEMA_GOLD}.performance_acoes",    "ticker, ano"),
        (f"{SCHEMA_GOLD}.deteccao_fraude",      "score_fraude"),
        (f"{SCHEMA_GOLD}.fraude_streaming",     "score_fraude"),
        (f"{SCHEMA_GOLD}.posicao_clientes",     "ticker"),
        (f"{SCHEMA_GOLD}.score_risco_clientes", "categoria_risco"),
    ]
    # fmt: on

    for tabela, cols_zorder in tabelas_manutencao:
        try:
            spark.sql(f"OPTIMIZE {tabela} ZORDER BY ({cols_zorder})")
            spark.sql(f"VACUUM {tabela} RETAIN 168 HOURS")
            info("job_observabilidade", f"   {tabela}")
        except Exception as e:
            info("job_observabilidade", f"   {tabela}: {e}")

    fim = datetime.now()
    info("job_observabilidade", "\n=== JOB OBSERVABILIDADE CONCLUIDO ===")
    info("job_observabilidade", f"Tabelas monitoradas: {len(resultados)}")
    info("job_observabilidade", f"Duracao: {(fim - inicio).total_seconds():.2f}s")

if __name__ == "__main__":
    main()
