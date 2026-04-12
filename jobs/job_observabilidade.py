"""
Job: Observabilidade
"""

import sys

sys.path.insert(0, "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")

from datetime import datetime

from databricks.connect import DatabricksSession

from src.observability.monitoring import executar_monitoramento


def main():
    inicio = datetime.now()
    print(f"=== JOB OBSERVABILIDADE INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    resultados = executar_monitoramento(spark)

    if resultados:
        spark.sql("DROP TABLE IF EXISTS case_santander.gold.observabilidade")

        df_metricas = spark.createDataFrame(resultados)
        # fmt: off
        df_metricas.write \
            .format("delta") \
            .saveAsTable("case_santander.gold.observabilidade")
        # fmt: on

    # Manutenção Delta: OPTIMIZE compacta small files e VACUUM remove
    # versões antigas além da janela de retenção (168h = 7 dias)
    print("\nExecutando manutenção Delta (OPTIMIZE + VACUUM)...")
    # fmt: off
    tabelas_manutencao = [
        ("case_santander.silver.acoes",              "ticker, ano, mes"),
        ("case_santander.silver.ordens",             "hash_cliente, ticker"),
        ("case_santander.silver.clientes",           "hash_cliente"),
        ("case_santander.silver.streaming",          "ticker, hora"),
        ("case_santander.gold.anomalias",            "ticker"),
        ("case_santander.gold.performance_acoes",    "ticker, ano"),
        ("case_santander.gold.deteccao_fraude",      "score_fraude"),
        ("case_santander.gold.fraude_streaming",     "score_fraude"),
        ("case_santander.gold.posicao_clientes",     "ticker"),
        ("case_santander.gold.score_risco_clientes", "categoria_risco"),
    ]
    # fmt: on

    for tabela, cols_zorder in tabelas_manutencao:
        try:
            spark.sql(f"OPTIMIZE {tabela} ZORDER BY ({cols_zorder})")
            spark.sql(f"VACUUM {tabela} RETAIN 168 HOURS")
            print(f"  ✅ {tabela}")
        except Exception as e:
            print(f"  ⚠️ {tabela}: {e}")

    fim = datetime.now()
    print("\n=== JOB OBSERVABILIDADE CONCLUIDO ===")
    print(f"Tabelas monitoradas: {len(resultados)}")
    print(f"Duracao: {(fim - inicio).total_seconds():.2f}s")


main()
