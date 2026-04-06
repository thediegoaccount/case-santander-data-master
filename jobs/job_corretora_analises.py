"""
Job: Corretora Analises
Calcula posicao de carteira, score de risco e deteccao de fraude.

Ou via Databricks Workflow:
    Task: t7_corretora_analises
"""
import sys
sys.path.insert(0, "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")

from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime


def main():
    inicio = datetime.now()
    print(f"=== JOB CORRETORA ANALISES INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()
    sql_conn  = dbutils.secrets.get(scope="kv-case-santander", key="sql-connection-string")
    data_hoje = datetime.now().strftime("%Y-%m-%d")

    # GOLD 1 — Posicao do cliente (carteira)
    print("Calculando posicao dos clientes...")
    df_ordens   = spark.sql("SELECT * FROM case_santander.silver.ordens")
    df_clientes = spark.sql("""
        SELECT hash_cliente, perfil_risco, faixa_saldo, score_credito 
        FROM case_santander.silver.clientes
    """)

    df_posicao = df_ordens \
        .groupBy("hash_cliente", "ticker") \
        .agg(
            F.sum(F.when(F.col("tipo") == "compra", F.col("quantidade"))
                .otherwise(-F.col("quantidade"))).alias("quantidade_liquida"),
            F.sum(F.when(F.col("tipo") == "compra", F.col("valor_total"))
                .otherwise(0)).alias("total_comprado"),
            F.sum(F.when(F.col("tipo") == "venda", F.col("valor_total"))
                .otherwise(0)).alias("total_vendido"),
            F.count("*").alias("total_ordens"),
            F.sum(F.when(F.col("status") == "executada", 1).otherwise(0)).alias("ordens_executadas"),
            F.sum(F.when(F.col("status") == "cancelada", 1).otherwise(0)).alias("ordens_canceladas")
        ) \
        .withColumn("valor_investido",    F.round(F.col("total_comprado") - F.col("total_vendido"), 2)) \
        .withColumn("resultado_estimado", F.round(F.col("total_vendido") - F.col("total_comprado"), 2)) \
        .withColumn("situacao",
            F.when(F.col("quantidade_liquida") > 0, "Comprado")
            .when(F.col("quantidade_liquida") < 0, "Vendido a Descoberto")
            .otherwise("Zerado")) \
        .withColumn("data_processamento", F.lit(data_hoje)) \
        .join(F.broadcast(df_clientes), on="hash_cliente", how="left")

    df_posicao.write.format("delta").mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("case_santander.gold.posicao_clientes")
    print(f"✅ gold.posicao_clientes → {df_posicao.count()} registros")

    # GOLD 2 — Score de risco
    print("Calculando score de risco...")
    df_score = df_posicao \
        .groupBy("hash_cliente", "perfil_risco", "faixa_saldo", "score_credito") \
        .agg(
            F.count("ticker").alias("num_ativos"),
            F.sum("total_ordens").alias("total_ordens"),
            F.sum("ordens_canceladas").alias("total_canceladas"),
            F.sum("valor_investido").alias("valor_total_investido"),
            F.sum(F.when(F.col("situacao") == "Vendido a Descoberto", 1)
                .otherwise(0)).alias("posicoes_descoberto"),
            F.round(F.avg("resultado_estimado"), 2).alias("resultado_medio")
        ) \
        .withColumn("taxa_cancelamento_pct",
            F.round(F.col("total_canceladas") / F.col("total_ordens") * 100, 2)) \
        .withColumn("score_credito_norm",
            F.round(F.col("score_credito") / 850 * 100, 2)) \
        .withColumn("score_perfil",
            F.when(F.col("perfil_risco") == "Arrojado", 100)
            .when(F.col("perfil_risco") == "Moderado", 60)
            .otherwise(30)) \
        .withColumn("score_saldo",
            F.when(F.col("faixa_saldo") == "Alto", 100)
            .when(F.col("faixa_saldo") == "Medio", 60)
            .when(F.col("faixa_saldo") == "Baixo", 30)
            .otherwise(10)) \
        .withColumn("score_comportamento",
            F.when(F.col("taxa_cancelamento_pct") < 20, 100)
            .when(F.col("taxa_cancelamento_pct") < 50, 60)
            .otherwise(20)) \
        .withColumn("score_risco",
            F.round(
                F.col("score_credito_norm")  * 0.4 +
                F.col("score_perfil")        * 0.2 +
                F.col("score_saldo")         * 0.2 +
                F.col("score_comportamento") * 0.2, 2)) \
        .withColumn("categoria_risco",
            F.when(F.col("score_risco") >= 70, "Baixo Risco")
            .when(F.col("score_risco") >= 50, "Risco Moderado")
            .when(F.col("score_risco") >= 30, "Risco Alto")
            .otherwise("Risco Critico")) \
        .withColumn("limite_operacional",
            F.when(F.col("categoria_risco") == "Baixo Risco",   500000)
            .when(F.col("categoria_risco") == "Risco Moderado", 200000)
            .when(F.col("categoria_risco") == "Risco Alto",      50000)
            .otherwise(10000)) \
        .withColumn("data_processamento", F.lit(data_hoje))

    df_score.write.format("delta").mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("case_santander.gold.score_risco_clientes")
    print(f"✅ gold.score_risco_clientes → {df_score.count()} registros")

    # GOLD 3 — Perfil clientes
    df_perfil = spark.sql("""
        SELECT 
            perfil_risco, faixa_etaria, score_categoria, pais,
            COUNT(*) as total_clientes,
            ROUND(AVG(saldo), 2) as saldo_medio,
            ROUND(AVG(score_credito), 0) as score_medio,
            ROUND(AVG(salario_estimado), 2) as salario_medio,
            SUM(CASE WHEN churn THEN 1 ELSE 0 END) as total_churn,
            ROUND(AVG(CASE WHEN churn THEN 1.0 ELSE 0.0 END) * 100, 2) as taxa_churn_pct
        FROM case_santander.silver.clientes
        GROUP BY perfil_risco, faixa_etaria, score_categoria, pais
        ORDER BY total_clientes DESC
    """)
    df_perfil.write.format("delta").mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("case_santander.gold.perfil_clientes")
    print(f"✅ gold.perfil_clientes → {df_perfil.count()} registros")

    # GOLD 4 — Ordens consolidadas
    df_ordens_gold = spark.sql("""
        SELECT ticker, perfil_risco, faixa_saldo, tipo, status, ano,
            COUNT(*) as total_ordens,
            ROUND(SUM(valor_total), 2) as volume_total,
            ROUND(AVG(preco), 2) as preco_medio,
            ROUND(AVG(quantidade), 0) as qtd_media
        FROM case_santander.silver.ordens
        GROUP BY ticker, perfil_risco, faixa_saldo, tipo, status, ano
        ORDER BY volume_total DESC
    """)
    df_ordens_gold.write.format("delta").mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("case_santander.gold.ordens_consolidadas")
    print(f"✅ gold.ordens_consolidadas → {df_ordens_gold.count()} registros")

    # GOLD 5 — Ranking acoes por perfil
    df_ranking = spark.sql("""
        SELECT ticker, perfil_risco,
            COUNT(*) as total_ordens,
            ROUND(SUM(valor_total), 2) as volume_total,
            ROUND(AVG(preco), 2) as preco_medio
        FROM case_santander.silver.ordens
        WHERE status = 'executada'
        GROUP BY ticker, perfil_risco
        ORDER BY volume_total DESC
    """)
    df_ranking.write.format("delta").mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("case_santander.gold.ranking_acoes_perfil")
    print(f"✅ gold.ranking_acoes_perfil → {df_ranking.count()} registros")

    # Carga SQL Database
    print("Carregando no SQL Database...")
    tabelas_sql = [
        ("case_santander.gold.perfil_clientes",    "dbo.perfil_clientes"),
        ("case_santander.gold.ordens_consolidadas","dbo.ordens_consolidadas"),
        ("case_santander.gold.ranking_acoes_perfil","dbo.ranking_acoes_perfil"),
        ("case_santander.gold.posicao_clientes",   "dbo.posicao_clientes"),
        ("case_santander.gold.score_risco_clientes","dbo.score_risco_clientes"),
    ]

    for tabela_uc, tabela_sql in tabelas_sql:
        try:
            df = spark.sql(f"SELECT * FROM {tabela_uc}")
            df.write.format("jdbc") \
                .option("url", sql_conn) \
                .option("dbtable", tabela_sql) \
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                .mode("overwrite").save()
            print(f"  ✅ {tabela_sql} → {df.count()} registros")
        except Exception as e:
            print(f"  ❌ {tabela_sql} → {e}")

    fim = datetime.now()
    print(f"\n=== JOB CORRETORA ANALISES CONCLUIDO ===")
    print(f"Duracao: {(fim - inicio).total_seconds():.2f}s")


main()
