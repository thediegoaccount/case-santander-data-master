"""
Slowly Changing Dimensions (SCD) Type 2
"""

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from datetime import datetime


def aplicar_scd_type2(spark: SparkSession, df_novos, tabela_uc: str, chave: str) -> None:
    """
    Aplica SCD Type 2 em uma tabela Delta Lake via Unity Catalog.
    """
    from delta.tables import DeltaTable

    data_hoje = datetime.now().strftime("%Y-%m-%d")

    # fmt: off
    df_com_scd = df_novos \
        .withColumn("data_inicio", F.lit(data_hoje)) \
        .withColumn("data_fim",    F.lit("9999-12-31")) \
        .withColumn("atual",       F.lit(True))
    # fmt: on

    try:
        delta_table = DeltaTable.forName(spark, tabela_uc)

        # Fecha registros antigos
        # fmt: off
        delta_table.alias("antigo") \
            .merge(
                df_novos.alias("novo"),
                f"antigo.{chave} = novo.{chave} AND antigo.atual = true"
            ) \
            .whenMatchedUpdate(set={
                "data_fim": F.lit(data_hoje),
                "atual":    F.lit(False)
            }) \
            .execute()

        # Insere novos registros
        df_com_scd.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(tabela_uc)
        # fmt: on

        print(f"SCD Type 2 atualizado: {tabela_uc}")

    except Exception as e:
        if "is not a Delta table" in str(e) or "Table or view not found" in str(e):
            # Primeira carga
            # fmt: off
            df_com_scd.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable(tabela_uc)
            # fmt: on
            print(f"SCD Type 2 primeira carga: {tabela_uc}")
        else:
            raise e


def aplicar_scd_clientes(spark: SparkSession, storage_account: str = None) -> int:
    """
    Aplica SCD Type 2 na tabela de clientes via Unity Catalog.
    """
    print("Aplicando SCD Type 2 em clientes...")

    df_clientes = spark.sql(
        """
        SELECT
            id_cliente, hash_cliente, sobrenome_masked,
            perfil_risco, score_credito, faixa_saldo,
            faixa_etaria, score_categoria, ativo, churn
        FROM case_santander.silver.clientes
    """
    )

    aplicar_scd_type2(spark, df_clientes, "case_santander.silver.clientes_scd", "hash_cliente")

    total = spark.sql(
        """
        SELECT COUNT(*) as total
        FROM case_santander.silver.clientes_scd
        WHERE atual = true
    """
    ).collect()[0]["total"]

    print(f"SCD clientes — {total} registros ativos")
    return total


def aplicar_scd_score_risco(spark: SparkSession, storage_account: str = None) -> int:
    """
    Aplica SCD Type 2 no score de risco via Unity Catalog.
    """
    print("Aplicando SCD Type 2 em score de risco...")

    df_score = spark.sql(
        """
        SELECT
            hash_cliente, perfil_risco, faixa_saldo,
            score_credito, score_risco, categoria_risco,
            limite_operacional, num_ativos, total_ordens
        FROM case_santander.gold.score_risco_clientes
    """
    )

    aplicar_scd_type2(spark, df_score, "case_santander.gold.score_risco_scd", "hash_cliente")

    total = spark.sql(
        """
        SELECT COUNT(*) as total
        FROM case_santander.gold.score_risco_scd
        WHERE atual = true
    """
    ).collect()[0]["total"]

    print(f"SCD score risco — {total} registros ativos")
    return total
