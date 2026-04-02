"""
Slowly Changing Dimensions (SDC) Type 2
Mantém histórico de mudanças em dimensões de clientes
"""
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from datetime import datetime


def aplicar_sdc_type2(spark: SparkSession, df_novos, 
                      tabela_path: str, chave: str) -> None:
    """
    Aplica SDC Type 2 em uma tabela Delta Lake.
    
    - Fecha registros antigos (data_fim = hoje, atual = false)
    - Insere novos registros (data_fim = 9999-12-31, atual = true)
    
    Args:
        spark:        SparkSession
        df_novos:     DataFrame com novos dados
        tabela_path:  Caminho da tabela Delta
        chave:        Coluna chave para identificar o registro
    """
    from delta.tables import DeltaTable
    data_hoje = datetime.now().strftime("%Y-%m-%d")

    df_com_sdc = df_novos \
        .withColumn("data_inicio", F.lit(data_hoje)) \
        .withColumn("data_fim",    F.lit("9999-12-31")) \
        .withColumn("atual",       F.lit(True))

    if DeltaTable.isDeltaTable(spark, tabela_path):
        delta_table = DeltaTable.forPath(spark, tabela_path)

        # Fecha registros antigos que mudaram
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
        df_com_sdc.write \
            .format("delta") \
            .mode("append") \
            .save(tabela_path)

        print(f"SDC Type 2 aplicado — registros atualizados em {tabela_path}")

    else:
        # Primeira carga
        df_com_sdc.write \
            .format("delta") \
            .mode("overwrite") \
            .save(tabela_path)

        print(f"SDC Type 2 — primeira carga em {tabela_path}")


def aplicar_sdc_clientes(spark: SparkSession, 
                         storage_account: str) -> int:
    """
    Aplica SDC Type 2 na tabela de clientes.
    Rastreia mudancas de perfil de risco ao longo do tempo.

    Returns: total de registros ativos
    """
    silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/clientes/"
    sdc_path    = f"abfss://silver@{storage_account}.dfs.core.windows.net/clientes_sdc/"

    print("Aplicando SDC Type 2 em clientes...")

    df_clientes = spark.read.format("delta").load(silver_path) \
        .select(
            "id_cliente",
            "hash_cliente",
            "sobrenome_masked",
            "perfil_risco",
            "score_credito",
            "faixa_saldo",
            "faixa_etaria",
            "score_categoria",
            "ativo",
            "churn"
        )

    aplicar_sdc_type2(spark, df_clientes, sdc_path, "hash_cliente")

    total = spark.read.format("delta").load(sdc_path) \
        .filter(F.col("atual") == True).count()

    print(f"SDC clientes — {total} registros ativos")
    return total


def aplicar_sdc_score_risco(spark: SparkSession,
                             storage_account: str) -> int:
    """
    Aplica SDC Type 2 na tabela de score de risco.
    Rastreia evolucao do score e limite operacional.

    Returns: total de registros ativos
    """
    gold_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/score_risco_clientes/"
    sdc_path  = f"abfss://gold@{storage_account}.dfs.core.windows.net/score_risco_sdc/"

    print("Aplicando SDC Type 2 em score de risco...")

    df_score = spark.read.format("delta").load(gold_path) \
        .select(
            "hash_cliente",
            "perfil_risco",
            "faixa_saldo",
            "score_credito",
            "score_risco",
            "categoria_risco",
            "limite_operacional",
            "num_ativos",
            "total_ordens"
        )

    aplicar_sdc_type2(spark, df_score, sdc_path, "hash_cliente")

    total = spark.read.format("delta").load(sdc_path) \
        .filter(F.col("atual") == True).count()

    print(f"SDC score risco — {total} registros ativos")
    return total
