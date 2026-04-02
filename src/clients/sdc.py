"""
Slowly Changing Dimensions (SDC) Type 2
Mantém histórico de mudanças em dimensões de clientes
"""
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from datetime import datetime

try:
    from delta.tables import DeltaTable
except ImportError:
    DeltaTable = None


def aplicar_sdc_type2(spark: SparkSession, df_novos, 
                       path: str, chave: str) -> None:
    """
    Aplica SDC Type 2 em uma tabela Delta Lake.
    
    - Fecha registros antigos (data_fim = hoje, atual = false)
    - Insere novos registros (data_fim = 9999-12-31, atual = true)
    
    Args:
        spark:    SparkSession
        df_novos: DataFrame com novos valores
        path:     Caminho da tabela Delta
        chave:    Coluna chave para identificar o registro
    """
    data_hoje = datetime.now().strftime("%Y-%m-%d")

    df_com_sdc = df_novos \
        .withColumn("data_inicio", F.lit(data_hoje)) \
        .withColumn("data_fim",    F.lit("9999-12-31")) \
        .withColumn("atual",       F.lit(True))

    if DeltaTable and DeltaTable.isDeltaTable(spark, path):
        delta_table = DeltaTable.forPath(spark, path)

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
            .save(path)

        print(f"SDC Type 2 atualizado: {path}")
    else:
        # Primeira carga
        df_com_sdc.write \
            .format("delta") \
            .mode("overwrite") \
            .save(path)

        print(f"SDC Type 2 criado: {path}")


def aplicar_sdc_clientes(spark: SparkSession, 
                          storage_account: str) -> int:
    """
    Aplica SDC Type 2 na tabela de clientes.
    Rastreia historico de perfil de risco.

    Returns: total de registros ativos
    """
    silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/clientes/"
    sdc_path    = f"abfss://gold@{storage_account}.dfs.core.windows.net/sdc_clientes/"

    print("Aplicando SDC Type 2 em clientes...")

    df_clientes = spark.read.format("delta").load(silver_path) \
        .select(
            "hash_cliente",
            "sobrenome_masked",
            "perfil_risco",
            "faixa_saldo",
            "score_credito",
            "faixa_etaria",
            "score_categoria",
            "ativo",
            "churn"
        )

    aplicar_sdc_type2(spark, df_clientes, sdc_path, "hash_cliente")

    # Registrando no Unity Catalog
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS case_santander.gold.sdc_clientes
        USING DELTA
        LOCATION '{sdc_path}'
        COMMENT 'SDC Type 2 — historico de perfil de clientes'
    """)

    total = spark.read.format("delta").load(sdc_path) \
        .filter(F.col("atual") == True).count()

    print(f"SDC clientes — registros ativos: {total}")
    return total


def aplicar_sdc_score_risco(spark: SparkSession,
                             storage_account: str) -> int:
    """
    Aplica SDC Type 2 no score de risco dos clientes.
    Rastreia historico de score e limite operacional.

    Returns: total de registros ativos
    """
    sdc_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/sdc_score_risco/"

    print("Aplicando SDC Type 2 em score de risco...")

    df_score = spark.sql("""
        SELECT 
            hash_cliente,
            perfil_risco,
            score_risco,
            categoria_risco,
            limite_operacional,
            num_ativos,
            total_ordens,
            taxa_cancelamento_pct
        FROM case_santander.gold.score_risco_clientes
    """)

    aplicar_sdc_type2(spark, df_score, sdc_path, "hash_cliente")

    # Registrando no Unity Catalog
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS case_santander.gold.sdc_score_risco
        USING DELTA
        LOCATION '{sdc_path}'
        COMMENT 'SDC Type 2 — historico de score de risco'
    """)

    total = spark.read.format("delta").load(sdc_path) \
        .filter(F.col("atual") == True).count()

    print(f"SDC score risco — registros ativos: {total}")
    return total


def consultar_historico(spark: SparkSession, 
                         hash_cliente: str) -> None:
    """
    Consulta historico completo de um cliente via SDC Type 2
    """
    print(f"\nHistorico SDC — Cliente: {hash_cliente}")
    print("\nPerfil de risco:")
    spark.sql(f"""
        SELECT hash_cliente, perfil_risco, score_credito,
               data_inicio, data_fim, atual
        FROM case_santander.gold.sdc_clientes
        WHERE hash_cliente = '{hash_cliente}'
        ORDER BY data_inicio
    """).show(truncate=False)

    print("\nScore de risco:")
    spark.sql(f"""
        SELECT hash_cliente, score_risco, categoria_risco,
               limite_operacional, data_inicio, data_fim, atual
        FROM case_santander.gold.sdc_score_risco
        WHERE hash_cliente = '{hash_cliente}'
        ORDER BY data_inicio
    """).show(truncate=False)
