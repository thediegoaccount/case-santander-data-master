"""
Monitoramento de qualidade dos dados por camada
"""
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger("case-santander")


def monitorar_camada(spark: SparkSession, nome: str, 
                     camada: str, storage_account: str) -> dict:
    """
    Monitora qualidade de uma tabela Delta Lake.
    Returns: dicionario com metricas de qualidade
    """
    inicio = datetime.now()
    path   = f"abfss://{camada}@{storage_account}.dfs.core.windows.net/{nome}/"

    try:
        df    = spark.read.format("delta").load(path)
        total = df.count()
        nulos = sum(df.select([
            F.sum(F.col(c).isNull().cast("int")).alias(c) 
            for c in df.columns
        ]).first().asDict().values())
        duplicatas = total - df.dropDuplicates().count()
        qualidade  = round((1 - nulos / (total * len(df.columns))) * 100, 2)

        metricas = {
            "camada":           camada,
            "tabela":           nome,
            "data_verificacao": datetime.now().strftime("%Y-%m-%d"),
            "total_registros":  total,
            "total_nulos":      nulos,
            "total_duplicatas": duplicatas,
            "qualidade_pct":    qualidade,
            "tempo_seg":        round((datetime.now() - inicio).total_seconds(), 2)
        }

        logger.info(
            f"[{camada}][{nome}] Registros: {total} | "
            f"Nulos: {nulos} | Duplicatas: {duplicatas} | "
            f"Qualidade: {qualidade}%"
        )

        if qualidade < 95:
            logger.error(f"[ALERTA CRITICO] {camada}/{nome} — Qualidade: {qualidade}%")
        if duplicatas > 0:
            logger.warning(f"[ALERTA] {camada}/{nome} — Duplicatas: {duplicatas}")
        if total == 0:
            logger.error(f"[ALERTA CRITICO] {camada}/{nome} — Sem registros!")

        return metricas

    except Exception as e:
        logger.error(f"[ERRO] {camada}/{nome}: {e}")
        return {}


def executar_monitoramento(spark: SparkSession, storage_account: str) -> list:
    """
    Executa monitoramento em todas as camadas Silver e Gold.
    Returns: lista de metricas por tabela
    """
    tabelas = [
        ("acoes",      "silver"),
        ("bcb",        "silver"),
        ("world_bank", "silver"),
        ("clientes",   "silver"),
        ("ordens",     "silver"),
    ]

    resultados = []
    for tabela, camada in tabelas:
        m = monitorar_camada(spark, tabela, camada, storage_account)
        if m:
            resultados.append(m)

    return resultados
