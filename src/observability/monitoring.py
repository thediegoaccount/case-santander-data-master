"""
Monitoramento de qualidade dos dados por camada
"""

import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.config.tables import SCHEMA_GOLD, SCHEMA_SILVER

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
logger = logging.getLogger("case-santander")


def monitorar_tabela(spark: SparkSession, tabela_uc: str) -> dict:
    """
    Monitora qualidade de uma tabela via Unity Catalog.
    Returns: dicionario com metricas de qualidade
    """
    inicio = datetime.now()
    partes = tabela_uc.split(".")
    # partes[1] e "hk_silver"/"prod_silver"; o rotulo da camada e so a
    # segunda metade, para as metricas nao mudarem de nome entre ambientes.
    camada = partes[1].split("_", 1)[-1]
    nome = partes[2]

    try:
        df = spark.sql(f"SELECT * FROM {tabela_uc}")
        total = df.count()

        if total == 0:
            logger.error(f"[ALERTA CRITICO] {tabela_uc} — Sem registros!")
            return {}

        nulos = sum(
            df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).first().asDict().values()
        )

        # Versao Delta da tabela. Os jobs de streaming leem
        # `SELECT MAX(versao_cdf) FROM ...observabilidade WHERE tabela=...`
        # como marca d'agua do CDC, mas esta coluna nunca era gravada:
        # UNRESOLVED_COLUMN era engolido por `except` e cada execucao (a cada
        # 5 min) caia em full scan. O CDC anunciado nunca funcionou.
        try:
            versao_cdf = spark.sql(f"DESCRIBE HISTORY {tabela_uc} LIMIT 1").first()["version"]
        except Exception:
            versao_cdf = 0

        duplicatas = total - df.dropDuplicates().count()
        qualidade = round((1 - nulos / (total * len(df.columns))) * 100, 2)

        # fmt: off
        metricas = {
            "camada":           camada,
            "tabela":           nome,
            "data_verificacao": datetime.now().strftime("%Y-%m-%d"),
            "total_registros":  total,
            "total_nulos":      nulos,
            "total_duplicatas": duplicatas,
            "versao_cdf":       versao_cdf,
            "qualidade_pct":    qualidade,
            "tempo_seg":        round((datetime.now() - inicio).total_seconds(), 2)
        }
        # fmt: on

        logger.info(
            f"[{camada}][{nome}] Registros: {total} | "
            f"Nulos: {nulos} | Duplicatas: {duplicatas} | "
            f"Qualidade: {qualidade}%"
        )

        if qualidade < 95:
            logger.error(f"[ALERTA CRITICO] {tabela_uc} — Qualidade: {qualidade}%")
        if duplicatas > 0:
            logger.warning(f"[ALERTA] {tabela_uc} — Duplicatas: {duplicatas}")

        return metricas

    except Exception as e:
        logger.error(f"[ERRO] {tabela_uc}: {e}")
        return {}


def executar_monitoramento(spark: SparkSession, storage_account: str = None) -> list:
    """
    Executa monitoramento em todas as camadas via Unity Catalog.
    Returns: lista de metricas por tabela
    """
    tabelas = [
        f"{SCHEMA_SILVER}.acoes",
        f"{SCHEMA_SILVER}.bcb",
        f"{SCHEMA_SILVER}.world_bank",
        f"{SCHEMA_SILVER}.clientes",
        f"{SCHEMA_SILVER}.ordens",
        f"{SCHEMA_SILVER}.streaming",
        f"{SCHEMA_GOLD}.anomalias",
        f"{SCHEMA_GOLD}.posicao_clientes",
        f"{SCHEMA_GOLD}.score_risco_clientes",
        f"{SCHEMA_GOLD}.deteccao_fraude",
        f"{SCHEMA_GOLD}.fraude_streaming",
        f"{SCHEMA_GOLD}.anomalias_intraday",
        f"{SCHEMA_GOLD}.volume_intraday",
        f"{SCHEMA_GOLD}.ranking_acoes_realtime",
    ]

    print(f"Monitorando {len(tabelas)} tabelas...")
    resultados = []

    for tabela in tabelas:
        m = monitorar_tabela(spark, tabela)
        if m:
            resultados.append(m)
            print(f"   {tabela} — {m['total_registros']} registros — qualidade {m['qualidade_pct']}%")
        else:
            print(f"   {tabela} — erro ou sem dados")

    return resultados
