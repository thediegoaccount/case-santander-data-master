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


def monitorar_tabela(spark: SparkSession, tabela_uc: str) -> dict:
    """
    Monitora qualidade de uma tabela via Unity Catalog.
    Returns: dicionario com metricas de qualidade
    """
    inicio = datetime.now()
    partes = tabela_uc.split(".")
    camada = partes[1]
    nome = partes[2]

    try:
        df = spark.sql(f"SELECT * FROM {tabela_uc}")
        total = df.count()

        if total == 0:
            logger.error(f"[ALERTA CRITICO] {tabela_uc} — Sem registros!")
            return {}

        nulos = sum(df.select([
            F.sum(F.col(c).isNull().cast("int")).alias(c)
            for c in df.columns
        ]).first().asDict().values())

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
        "case_santander.silver.acoes",
        "case_santander.silver.bcb",
        "case_santander.silver.world_bank",
        "case_santander.silver.clientes",
        "case_santander.silver.ordens",
        "case_santander.silver.streaming",
        "case_santander.gold.anomalias",
        "case_santander.gold.posicao_clientes",
        "case_santander.gold.score_risco_clientes",
        "case_santander.gold.deteccao_fraude",
        "case_santander.gold.fraude_streaming",
        "case_santander.gold.anomalias_intraday",
        "case_santander.gold.volume_intraday",
        "case_santander.gold.ranking_acoes_realtime",
    ]

    print(f"Monitorando {len(tabelas)} tabelas...")
    resultados = []

    for tabela in tabelas:
        m = monitorar_tabela(spark, tabela)
        if m:
            resultados.append(m)
            print(f"  ✅ {tabela} — {m['total_registros']} registros — qualidade {m['qualidade_pct']}%")
        else:
            print(f"  ❌ {tabela} — erro ou sem dados")

    return resultados
