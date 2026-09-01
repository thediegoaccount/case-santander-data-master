from datetime import datetime
import time

import pandas as pd
import yfinance as yf

from src.config.settings import ACOES
from src.config.environment import get_config, get_env, is_production
from src.ingestion.api_wrapper import rate_limiter
from src.utils.retry import retry_on_connection_error


@retry_on_connection_error(max_attempts=3)
def extrair_acoes(spark, storage_account: str = None, acoes: list = ACOES) -> int:
    """
    Extrai dados historicos de acoes da B3 via Yahoo Finance
    e grava na camada Bronze do ADLS com isolamento por ambiente.

    Args:
        spark: Sessão Spark
        storage_account: Nome do storage account (opcional, usa config do ambiente)
        acoes: Lista de ações a extrair

    Returns: total de registros gravados
    """
    env = get_env()
    config = get_config()

    # Usar storage account do ambiente se não fornecido
    if storage_account is None:
        storage_account = config["storage_account"]

    data_hoje = datetime.now().strftime("%Y-%m-%d")

    print(f"[{env.upper()}] Extraindo {len(acoes)} acoes B3...")
    print(f"[{env.upper()}] Storage Account: {storage_account}")
    print(f"[{env.upper()}] Catalog: {config['catalog']}")

    if is_production():
        print(f"[{env.upper()}] *** PRODUÇÃO *** - Dados reais serão gravados")

    df_total = pd.DataFrame()

    for i, acao in enumerate(acoes):
        # Rate limiting por ambiente
        rate_limiter.wait_if_needed("yahoo_finance")

        try:
            ticker = yf.Ticker(acao)
            df = ticker.history(period="2y")
            df["ticker"] = acao
            df["data_extracao"] = data_hoje
            df["ambiente"] = env  # Tag de ambiente para rastreabilidade
            df_total = pd.concat([df_total, df])
            print(f"[{env.upper()}] OK: {acao} — {len(df)} registros ({i+1}/{len(acoes)})")

            # Pequena pausa entre requests para respeitar rate limit
            time.sleep(0.5)

        except Exception as e:
            print(f"[{env.upper()}] ERRO: {acao} — {e}")

    if df_total.empty:
        print(f"[{env.upper()}] Nenhum dado extraído")
        return 0

    df_total = df_total.reset_index()
    df_total.columns = [c.lower().replace(" ", "_") for c in df_total.columns]

    # Caminho isolado por ambiente
    bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/acoes/data={data_hoje}/"
    df_spark = spark.createDataFrame(df_total)
    df_spark.write.mode("overwrite").parquet(bronze_path)

    total = df_spark.count()
    print(f"[{env.upper()}] Bronze acoes gravado: {total} registros")
    return total
