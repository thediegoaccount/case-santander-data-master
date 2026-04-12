from datetime import datetime

import pandas as pd
import yfinance as yf

from src.config.settings import ACOES


def extrair_acoes(spark, storage_account: str, acoes: list = ACOES) -> int:
    """
    Extrai dados historicos de acoes da B3 via Yahoo Finance
    e grava na camada Bronze do ADLS.

    Returns: total de registros gravados
    """
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    print(f"Extraindo {len(acoes)} acoes B3...")

    df_total = pd.DataFrame()

    for acao in acoes:
        try:
            ticker = yf.Ticker(acao)
            df = ticker.history(period="2y")
            df["ticker"] = acao
            df["data_extracao"] = data_hoje
            df_total = pd.concat([df_total, df])
            print(f"  OK: {acao} — {len(df)} registros")
        except Exception as e:
            print(f"  ERRO: {acao} — {e}")

    df_total = df_total.reset_index()
    df_total.columns = [c.lower().replace(" ", "_") for c in df_total.columns]

    bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/acoes/data={data_hoje}/"
    df_spark = spark.createDataFrame(df_total)
    df_spark.write.mode("overwrite").parquet(bronze_path)

    total = df_spark.count()
    print(f"Bronze acoes gravado: {total} registros")
    return total
