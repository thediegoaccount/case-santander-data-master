import requests
import pandas as pd
from datetime import datetime


def extrair_bcb(spark, storage_account: str) -> int:
    """
    Extrai indicadores economicos do Banco Central (BCB)
    e grava na camada Bronze do ADLS.

    Returns: total de registros gravados
    """
    data_hoje    = datetime.now().strftime("%Y-%m-%d")
    data_inicial = "01/04/2021"
    data_final   = "01/04/2026"

    def buscar_diario(codigo, nome):
        url = (f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
               f"?formato=json&dataInicial={data_inicial}&dataFinal={data_final}")
        try:
            response = requests.get(url, timeout=10)
            df = pd.DataFrame(response.json())
            df["indicador"]     = nome
            df["data_extracao"] = data_hoje
            df.columns          = [c.lower() for c in df.columns]
            df["valor"]         = pd.to_numeric(df["valor"], errors="coerce")
            print(f"  OK: {nome} — {len(df)} registros")
            return df
        except Exception as e:
            print(f"  ERRO: {nome} — {e}")
            return pd.DataFrame()

    def buscar_mensal(codigo, nome):
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json"
        try:
            response = requests.get(url, timeout=10)
            df = pd.DataFrame(response.json())
            df["indicador"]     = nome
            df["data_extracao"] = data_hoje
            df.columns          = [c.lower() for c in df.columns]
            df["valor"]         = pd.to_numeric(df["valor"], errors="coerce")
            print(f"  OK: {nome} — {len(df)} registros")
            return df
        except Exception as e:
            print(f"  ERRO: {nome} — {e}")
            return pd.DataFrame()

    print("Extraindo BCB...")
    df_selic  = buscar_diario(11,  "selic")
    df_cambio = buscar_diario(1,   "cambio_usd_brl")
    df_ipca   = buscar_mensal(433, "ipca")
    df_bcb    = pd.concat([df_selic, df_cambio, df_ipca], ignore_index=True)

    bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/bcb/data={data_hoje}/"
    df_spark = spark.createDataFrame(df_bcb)
    df_spark.write.mode("overwrite").parquet(bronze_path)

    total = df_spark.count()
    print(f"Bronze BCB gravado: {total} registros")
    return total
