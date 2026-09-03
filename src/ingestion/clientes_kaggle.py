"""
Ingestão de clientes a partir do dataset público de churn bancário (Kaggle).

Escreve UMA tabela: <catalog>.<env>_bronze.clientes
"""

import os
import zipfile
from datetime import datetime

import pandas as pd
import requests

from src.config.logging import info
from src.config.secrets import get_secret
from src.config.tables import SCHEMA_BRONZE
from src.security.hashing import hash_customer_id, hash_surname
from src.utils.delta import merge_ou_cria

CTX = "clientes_kaggle"
URL_DATASET = "https://www.kaggle.com/api/v1/datasets/download/mathchi/churn-for-bank-customers"


def classificar_perfil(score) -> str:
    """Perfil de investidor a partir do score de crédito."""
    if score >= 750:
        return "Arrojado"
    elif score >= 600:
        return "Moderado"
    return "Conservador"


def classificar_saldo(balance) -> str:
    """Faixa de saldo em conta."""
    if balance == 0:
        return "Sem saldo"
    elif balance < 50000:
        return "Baixo"
    elif balance < 150000:
        return "Medio"
    return "Alto"


def _baixar_csv() -> str:
    """Baixa e extrai o dataset. Retorna o caminho do CSV."""
    pid = os.getpid()
    work_dir = f"/tmp/kaggle_{pid}"
    zip_path = f"{work_dir}/churn.zip"
    os.makedirs(work_dir, exist_ok=True)

    resposta = requests.get(
        URL_DATASET,
        auth=(get_secret("kaggle-username"), get_secret("kaggle-key")),
        stream=True,
    )
    resposta.raise_for_status()
    with open(zip_path, "wb") as f:
        for chunk in resposta.iter_content(chunk_size=8192):
            f.write(chunk)

    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(work_dir)

    csv_path = f"{work_dir}/churn.csv"
    info(CTX, f"Dataset Kaggle baixado: {csv_path}")
    return csv_path


def extrair_clientes(spark) -> int:
    """
    Baixa o dataset, aplica anonimização LGPD e grava bronze.clientes.

    Retorna a quantidade de clientes gravados.
    """
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    df_raw = pd.read_csv(_baixar_csv())

    df = df_raw.copy()
    # fmt: off
    # CustomerId e Surname sao PII: viram hash SHA256 com salt do Key Vault,
    # one-way, nao reversivel nem com acesso ao salt.
    df["id_cliente"] = df["CustomerId"].apply(lambda x: f"CLI{x}")
    df["hash_cliente"] = df["CustomerId"].apply(lambda v: hash_customer_id(str(v)))
    df["sobrenome_masked"] = df["Surname"].apply(lambda v: hash_surname(str(v)))
    df["perfil_risco"] = df["CreditScore"].apply(classificar_perfil)
    df["faixa_saldo"] = df["Balance"].apply(classificar_saldo)
    df["ativo"] = df["IsActiveMember"].apply(lambda x: x == 1)
    df["churn"] = df["Exited"].apply(lambda x: x == 1)
    df["data_extracao"] = data_hoje

    df_final = df[[
        "id_cliente", "hash_cliente", "sobrenome_masked",
        "CreditScore", "Geography", "Gender", "Age", "Tenure",
        "Balance", "faixa_saldo", "NumOfProducts", "perfil_risco",
        "ativo", "churn", "EstimatedSalary", "data_extracao"
    ]].rename(columns={
        "CreditScore": "score_credito", "Geography": "pais",
        "Gender": "genero", "Age": "idade", "Tenure": "anos_cliente",
        "Balance": "saldo", "NumOfProducts": "num_produtos",
        "EstimatedSalary": "salario_estimado"
    })
    # fmt: on

    merge_ou_cria(spark, spark.createDataFrame(df_final),
                  f"{SCHEMA_BRONZE}.clientes", "hash_cliente", CTX)

    total = len(df_final)
    info(CTX, f"Bronze clientes gravado: {total} registros")
    return total
