"""
Job: Clientes e Ordens
Ingere dados de clientes via Kaggle e gera ordens simuladas.

Ou via Databricks Workflow:
    Task: t6_clientes_ordens
"""

import sys

sys.path.insert(0, "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")

import hashlib
import os
import random
import zipfile
from datetime import datetime, timedelta

import pandas as pd
import requests
from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils
from pyspark.sql import functions as F

from src.config.settings import configure_adls


def hash_id(valor):
    return hashlib.sha256(str(valor).encode()).hexdigest()[:16]


def mascarar_sobrenome(sobrenome):
    return sobrenome[0] + "*" * (len(sobrenome) - 1)


def classificar_perfil(score):
    if score >= 750:
        return "Arrojado"
    elif score >= 600:
        return "Moderado"
    else:
        return "Conservador"


def classificar_saldo(balance):
    if balance == 0:
        return "Sem saldo"
    elif balance < 50000:
        return "Baixo"
    elif balance < 150000:
        return "Medio"
    else:
        return "Alto"


def main():
    inicio = datetime.now()
    print(f"=== JOB CLIENTES ORDENS INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id = dbutils.secrets.get(scope="kv-case-santander", key="client-id")
    tenant_id = dbutils.secrets.get(scope="kv-case-santander", key="tenant-id")
    client_secret = dbutils.secrets.get(scope="kv-case-santander", key="client-secret")
    storage_account = dbutils.secrets.get(scope="kv-case-santander", key="storage-account")
    kaggle_username = dbutils.secrets.get(scope="kv-case-santander", key="kaggle-username")
    kaggle_key = dbutils.secrets.get(scope="kv-case-santander", key="kaggle-key")

    configure_adls(spark, storage_account, client_id, tenant_id, client_secret)

    data_hoje = datetime.now().strftime("%Y-%m-%d")

    # Download Kaggle
    pid = os.getpid()
    work_dir = f"/tmp/kaggle_{pid}"
    zip_path = f"{work_dir}/churn.zip"
    os.makedirs(work_dir, exist_ok=True)

    url = "https://www.kaggle.com/api/v1/datasets/download/mathchi/churn-for-bank-customers"
    response = requests.get(url, auth=(kaggle_username, kaggle_key), stream=True)
    with open(zip_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(work_dir)

    csv_path = f"{work_dir}/churn.csv"
    print(f"✅ Dataset Kaggle baixado: {csv_path}")

    # Processar clientes com LGPD
    df_raw = pd.read_csv(csv_path)

    df_clientes = df_raw.copy()
    # fmt: off
    df_clientes["id_cliente"]       = df_clientes["CustomerId"].apply(lambda x: f"CLI{x}")
    df_clientes["hash_cliente"]     = df_clientes["CustomerId"].apply(hash_id)
    df_clientes["sobrenome_masked"] = df_clientes["Surname"].apply(mascarar_sobrenome)
    df_clientes["perfil_risco"]     = df_clientes["CreditScore"].apply(classificar_perfil)
    df_clientes["faixa_saldo"]      = df_clientes["Balance"].apply(classificar_saldo)
    df_clientes["ativo"]            = df_clientes["IsActiveMember"].apply(lambda x: x == 1)
    df_clientes["churn"]            = df_clientes["Exited"].apply(lambda x: x == 1)
    df_clientes["data_extracao"]    = data_hoje
    # fmt: on

    # fmt: off
    df_clientes_final = df_clientes[[
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

    # Gravar clientes no Bronze
    df_clientes_spark = spark.createDataFrame(df_clientes_final)
    # fmt: off
    df_clientes_spark.write.format("delta").mode("overwrite") \
        .saveAsTable("case_santander.bronze.clientes")
    print(f"✅ bronze.clientes → {df_clientes_spark.count()} registros")

    # Gerar ordens simuladas
    acoes = ["PETR4.SA", "VALE3.SA", "ITUB4.SA", "BBDC4.SA",
             "ABEV3.SA", "MGLU3.SA", "WEGE3.SA", "BBAS3.SA", "SANB11.SA"]

    clientes_amostra = df_clientes_final.sample(1000).to_dict("records")
    ordens = []

    for cliente in clientes_amostra:
        num_ordens = random.randint(1, 10)
        for _ in range(num_ordens):
            acao = random.choice(acoes)
            preco = round(random.uniform(10, 100), 2)
            qtd = random.randint(100, 10000)
            data_ord = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 457))

            ordens.append({
                "id_ordem":      f"ORD{random.randint(1000000, 9999999)}",
                "hash_cliente":  cliente["hash_cliente"],
                "perfil_risco":  cliente["perfil_risco"],
                "faixa_saldo":   cliente["faixa_saldo"],
                "ticker":        acao,
                "preco":         preco,
                "quantidade":    qtd,
                "valor_total":   round(preco * qtd, 2),
                "tipo":          random.choice(["compra", "venda"]),
                "corretora":     "Santander Corretora",
                "status":        random.choice(["executada", "cancelada", "pendente"]),
                "data_ordem":    data_ord.strftime("%Y-%m-%d"),
                "data_extracao": data_hoje
            })

    df_ordens_spark = spark.createDataFrame(pd.DataFrame(ordens))
    df_ordens_spark.write.format("delta").mode("overwrite") \
        .saveAsTable("case_santander.bronze.ordens")
    print(f"✅ bronze.ordens → {df_ordens_spark.count()} registros")

    # Silver — clientes
    df_clientes_silver = spark.sql("SELECT * FROM case_santander.bronze.clientes") \
        .withColumn("faixa_etaria",
            F.when(F.col("idade") < 30, "Jovem")
            .when(F.col("idade") < 50, "Adulto")
            .otherwise("Senior")) \
        .withColumn("score_categoria",
            F.when(F.col("score_credito") >= 750, "Excelente")
            .when(F.col("score_credito") >= 650, "Bom")
            .when(F.col("score_credito") >= 550, "Regular")
            .otherwise("Ruim")) \
        .withColumn("data_processamento", F.lit(data_hoje))

    df_clientes_silver.write.format("delta").mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("case_santander.silver.clientes")
    print(f"✅ silver.clientes → {df_clientes_silver.count()} registros")

    # Silver — ordens
    df_ordens_silver = spark.sql("SELECT * FROM case_santander.bronze.ordens") \
        .withColumn("data_ordem", F.to_date("data_ordem")) \
        .withColumn("ano",        F.year("data_ordem")) \
        .withColumn("mes",        F.month("data_ordem")) \
        .withColumn("data_processamento", F.lit(data_hoje))

    df_ordens_silver.write.format("delta").mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("case_santander.silver.ordens")
    # fmt: on
    print(f"✅ silver.ordens → {df_ordens_silver.count()} registros")

    fim = datetime.now()
    print("\n=== JOB CLIENTES ORDENS CONCLUIDO ===")
    print(f"Duracao: {(fim - inicio).total_seconds():.2f}s")


main()
