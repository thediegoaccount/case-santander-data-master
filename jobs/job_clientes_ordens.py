"""
Job: Clientes e Ordens
Ingere dados de clientes via Kaggle e gera ordens simuladas.

Ou via Databricks Workflow:
    Task: t6_clientes_ordens
"""

import sys
from src.config.environment import setup_python_path

setup_python_path()
from src.config.logging import info, error, warning

import os
import random
import zipfile
from datetime import datetime, timedelta

import pandas as pd
import requests
from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils
from src.config.secrets import get_secret
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from src.config.settings import configure_adls
from src.security.hashing import hash_with_salt, hash_customer_id, hash_surname
from src.config.tables import SCHEMA_BRONZE, SCHEMA_SILVER


def hash_id(valor):
    """
    Mascara CustomerId usando SHA256 + salt (one-way hash)
    Usa salt do Key Vault para evitar reconstituição
    """
    return hash_customer_id(str(valor))


def mascarar_sobrenome(sobrenome):
    """
    Mascara sobrenome usando SHA256 + salt (one-way hash)
    Não é possível reverter mesmo com acesso ao salt
    """
    return hash_surname(str(sobrenome))


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
    info("job_clientes_ordens", f"=== JOB CLIENTES ORDENS INICIADO: {inicio} ===")

    spark = DatabricksSession.builder.getOrCreate()

    client_id = get_secret("client-id")
    tenant_id = get_secret("tenant-id")
    client_secret = get_secret("client-secret")
    storage_account = get_secret("storage-account")
    kaggle_username = get_secret("kaggle-username")
    kaggle_key = get_secret("kaggle-key")

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
    info("job_clientes_ordens", f" Dataset Kaggle baixado: {csv_path}")

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

    # Gravar clientes no Bronze com CDC (MERGE)
    df_clientes_spark = spark.createDataFrame(df_clientes_final)
    tabela_clientes_bronze = f"{SCHEMA_BRONZE}.clientes"
    
    try:
        # Tentar MERGE para CDC (apenas mudanças)
        delta_table = DeltaTable.forName(spark, tabela_clientes_bronze)
        
        delta_table.alias("target") \
            .merge(
                df_clientes_spark.alias("source"),
                "target.hash_cliente = source.hash_cliente"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        
        info("job_clientes_ordens", "[OK] bronze.clientes atualizado via MERGE (CDC)")
        
    except Exception as e:
        if "is not a Delta table" in str(e) or "Table or view not found" in str(e):
            # Primeira carga
            df_clientes_spark.write.format("delta").mode("overwrite") \
                .saveAsTable(tabela_clientes_bronze)
            info("job_clientes_ordens", "[OK] bronze.clientes primeira carga")
        else:
            raise e

    # Gerar ordens simuladas
    acoes = ["PETR4.SA", "VALE3.SA", "ITUB4.SA", "BBDC4.SA",
             "ABEV3.SA", "MGLU3.SA", "WEGE3.SA", "BBAS3.SA", "SANB11.SA"]

    clientes_amostra = df_clientes_final.sample(1000).to_dict("records")
    ordens = []
    ordem_counter = 0

    for cliente in clientes_amostra:
        num_ordens = random.randint(1, 10)
        for _ in range(num_ordens):
            acao = random.choice(acoes)
            preco = round(random.uniform(10, 100), 2)
            qtd = random.randint(100, 10000)
            data_ord = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 457))
            
            # ID determinístico baseado em hash_cliente + timestamp
            ordem_counter += 1
            id_ordem = f"ORD{cliente['hash_cliente']}-{data_ord.strftime('%Y%m%d')}-{ordem_counter:04d}"

            ordens.append({
                "id_ordem":      id_ordem,
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
    tabela_ordens_bronze = f"{SCHEMA_BRONZE}.ordens"
    
    try:
        # Tentar MERGE para CDC (apenas mudanças)
        delta_table = DeltaTable.forName(spark, tabela_ordens_bronze)
        
        delta_table.alias("target") \
            .merge(
                df_ordens_spark.alias("source"),
                "target.id_ordem = source.id_ordem"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        
        info("job_clientes_ordens", "[OK] bronze.ordens atualizado via MERGE (CDC)")
        
    except Exception as e:
        if "is not a Delta table" in str(e) or "Table or view not found" in str(e):
            # Primeira carga
            df_ordens_spark.write.format("delta").mode("overwrite") \
                .saveAsTable(tabela_ordens_bronze)
            info("job_clientes_ordens", "[OK] bronze.ordens primeira carga")
        else:
            raise e

    # Silver — clientes com CDC (MERGE)
    df_clientes_silver = spark.sql(f"SELECT * FROM {SCHEMA_BRONZE}.clientes") \
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
    
    tabela_clientes_silver = f"{SCHEMA_SILVER}.clientes"
    
    try:
        # Tentar MERGE para CDC (apenas mudanças)
        delta_table = DeltaTable.forName(spark, tabela_clientes_silver)
        
        delta_table.alias("target") \
            .merge(
                df_clientes_silver.alias("source"),
                "target.hash_cliente = source.hash_cliente"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        
        info("job_clientes_ordens", "[OK] silver.clientes atualizado via MERGE (CDC)")
        
    except Exception as e:
        if "is not a Delta table" in str(e) or "Table or view not found" in str(e):
            # Primeira carga
            df_clientes_silver.write.format("delta").mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable(tabela_clientes_silver)
            info("job_clientes_ordens", "[OK] silver.clientes primeira carga")
        else:
            raise e

    # Silver — ordens com CDC (MERGE)
    df_ordens_silver = spark.sql(f"SELECT * FROM {SCHEMA_BRONZE}.ordens") \
        .withColumn("data_ordem", F.to_date("data_ordem")) \
        .withColumn("ano",        F.year("data_ordem")) \
        .withColumn("mes",        F.month("data_ordem")) \
        .withColumn("data_processamento", F.lit(data_hoje))
    
    tabela_ordens_silver = f"{SCHEMA_SILVER}.ordens"
    
    try:
        # Tentar MERGE para CDC (apenas mudanças)
        delta_table = DeltaTable.forName(spark, tabela_ordens_silver)
        
        delta_table.alias("target") \
            .merge(
                df_ordens_silver.alias("source"),
                "target.id_ordem = source.id_ordem"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        
        info("job_clientes_ordens", "[OK] silver.ordens atualizado via MERGE (CDC)")
        
    except Exception as e:
        if "is not a Delta table" in str(e) or "Table or view not found" in str(e):
            # Primeira carga
            df_ordens_silver.write.format("delta").mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable(tabela_ordens_silver)
            info("job_clientes_ordens", "[OK] silver.ordens primeira carga")
        else:
            raise e

    fim = datetime.now()
    info("job_clientes_ordens", "\n=== JOB CLIENTES ORDENS CONCLUIDO ===")
    info("job_clientes_ordens", f"Duracao: {(fim - inicio).total_seconds():.2f}s")
    info("job_clientes_ordens", f"Total clientes: {len(df_clientes_final)}")
    info("job_clientes_ordens", f"Total ordens geradas: {len(ordens)}")
    info("job_clientes_ordens", " CDC implementado: MERGE para clientes e ordens")

if __name__ == "__main__":
    main()
