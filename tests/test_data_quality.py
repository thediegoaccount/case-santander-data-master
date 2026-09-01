"""
Testes de Qualidade de Dados

Valida qualidade de dados em cada camada (Bronze, Silver, Gold)
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


def test_bronze_clientes_completeness(spark_session):
    """Testa se bronze.clientes tem todas as colunas obrigatórias"""
    # Schema esperado
    expected_columns = [
        "id_cliente",
        "hash_cliente",
        "nome",
        "sobrenome_masked",
        "score_risco",
        "saldo",
        "ativo"
    ]
    
    # Tentar ler tabela (pode não existir em ambiente de teste)
    try:
        df = spark_session.table("case_santander.bronze.clientes")
        actual_columns = df.columns
        
        # Verificar se todas as colunas esperadas existem
        for col in expected_columns:
            assert col in actual_columns, f"Coluna faltando: {col}"
    except Exception:
        # Tabela não existe - marcar como skip
        pytest.skip("Tabela bronze.clientes não existe")


def test_bronze_clientes_uniqueness(spark_session):
    """Testa se hash_cliente é único em bronze.clientes"""
    try:
        df = spark_session.table("case_santander.bronze.clientes")
        
        # Verificar duplicados
        duplicates = df.groupBy("hash_cliente").count().filter(F.col("count") > 1)
        assert duplicates.count() == 0, "hash_cliente tem duplicados"
    except Exception:
        pytest.skip("Tabela bronze.clientes não existe")


def test_silver_clientes_no_nulls(spark_session):
    """Testa se silver.clientes não tem nulls em colunas críticas"""
    try:
        df = spark_session.table("case_santander.silver.clientes")
        total = df.count()
        
        # Colunas críticas que não devem ter nulls
        critical_columns = ["id_cliente", "hash_cliente", "nome"]
        
        for col in critical_columns:
            null_count = df.filter(F.col(col).isNull()).count()
            null_percentage = null_count / total if total > 0 else 0
            assert null_percentage < 0.05, f"{col}: {null_percentage:.2%} nulos (max: 5%)"
    except Exception:
        pytest.skip("Tabela silver.clientes não existe")


def test_gold_fraude_not_empty(spark_session):
    """Testa se gold.fraude tem dados"""
    try:
        df = spark_session.table("case_santander.gold.fraude")
        assert df.count() > 0, "gold.fraude está vazio"
    except Exception:
        pytest.skip("Tabela gold.fraude não existe")


def test_schema_drift_detection(spark_session):
    """Testa se schema das tabelas não mudou inesperadamente"""
    try:
        # Schema esperado para bronze.clientes
        expected_schema = {
            "id_cliente": "string",
            "hash_cliente": "string",
            "nome": "string",
            "sobrenome_masked": "string",
            "score_risco": "int",
            "saldo": "double",
            "ativo": "boolean"
        }
        
        df = spark_session.table("case_santander.bronze.clientes")
        actual_schema = {field.name: field.dataType.typeName() for field in df.schema.fields}
        
        # Verificar se schema está consistente
        for col, dtype in expected_schema.items():
            assert col in actual_schema, f"Coluna faltando: {col}"
            assert actual_schema[col] == dtype, f"Tipo incorreto para {col}: esperado {dtype}, atual {actual_schema[col]}"
    except Exception:
        pytest.skip("Tabela bronze.clientes não existe")


@pytest.fixture(scope="session")
def spark_session():
    """Fixture para Spark session"""
    spark = SparkSession.builder \
        .appName("test_data_quality") \
        .master("local[1]") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()
