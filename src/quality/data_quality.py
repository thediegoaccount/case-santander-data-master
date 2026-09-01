"""
Data Quality Framework

Validações automatizadas de qualidade de dados em cada camada.
"""

from pyspark.sql import functions as F
from typing import List, Dict, Any
from src.config.logging import info, error, warning


class DataQualityError(Exception):
    """Erro de qualidade de dados"""

    pass


class SchemaDriftError(Exception):
    """Erro de drift de schema"""

    pass


class DataQualityValidator:
    """Validador de qualidade de dados"""

    def __init__(self, job_name: str):
        self.job_name = job_name

    def validate_completeness(self, df, required_columns: List[str]):
        """
        Valida se todas as colunas obrigatórias existem

        Args:
            df: DataFrame Spark
            required_columns: Lista de colunas obrigatórias

        Raises:
            DataQualityError: Se colunas faltando
        """
        actual_columns = df.columns
        missing = [col for col in required_columns if col not in actual_columns]

        if missing:
            error(self.job_name, f"Colunas faltando: {missing}")
            raise DataQualityError(f"Colunas faltando: {missing}")

        info(self.job_name, f"Completidade OK: {len(required_columns)} colunas validadas")

    def validate_uniqueness(self, df, key_column: str):
        """
        Valida se a chave é única

        Args:
            df: DataFrame Spark
            key_column: Coluna chave

        Raises:
            DataQualityError: Se houver duplicados
        """
        duplicates = df.groupBy(key_column).count().filter(F.col("count") > 1)
        duplicate_count = duplicates.count()

        if duplicate_count > 0:
            error(self.job_name, f"Duplicados encontrados em {key_column}: {duplicate_count}")
            raise DataQualityError(f"{key_column} tem {duplicate_count} duplicados")

        info(self.job_name, f"Unicidade OK: {key_column} é único")

    def validate_nulls(self, df, max_null_percentage: float = 0.05):
        """
        Valida porcentagem de nulos

        Args:
            df: DataFrame Spark
            max_null_percentage: Porcentagem máxima de nulos (padrão: 5%)

        Raises:
            DataQualityError: Se nulos excederem limite
        """
        total = df.count()

        if total == 0:
            warning(self.job_name, "DataFrame vazio - skip validação de nulos")
            return

        for col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            null_pct = null_count / total

            if null_pct > max_null_percentage:
                error(self.job_name, f"{col}: {null_pct:.2%} nulos (max: {max_null_percentage:.0%})")
                raise DataQualityError(f"{col}: {null_pct:.2%} nulos (max: {max_null_percentage:.0%})")

        info(self.job_name, f"Nulos OK: Todas as colunas abaixo de {max_null_percentage:.0%}")

    def validate_schema_drift(self, df, expected_schema: Dict[str, str]):
        """
        Detecta mudanças de schema

        Args:
            df: DataFrame Spark
            expected_schema: Schema esperado {coluna: tipo}

        Raises:
            SchemaDriftError: Se schema mudou
        """
        current_columns = set(df.columns)
        expected_columns = set(expected_schema.keys())

        new_cols = current_columns - expected_columns
        missing_cols = expected_columns - current_columns

        if new_cols or missing_cols:
            error(self.job_name, f"Schema drift: +{new_cols} -{missing_cols}")
            raise SchemaDriftError(f"Schema drift: +{new_cols} -{missing_cols}")

        # Verificar tipos
        actual_schema = {field.name: field.dataType.typeName() for field in df.schema.fields}
        for col, expected_type in expected_schema.items():
            if actual_schema[col] != expected_type:
                error(self.job_name, f"Tipo incorreto para {col}: esperado {expected_type}, atual {actual_schema[col]}")
                raise SchemaDriftError(f"Tipo incorreto para {col}")

        info(self.job_name, "Schema OK: Sem drift detectado")

    def validate_row_count(self, df, min_rows: int = 1):
        """
        Valida número mínimo de linhas

        Args:
            df: DataFrame Spark
            min_rows: Mínimo de linhas esperado

        Raises:
            DataQualityError: Se linhas abaixo do mínimo
        """
        count = df.count()

        if count < min_rows:
            error(self.job_name, f"Linhas insuficientes: {count} (min: {min_rows})")
            raise DataQualityError(f"Linhas insuficientes: {count} (min: {min_rows})")

        info(self.job_name, f"Count OK: {count} linhas")

    def validate_range(self, df, column: str, min_val: float, max_val: float):
        """
        Valida se valores estão em range

        Args:
            df: DataFrame Spark
            column: Coluna a validar
            min_val: Valor mínimo
            max_val: Valor máximo

        Raises:
            DataQualityError: Se valores fora do range
        """
        out_of_range = df.filter((F.col(column) < min_val) | (F.col(column) > max_val)).count()

        if out_of_range > 0:
            error(self.job_name, f"{column}: {out_of_range} valores fora do range [{min_val}, {max_val}]")
            raise DataQualityError(f"{column}: {out_of_range} valores fora do range")

        info(self.job_name, f"Range OK: {column} dentro de [{min_val}, {max_val}]")

    def validate_consistency(self, df, column: str, allowed_values: List[Any]):
        """
        Valida se valores estão em lista permitida

        Args:
            df: DataFrame Spark
            column: Coluna a validar
            allowed_values: Valores permitidos

        Raises:
            DataQualityError: Se valores não permitidos
        """
        invalid = df.filter(~F.col(column).isin(allowed_values)).count()

        if invalid > 0:
            error(self.job_name, f"{column}: {invalid} valores não permitidos")
            raise DataQualityError(f"{column}: {invalid} valores não permitidos")

        info(self.job_name, f"Consistência OK: {column} tem apenas valores permitidos")

    def run_all_validations(self, df, validations: Dict[str, Any]):
        """
        Executa todas as validações especificadas

        Args:
            df: DataFrame Spark
            validations: Dicionário de validações
                {
                    "completeness": {"required_columns": [...]},
                    "uniqueness": {"key_column": "..."},
                    "nulls": {"max_null_percentage": 0.05},
                    "row_count": {"min_rows": 1}
                }

        Raises:
            DataQualityError: Se alguma validação falhar
        """
        info(self.job_name, "Iniciando validações de qualidade de dados")

        for validation_type, params in validations.items():
            try:
                if validation_type == "completeness":
                    self.validate_completeness(df, **params)
                elif validation_type == "uniqueness":
                    self.validate_uniqueness(df, **params)
                elif validation_type == "nulls":
                    self.validate_nulls(df, **params)
                elif validation_type == "schema_drift":
                    self.validate_schema_drift(df, **params)
                elif validation_type == "row_count":
                    self.validate_row_count(df, **params)
                elif validation_type == "range":
                    self.validate_range(df, **params)
                elif validation_type == "consistency":
                    self.validate_consistency(df, **params)
                else:
                    warning(self.job_name, f"Validação desconhecida: {validation_type}")
            except Exception as e:
                error(self.job_name, f"Validação {validation_type} falhou: {str(e)}")
                raise

        info(self.job_name, "Todas as validações passaram")
