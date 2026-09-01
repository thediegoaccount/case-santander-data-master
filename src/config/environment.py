"""
Configuração por ambiente - Isolamento entre HK (homologação reduzido) e PROD
"""

import os
import sys
from typing import Dict, Any


class EnvironmentConfig:
    """Configuração isolada por ambiente"""

    ENVIRONMENTS = ["hk", "prod"]

    @staticmethod
    def get_repo_path() -> str:
        """
        Retorna caminho do repositório dinamicamente

        Priority:
        1. Variável de ambiente REPO_PATH
        2. Databricks Workspace path padrão
        3. Diretório atual (local)
        """
        # 1. Variável de ambiente
        repo_path = os.getenv("REPO_PATH")
        if repo_path:
            return repo_path

        # 2. Databricks Workspace path padrão
        if os.getenv("DATABRICKS_RUNTIME_VERSION"):
            return "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master"

        # 3. Diretório atual (local)
        return os.getcwd()

    @staticmethod
    def setup_python_path():
        """
        Configura sys.path para importar módulos do repositório
        Deve ser chamado no início de cada job
        """
        repo_path = EnvironmentConfig.get_repo_path()

        # Adicionar ao sys.path se não estiver
        if repo_path not in sys.path:
            sys.path.insert(0, repo_path)

        return repo_path

    @staticmethod
    def get_current_env() -> str:
        """Retorna ambiente atual via variável de ambiente"""
        env = os.getenv("ENVIRONMENT", "hk").lower()
        if env not in EnvironmentConfig.ENVIRONMENTS:
            raise ValueError(f"Ambiente inválido: {env}. Opções: {EnvironmentConfig.ENVIRONMENTS}")
        return env

    @staticmethod
    def get_config(env: str = None) -> Dict[str, Any]:
        """Retorna configuração completa do ambiente"""
        if env is None:
            env = EnvironmentConfig.get_current_env()

        configs = {
            "hk": {
                "storage_account": "stcasesantander-hk",
                "key_vault": "kv-case-santander-hk",
                "catalog": "case_santander",  # Mesmo catalog, schemas separados
                "schema_prefix": "hk_",  # Schemas: hk_bronze, hk_silver, hk_gold
                "eventhub_ns": "evhcasesantander-hk",
                "eventhub_name": "transacoes-financeiras-hk",
                "databricks_workspace": "adb-xxx-hk.azuredatabricks.net",
                "api_rate_limit": {
                    "yahoo_finance": 15,  # requests/min (reduzido)
                    "bcb": 45,  # requests/min (reduzido)
                    "world_bank": 90,  # requests/min (reduzido)
                    "kaggle": 5,  # requests/min (reduzido)
                },
                "data_retention_days": 30,  # 30 dias (reduzido vs 90 dias prod)
                "enable_streaming": False,  # Sem streaming (redução de custo)
                "is_production": False,
            },
            "prod": {
                "storage_account": "stcasesantander",
                "key_vault": "kv-case-santander",
                "catalog": "case_santander",  # Mesmo catalog, schemas separados
                "schema_prefix": "prod_",  # Schemas: prod_bronze, prod_silver, prod_gold
                "eventhub_ns": "evhcasesantander",
                "eventhub_name": "transacoes-financeiras",
                "databricks_workspace": "adb-xxx.azuredatabricks.net",
                "api_rate_limit": {
                    "yahoo_finance": 30,  # requests/min (completo)
                    "bcb": 120,  # requests/min (completo)
                    "world_bank": 300,  # requests/min (completo)
                    "kaggle": 20,  # requests/min (completo)
                },
                "data_retention_days": 90,  # 90 dias (completo)
                "enable_streaming": True,  # Com streaming (completo)
                "is_production": True,
            },
        }

        return configs[env]

    @staticmethod
    def get_paths(env: str = None) -> Dict[str, str]:
        """Retorna caminhos com schemas separados por ambiente (mesmo catalog)"""
        config = EnvironmentConfig.get_config(env)
        storage = config["storage_account"]
        catalog = config["catalog"]
        schema_prefix = config["schema_prefix"]

        return {
            # ADLS Paths (isolados por storage account)
            "bronze_acoes": f"abfss://bronze@{storage}.dfs.core.windows.net/acoes/",
            "bronze_bcb": f"abfss://bronze@{storage}.dfs.core.windows.net/bcb/",
            "bronze_world_bank": f"abfss://bronze@{storage}.dfs.core.windows.net/world_bank/",
            "bronze_kafka": f"abfss://bronze@{storage}.dfs.core.windows.net/kafka/",
            "bronze_clientes": f"abfss://bronze@{storage}.dfs.core.windows.net/clientes/",
            "bronze_ordens": f"abfss://bronze@{storage}.dfs.core.windows.net/ordens/",
            # Silver
            "silver_acoes": f"abfss://silver@{storage}.dfs.core.windows.net/acoes/",
            "silver_bcb": f"abfss://silver@{storage}.dfs.core.windows.net/bcb/",
            "silver_world_bank": f"abfss://silver@{storage}.dfs.core.windows.net/world_bank/",
            "silver_streaming": f"abfss://silver@{storage}.dfs.core.windows.net/streaming/",
            "silver_clientes": f"abfss://silver@{storage}.dfs.core.windows.net/clientes/",
            "silver_ordens": f"abfss://silver@{storage}.dfs.core.windows.net/ordens/",
            # Gold
            "gold_anomalias": f"abfss://gold@{storage}.dfs.core.windows.net/anomalias/",
            "gold_performance": f"abfss://gold@{storage}.dfs.core.windows.net/performance_acoes/",
            "gold_cambio": f"abfss://gold@{storage}.dfs.core.windows.net/acoes_vs_cambio/",
            "gold_observ": f"abfss://gold@{storage}.dfs.core.windows.net/observabilidade/",
            # Unity Catalog Tables (mesmo catalog, schemas separados)
            "schema_bronze": f"{catalog}.{schema_prefix}bronze",
            "schema_silver": f"{catalog}.{schema_prefix}silver",
            "schema_gold": f"{catalog}.{schema_prefix}gold",
            "table_bronze_clientes": f"{catalog}.{schema_prefix}bronze.clientes",
            "table_bronze_ordens": f"{catalog}.{schema_prefix}bronze.ordens",
            "table_silver_clientes": f"{catalog}.{schema_prefix}silver.clientes",
            "table_silver_ordens": f"{catalog}.{schema_prefix}silver.ordens",
            "table_silver_streaming": f"{catalog}.{schema_prefix}silver.streaming",
        }

    @staticmethod
    def validate_environment(env: str = None) -> bool:
        """Valida se ambiente está corretamente configurado"""
        config = EnvironmentConfig.get_config(env)

        # Verificar se é produção
        if config["is_production"]:
            # Validações adicionais para produção
            print(f"[WARN] Ambiente de PRODUÇÃO detectado: {env}")
            print(f"[WARN] Storage Account: {config['storage_account']}")
            print(f"[WARN] Catalog: {config['catalog']}")

            # Em produção, exigir confirmação (opcional)
            confirmation = os.getenv("CONFIRM_PRODUCTION", "false").lower()
            if confirmation != "true":
                print("[ERROR] Produção requer confirmação explícita")
                print("[ERROR] Defina CONFIRM_PRODUCTION=true para continuar")
                return False

        return True


# Funções de conveniência
def get_env() -> str:
    """Retorna ambiente atual"""
    return EnvironmentConfig.get_current_env()


def get_config() -> Dict[str, Any]:
    """Retorna configuração do ambiente atual"""
    return EnvironmentConfig.get_config()


def get_paths() -> Dict[str, str]:
    """Retorna caminhos do ambiente atual"""
    return EnvironmentConfig.get_paths()


def is_production() -> bool:
    """Verifica se é ambiente de produção"""
    return EnvironmentConfig.get_config()["is_production"]
