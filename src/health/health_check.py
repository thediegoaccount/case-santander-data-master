"""
Health Check Framework

Verifica saúde do sistema e recursos externos.
"""

from typing import Dict, List
from databricks.connect import DatabricksSession
from src.config.logging import info, error, critical
from src.config.secrets import get_secret


class HealthCheckError(Exception):
    """Erro de health check"""

    pass


class HealthChecker:
    """Verificador de saúde do sistema"""

    def __init__(self, job_name: str):
        self.job_name = job_name
        self.checks = {}

    def check_databricks_connection(self) -> bool:
        """Verifica conexão com Databricks"""
        try:
            spark = DatabricksSession.builder.getOrCreate()
            spark.sql("SELECT 1").collect()
            info(self.job_name, "Databricks connection OK")
            return True
        except Exception as e:
            error(self.job_name, f"Databricks connection FAILED: {str(e)}")
            return False

    def check_storage_connection(self) -> bool:
        """Verifica conexão com ADLS"""
        try:
            spark = DatabricksSession.builder.getOrCreate()
            storage_account = get_secret("storage-account")

            # Tentar listar containers
            spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")

            # Verificar se consegue acessar storage
            test_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
            spark.read.format("parquet").load(test_path).limit(0)

            info(self.job_name, f"Storage connection OK: {storage_account}")
            return True
        except Exception as e:
            error(self.job_name, f"Storage connection FAILED: {str(e)}")
            return False

    def check_key_vault_connection(self) -> bool:
        """Verifica conexão com Key Vault"""
        try:
            # Tentar recuperar um secret
            client_id = get_secret("client-id")

            if client_id:
                info(self.job_name, "Key Vault connection OK")
                return True
            else:
                error(self.job_name, "Key Vault connection FAILED: secret vazia")
                return False
        except Exception as e:
            error(self.job_name, f"Key Vault connection FAILED: {str(e)}")
            return False

    def check_unity_catalog(self) -> bool:
        """Verifica Unity Catalog"""
        try:
            spark = DatabricksSession.builder.getOrCreate()

            # Tentar listar schemas
            spark.sql("SHOW SCHEMAS IN case_santander").collect()

            info(self.job_name, "Unity Catalog OK")
            return True
        except Exception as e:
            error(self.job_name, f"Unity Catalog FAILED: {str(e)}")
            return False

    def check_delta_tables(self, tables: List[str]) -> bool:
        """Verifica se tabelas Delta existem"""
        try:
            spark = DatabricksSession.builder.getOrCreate()

            for table in tables:
                try:
                    spark.table(table).limit(0)
                    info(self.job_name, f"Table OK: {table}")
                except Exception:
                    error(self.job_name, f"Table MISSING: {table}")
                    return False

            return True
        except Exception as e:
            error(self.job_name, f"Delta tables check FAILED: {str(e)}")
            return False

    def check_api_connectivity(self, url: str) -> bool:
        """Verifica conectividade com API externa"""
        try:
            import requests

            response = requests.get(url, timeout=5)

            if response.status_code == 200:
                info(self.job_name, f"API connectivity OK: {url}")
                return True
            else:
                error(self.job_name, f"API connectivity FAILED: {url} (status: {response.status_code})")
                return False
        except Exception as e:
            error(self.job_name, f"API connectivity FAILED: {url} - {str(e)}")
            return False

    def run_all_checks(self) -> Dict[str, bool]:
        """
        Executa todos os health checks

        Returns:
            Dicionário com resultados dos checks
        """
        info(self.job_name, "Iniciando health checks")

        self.checks = {
            "databricks": self.check_databricks_connection(),
            "storage": self.check_storage_connection(),
            "key_vault": self.check_key_vault_connection(),
            "unity_catalog": self.check_unity_catalog(),
        }

        # Verificar se todos passaram
        all_passed = all(self.checks.values())

        if all_passed:
            info(self.job_name, "Todos os health checks passaram")
        else:
            failed = [k for k, v in self.checks.items() if not v]
            error(self.job_name, f"Health checks falharam: {failed}")
            critical(self.job_name, "Sistema não está saudável")

        return self.checks

    def get_health_status(self) -> str:
        """Retorna status de saúde"""
        if not self.checks:
            return "UNKNOWN"

        if all(self.checks.values()):
            return "HEALTHY"
        elif any(self.checks.values()):
            return "DEGRADED"
        else:
            return "UNHEALTHY"

    def get_failed_checks(self) -> List[str]:
        """Retorna lista de checks que falharam"""
        return [k for k, v in self.checks.items() if not v]


def health_check_decorator(job_name: str):
    """
    Decorator para executar health check antes da função

    Args:
        job_name: Nome do job

    Example:
        @health_check_decorator("job_name")
        def main():
            pass
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            checker = HealthChecker(job_name)
            checker.run_all_checks()

            status = checker.get_health_status()

            if status == "UNHEALTHY":
                raise HealthCheckError("Sistema não está saudável - abortando execução")

            return func(*args, **kwargs)

        return wrapper

    return decorator
