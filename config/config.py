"""
Configuracoes do projeto.

Os nomes de recurso NAO sao hardcoded: derivam de src/config/environment.py,
que segue a mesma convencao do Terraform. Antes este arquivo fixava os nomes
de producao, o que quebrava em hk.
"""

import os
import sys

# Raiz do repo no sys.path: o projeto importa como "src.config.*".
# Nao usar "src" direto no path -- existe um pacote config/ na raiz e outro
# em src/config/, e os dois colidiriam.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.config.environment import EnvironmentConfig  # noqa: E402

_config = EnvironmentConfig.get_config()

STORAGE_ACCOUNT = _config["storage_account"]
EVENTHUB_NAME = _config["eventhub_name"]
EVENTHUB_NS = _config["eventhub_ns"]

# Camadas ADLS
BRONZE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD   = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# Acoes monitoradas
ACOES = [
    "PETR4.SA",   # Petrobras
    "VALE3.SA",   # Vale
    "ITUB4.SA",   # Itau
    "BBDC4.SA",   # Bradesco
    "ABEV3.SA",   # Ambev
    "MGLU3.SA",   # Magazine Luiza
    "WEGE3.SA",   # WEG
    "BBAS3.SA",   # Banco do Brasil
    "SANB11.SA",  # Santander
]

# Unity Catalog
CATALOG = _config["catalog"]
SCHEMA_PREFIX = _config["schema_prefix"]
SCHEMAS = [f"{SCHEMA_PREFIX}{layer}" for layer in ("bronze", "silver", "gold")]
