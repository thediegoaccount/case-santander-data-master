# Configuracoes do projeto
STORAGE_ACCOUNT = "stcasesantander"
EVENTHUB_NAME   = "transacoes-financeiras"
EVENTHUB_NS     = "evhcasesantander"
SQL_SERVER      = "sqlsvr-case-santander.database.windows.net"
SQL_DATABASE    = "sqldb-case-santander"

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
CATALOG = "case_santander"
SCHEMAS = ["bronze", "silver", "gold"]
