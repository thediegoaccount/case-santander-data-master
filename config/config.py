# Configurações do projeto
STORAGE_ACCOUNT = "stcasesantander"
EVENTHUB_NAME   = "transacoes-financeiras"
EVENTHUB_NS     = "evhcasesantander"

# Camadas ADLS
BRONZE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD   = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# Ações monitoradas
ACOES = [
    "PETR4.SA", "VALE3.SA", "ITUB4.SA", "BBDC4.SA",
    "ABEV3.SA", "MGLU3.SA", "WEGE3.SA", "BBAS3.SA"
]
