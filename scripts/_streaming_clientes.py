"""
Helper compartilhado pelos producers de streaming (eventhub_producer.py e
eventhub_producer_advanced.py): busca a amostra REAL de hash_cliente
exportada por job_exportar_amostra_streaming, com fallback para um pool
sintetico quando as credenciais nao estao configuradas ou a busca falha.

O producer roda FORA do Databricks -- nao tem dbutils, nao tem get_secret.
As mesmas 4 credenciais que os jobs batch pegam do Key Vault (client-id,
client-secret, tenant-id, storage-account) aqui vem de variaveis de
ambiente, o mesmo padrao ja usado para EVENTHUB_CONNECTION_STRING em
eventhub_producer_advanced.py.
"""

import json
import os

# Pool usado quando as credenciais Azure nao estao configuradas, ou a busca
# falha (rede, arquivo ainda nao exportado, etc.). Formato "SYN-nnnn" de
# proposito diferente de hash_cliente real (SHA256, 64 chars) -- nunca deve
# ser confundido com um cliente de bronze/silver.clientes.
CLIENTES_SINTETICOS = [f"SYN-{i:04d}" for i in range(200)]

_CONTAINER = "gold"
_PATH = "amostra_clientes_streaming/amostra.json"


def carregar_clientes():
    """
    Retorna (lista_de_hash_cliente, fonte). fonte e "real" ou "sintetico".

    "real" exige AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID e
    STORAGE_ACCOUNT no ambiente (.env). Sem elas, ou se a busca falhar por
    qualquer motivo, cai no pool sintetico -- o producer nunca fica
    bloqueado por falta de credencial.
    """
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    tenant_id = os.getenv("AZURE_TENANT_ID")
    storage_account = os.getenv("STORAGE_ACCOUNT")

    if not all([client_id, client_secret, tenant_id, storage_account]):
        print("[AVISO] AZURE_CLIENT_ID/AZURE_CLIENT_SECRET/AZURE_TENANT_ID/"
              "STORAGE_ACCOUNT nao configurados -- usando pool sintetico de clientes.")
        return CLIENTES_SINTETICOS, "sintetico"

    try:
        from azure.identity import ClientSecretCredential
        from azure.storage.filedatalake import DataLakeServiceClient

        credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        service = DataLakeServiceClient(
            account_url=f"https://{storage_account}.dfs.core.windows.net",
            credential=credential,
        )
        file_client = (
            service.get_file_system_client(_CONTAINER)
            .get_file_client(_PATH)
        )
        conteudo = file_client.download_file().readall()
        clientes = json.loads(conteudo).get("hash_cliente") or []
        if not clientes:
            raise ValueError("amostra.json sem a chave hash_cliente ou vazia")

        print(f"[INFO] {len(clientes)} clientes reais carregados de bronze.clientes "
              f"(job_exportar_amostra_streaming).")
        return clientes, "real"

    except Exception as e:
        print(f"[AVISO] Falha ao buscar amostra real de clientes ({e}) -- "
              f"usando pool sintetico. Rode job_exportar_amostra_streaming "
              f"pelo menos uma vez para gerar {_CONTAINER}/{_PATH}.")
        return CLIENTES_SINTETICOS, "sintetico"
