# Databricks Secret Scope Module
#
# Cria o scope Key Vault-backed que src/config/secrets.py consome via
# dbutils.secrets.get(scope=<nome do key vault>, key=...).
# O nome do scope e propositalmente igual ao nome do Key Vault.
#
# LIMITACAO DO AZURE: scope Key Vault-backed so pode ser criado com
# credencial de USUARIO AAD (nao service principal). Se o apply rodar como
# SP, este recurso falha e precisa ser criado uma vez manualmente:
#   databricks secrets create-scope --scope <nome> \
#     --scope-backend-type AZURE_KEYVAULT \
#     --resource-id <key_vault_id> --dns-name <key_vault_uri>

terraform {
  required_providers {
    databricks = {
      source                = "databricks/databricks"
      configuration_aliases = [databricks.workspace]
    }
  }
}

resource "databricks_secret_scope" "this" {
  provider = databricks.workspace

  name                     = var.scope_name
  initial_manage_principal = "users"

  keyvault_metadata {
    resource_id = var.key_vault_id
    dns_name    = var.key_vault_uri
  }
}
