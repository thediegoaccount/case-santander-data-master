# Outputs

output "resource_group_name" {
  description = "Resource group name"
  value       = module.resource_group.name
}

output "storage_account_name" {
  description = "Storage account name"
  value       = module.storage_account.storage_account_name
}

output "storage_account_dfs_endpoint" {
  description = "Endpoint DFS (ADLS Gen2)"
  value       = module.storage_account.primary_dfs_endpoint
}

output "key_vault_name" {
  description = "Key Vault name"
  value       = module.key_vault.key_vault_name
}

output "key_vault_id" {
  description = "Resource ID do Key Vault (usado ao criar o secret scope a mao)"
  value       = module.key_vault.key_vault_id
}

output "key_vault_uri" {
  description = "Key Vault URI"
  value       = module.key_vault.key_vault_uri
}

output "databricks_workspace_url" {
  description = "Databricks workspace URL"
  value       = module.databricks_workspace.workspace_url
}

output "databricks_workspace_host" {
  description = "Host com https:// - use como valor de databricks_host na etapa 2"
  value       = module.databricks_workspace.workspace_host
}

output "databricks_workspace_id" {
  description = "ID numerico do workspace no control plane Databricks"
  value       = module.databricks_workspace.workspace_id
}

output "event_hub_namespace_name" {
  description = "Event Hub namespace name"
  value       = module.event_hub.namespace_name
}

output "event_hub_name" {
  description = "Event Hub name"
  value       = module.event_hub.event_hub_name
}

output "event_hub_connection_string" {
  description = "Event Hub connection string"
  value       = module.event_hub.connection_string
  sensitive   = true
}

output "unity_catalog_name" {
  description = "Unity Catalog name"
  value       = module.unity_catalog.catalog_name
}

output "databricks_secret_scope" {
  description = "Secret scope consumido por src/config/secrets.py"
  value       = var.create_secret_scope ? module.secret_scope[0].scope_name : module.key_vault.key_vault_name
}

output "unity_catalog_schemas" {
  description = "Schemas criados no catalog"
  value       = module.unity_catalog.schemas
}

# Fonte da verdade da nomenclatura. src/config/environment.py deriva os
# mesmos nomes pela convencao; use este output para conferir.
output "resource_names" {
  description = "Nomes efetivos dos recursos, para conferir contra environment.py"
  value = {
    storage_account = module.storage_account.storage_account_name
    key_vault       = module.key_vault.key_vault_name
    eventhub_ns     = module.event_hub.namespace_name
    eventhub_name   = module.event_hub.event_hub_name
    catalog         = module.unity_catalog.catalog_name
    schema_prefix   = local.schema_prefix
  }
}
