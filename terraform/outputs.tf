# Outputs

output "resource_group_name" {
  description = "Resource group name"
  value       = module.resource_group.name
}

output "storage_account_name" {
  description = "Storage account name"
  value       = module.storage_account.storage_account_name
}

output "storage_account_primary_endpoint" {
  description = "Storage account primary endpoint"
  value       = module.storage_account.primary_endpoint
}

output "key_vault_name" {
  description = "Key Vault name"
  value       = module.key_vault.key_vault_name
}

output "key_vault_uri" {
  description = "Key Vault URI"
  value       = module.key_vault.key_vault_uri
}

output "databricks_workspace_url" {
  description = "Databricks workspace URL"
  value       = module.databricks_workspace.workspace_url
}

output "databricks_workspace_id" {
  description = "Databricks workspace ID"
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
