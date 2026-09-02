output "storage_account_name" {
  description = "Storage account name"
  value       = azurerm_storage_account.this.name
}

output "primary_dfs_endpoint" {
  description = "Endpoint DFS (ADLS Gen2), base dos paths abfss://"
  value       = azurerm_storage_account.this.primary_dfs_endpoint
}

output "primary_access_key" {
  description = "Storage account primary access key"
  value       = azurerm_storage_account.this.primary_access_key
  sensitive   = true
}

output "storage_account_id" {
  description = "Resource ID da storage account (escopo das role assignments)"
  value       = azurerm_storage_account.this.id
}
