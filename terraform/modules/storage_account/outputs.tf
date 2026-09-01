output "storage_account_name" {
  description = "Storage account name"
  value       = azurerm_storage_account.this.name
}

output "primary_endpoint" {
  description = "Storage account primary endpoint"
  value       = azurerm_storage_account.this.primary_endpoint
}

output "primary_access_key" {
  description = "Storage account primary access key"
  value       = azurerm_storage_account.this.primary_access_key
  sensitive   = true
}
