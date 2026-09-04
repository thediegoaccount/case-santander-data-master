output "storage_account_name" {
  description = "Storage account name"
  value       = azurerm_storage_account.this.name
}

output "primary_dfs_endpoint" {
  description = "Endpoint DFS (ADLS Gen2), base dos paths abfss://"
  value       = azurerm_storage_account.this.primary_dfs_endpoint
}

output "secondary_dfs_endpoint" {
  description = <<-EOT
    Endpoint DFS da regiao SECUNDARIA (RA-GRS) -- so populado apos o
    provider confirmar a replicacao geo. E o endpoint de LEITURA a usar
    numa indisponibilidade da regiao primaria, ANTES de decidir por um
    failover de conta (que e destrutivo/demorado e troca qual regiao e a
    "primaria"). Nao serve para escrita.
  EOT
  value       = azurerm_storage_account.this.secondary_dfs_endpoint
}

output "secondary_location" {
  description = "Regiao secundaria pareada pelo Azure para a replicacao RA-GRS (nao configuravel; decidida pelo par de regioes da regiao primaria)"
  value       = azurerm_storage_account.this.secondary_location
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
