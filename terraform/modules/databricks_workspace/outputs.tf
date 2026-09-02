output "workspace_url" {
  description = "Databricks workspace URL (hostname, sem esquema)"
  value       = azurerm_databricks_workspace.this.workspace_url
}

output "workspace_host" {
  description = "Databricks workspace host com https://"
  value       = "https://${azurerm_databricks_workspace.this.workspace_url}"
}

# ATENCAO: sao dois IDs diferentes.
# resource_id = ID ARM do Azure; workspace_id = ID numerico do control plane
# do Databricks, que e o exigido por databricks_metastore_assignment.
output "resource_id" {
  description = "Azure resource ID do workspace"
  value       = azurerm_databricks_workspace.this.id
}

output "workspace_id" {
  description = "ID numerico do workspace no control plane Databricks"
  value       = azurerm_databricks_workspace.this.workspace_id
}
