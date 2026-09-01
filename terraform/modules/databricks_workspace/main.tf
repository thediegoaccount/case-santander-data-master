# Databricks Workspace Module

resource "azurerm_databricks_workspace" "this" {
  name                      = "${var.workspace_prefix}${var.environment}"
  resource_group_name       = var.resource_group_name
  location                  = var.location
  sku                       = var.sku
  managed_resource_group_id = var.managed_resource_group_id

  custom_parameters {
    no_public_ip = false
  }

  tags = var.tags
}
