# Key Vault Module

resource "azurerm_key_vault" "this" {
  name                = "${var.key_vault_prefix}-${var.environment}"
  location            = var.location
  resource_group_name = var.resource_group_name
  tenant_id           = var.tenant_id
  sku_name            = "standard"

  soft_delete_retention_days = 7
  purge_protection_enabled   = false

  # Operador humano / service principal do Terraform
  access_policy {
    tenant_id = var.tenant_id
    object_id = var.object_id

    key_permissions = [
      "Get",
      "List",
      "Create",
      "Delete",
      "Recover"
    ]

    secret_permissions = [
      "Get",
      "List",
      "Set",
      "Delete",
      "Recover"
    ]
  }

  # Service principal "AzureDatabricks": obrigatorio para o secret scope
  # Key Vault-backed conseguir ler os secrets.
  access_policy {
    tenant_id = var.tenant_id
    object_id = var.databricks_sp_object_id

    secret_permissions = [
      "Get",
      "List"
    ]
  }

  tags = var.tags
}
