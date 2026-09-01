# Secrets Module

resource "azurerm_key_vault_secret" "client_id" {
  name         = "client-id"
  value        = var.secrets["client-id"]
  key_vault_id = var.key_vault_id
}

resource "azurerm_key_vault_secret" "client_secret" {
  name         = "client-secret"
  value        = var.secrets["client-secret"]
  key_vault_id = var.key_vault_id
}

resource "azurerm_key_vault_secret" "tenant_id" {
  name         = "tenant-id"
  value        = var.secrets["tenant-id"]
  key_vault_id = var.key_vault_id
}

resource "azurerm_key_vault_secret" "storage_account" {
  name         = "storage-account"
  value        = var.secrets["storage-account"]
  key_vault_id = var.key_vault_id
}

resource "azurerm_key_vault_secret" "kaggle_username" {
  name         = "kaggle-username"
  value        = var.secrets["kaggle-username"]
  key_vault_id = var.key_vault_id
}

resource "azurerm_key_vault_secret" "kaggle_key" {
  name         = "kaggle-key"
  value        = var.secrets["kaggle-key"]
  key_vault_id = var.key_vault_id
}

resource "azurerm_key_vault_secret" "salt" {
  name         = "salt"
  value        = var.secrets["salt"]
  key_vault_id = var.key_vault_id
  count        = var.include_salt ? 1 : 0
}
