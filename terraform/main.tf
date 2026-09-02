# Main Terraform Configuration
# Case Santander Data Engineering - Infrastructure as Code
#
# APLICACAO EM DUAS ETAPAS (o provider databricks precisa do workspace ja
# existente; Terraform nao aceita config de provider desconhecida no plan):
#
#   1) terraform apply -target=module.resource_group \
#                      -target=module.storage_account \
#                      -target=module.key_vault \
#                      -target=module.databricks_workspace \
#                      -target=module.event_hub
#   2) preencha databricks_host com o output databricks_workspace_host
#      e rode: terraform apply

terraform {
  required_version = ">= 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.47"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }

  backend "azurerm" {
    resource_group_name  = "rg-terraform-state"
    storage_account_name = "stterrasantander"
    container_name       = "tfstate"
    key                  = "case-santander-data.tfstate"
  }
}

provider "azurerm" {
  features {}
}

provider "azuread" {}

provider "random" {}

# Workspace-level: catalogs, schemas, secret scopes
provider "databricks" {
  alias = "workspace"
  host  = var.databricks_host
}

# Account-level: metastore e metastore assignment
provider "databricks" {
  alias      = "account"
  host       = "https://accounts.azuredatabricks.net"
  account_id = var.databricks_account_id
}

# Identidade que esta rodando o Terraform. Evita ter que informar
# tenant_id / object_id a mao -- as variaveis continuam existindo apenas
# como override.
data "azurerm_client_config" "current" {}

# Object ID do SP "AzureDatabricks" neste tenant. O client_id e fixo e
# igual em todos os tenants Azure.
data "azuread_service_principal" "databricks" {
  client_id = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
}

# O SP que os jobs usam para o ADLS (configure_adls em src/config/settings.py).
# var.client_id e o application id; role assignment exige o object id.
data "azuread_service_principal" "jobs" {
  client_id = var.client_id
}

# Identidade gerenciada do Unity Catalog para acessar o storage_root do
# metastore. Sem ela, toda tabela gerenciada (saveAsTable) falha.
resource "azurerm_databricks_access_connector" "unity_catalog" {
  name                = "dbac-case-santander-${var.environment}"
  resource_group_name = module.resource_group.name
  location            = var.location
  tags                = var.tags

  identity {
    type = "SystemAssigned"
  }
}

locals {
  schema_prefix = "${var.environment}_"

  tenant_id = coalesce(var.tenant_id, data.azurerm_client_config.current.tenant_id)
  object_id = coalesce(var.object_id, data.azurerm_client_config.current.object_id)
}

# Modules
module "resource_group" {
  source = "./modules/resource_group"

  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

module "storage_account" {
  source = "./modules/storage_account"

  resource_group_name = module.resource_group.name
  location            = var.location
  environment         = var.environment
  tags                = var.tags

  data_contributor_principal_ids = {
    jobs_sp       = data.azuread_service_principal.jobs.object_id
    unity_catalog = azurerm_databricks_access_connector.unity_catalog.identity[0].principal_id
  }
}

module "key_vault" {
  source = "./modules/key_vault"

  resource_group_name = module.resource_group.name
  location            = var.location
  environment         = var.environment
  tags                = var.tags

  tenant_id               = local.tenant_id
  object_id               = local.object_id
  databricks_sp_object_id = data.azuread_service_principal.databricks.object_id
}

module "databricks_workspace" {
  source = "./modules/databricks_workspace"

  resource_group_name = module.resource_group.name
  location            = var.location
  environment         = var.environment
  tags                = var.tags

  sku                         = var.databricks_sku
  managed_resource_group_name = var.databricks_managed_rg_name
}

module "event_hub" {
  source = "./modules/event_hub"

  resource_group_name = module.resource_group.name
  location            = var.location
  environment         = var.environment
  tags                = var.tags

  namespace_prefix = var.event_hub_namespace_prefix
  event_hub_prefix = var.event_hub_prefix

  # Capture grava direto na camada bronze, em kafka/
  storage_account_id = module.storage_account.storage_account_id
  capture_container  = "bronze"
}

module "secrets" {
  source = "./modules/secrets"

  key_vault_id = module.key_vault.key_vault_id

  secrets = {
    client-id       = var.client_id
    client-secret   = var.client_secret
    tenant-id       = local.tenant_id
    storage-account = module.storage_account.storage_account_name
    kaggle-username = var.kaggle_username
    kaggle-key      = var.kaggle_key
    salt            = var.salt
  }
}

# --- Etapa 2: exige var.databricks_host preenchida ---

module "secret_scope" {
  source = "./modules/secret_scope"
  count  = var.create_secret_scope ? 1 : 0

  providers = {
    databricks.workspace = databricks.workspace
  }

  # O scope tem o mesmo nome do Key Vault, que e o que
  # src/config/environment.py devolve em config["key_vault"].
  scope_name    = module.key_vault.key_vault_name
  key_vault_id  = module.key_vault.key_vault_id
  key_vault_uri = module.key_vault.key_vault_uri
}

module "unity_catalog" {
  source = "./modules/unity_catalog"

  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
  }

  catalog_name            = var.unity_catalog_name
  storage_account         = module.storage_account.storage_account_name
  location                = var.location
  environment             = var.environment
  schema_prefix           = local.schema_prefix
  databricks_workspace_id = module.databricks_workspace.workspace_id
  existing_metastore_id   = var.existing_metastore_id
  access_connector_id     = azurerm_databricks_access_connector.unity_catalog.id
}
