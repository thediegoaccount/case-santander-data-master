# Main Terraform Configuration
# Case Santander Data Engineering - Infrastructure as Code

terraform {
  required_version = ">= 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
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

provider "databricks" {
  host = var.databricks_host
}

provider "random" {}

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
}

module "key_vault" {
  source = "./modules/key_vault"

  resource_group_name = module.resource_group.name
  location            = var.location
  environment         = var.environment
  tags                = var.tags
}

module "databricks_workspace" {
  source = "./modules/databricks_workspace"

  resource_group_name = module.resource_group.name
  location            = var.location
  environment         = var.environment
  tags                = var.tags

  sku                 = var.databricks_sku
  managed_resource_group = var.databricks_managed_rg
}

module "event_hub" {
  source = "./modules/event_hub"

  resource_group_name = module.resource_group.name
  location            = var.location
  environment         = var.environment
  tags                = var.tags

  namespace_name      = var.event_hub_namespace
  event_hub_name      = var.event_hub_name
}

module "unity_catalog" {
  source = "./modules/unity_catalog"

  databricks_host = module.databricks_workspace.workspace_url
  catalog_name    = var.unity_catalog_name
}

module "secrets" {
  source = "./modules/secrets"

  key_vault_id = module.key_vault.key_vault_id

  secrets = {
    client-id     = var.client_id
    client-secret = var.client_secret
    tenant-id     = var.tenant_id
    storage-account = module.storage_account.storage_account_name
    kaggle-username = var.kaggle_username
    kaggle-key     = var.kaggle_key
    salt           = var.salt
  }
}
