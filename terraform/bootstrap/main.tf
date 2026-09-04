# Bootstrap do backend remoto
#
# O backend "azurerm" do terraform/main.tf aponta para recursos que precisam
# EXISTIR ANTES do primeiro `terraform init`. Este config os cria usando
# backend local (state fica em bootstrap/terraform.tfstate -- versione ou
# guarde em local seguro).
#
#   cd terraform/bootstrap && terraform init && terraform apply
#   cd .. && terraform init

terraform {
  required_version = ">= 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "tfstate" {
  name     = var.resource_group_name
  location = var.location
}

resource "azurerm_storage_account" "tfstate" {
  name                = var.storage_account_name
  resource_group_name = azurerm_resource_group.tfstate.name
  location            = azurerm_resource_group.tfstate.location
  account_tier        = "Standard"
  # Mesmo motivo do storage_account principal: numa indisponibilidade da
  # regiao primaria, LRS deixaria o proprio state do Terraform
  # inacessivel -- sem conseguir nem gerenciar/reconstruir a infra
  # enquanto durar. RA-GRS da leitura da copia geo-replicada sem esperar
  # failover.
  account_replication_type = "RAGRS"

  # State contem secrets: sem acesso publico e com versionamento para
  # permitir recuperar um state corrompido.
  allow_nested_items_to_be_public = false
  min_tls_version                 = "TLS1_2"

  blob_properties {
    versioning_enabled = true

    delete_retention_policy {
      days = 30
    }
  }
}

resource "azurerm_storage_container" "tfstate" {
  name                  = var.container_name
  storage_account_name  = azurerm_storage_account.tfstate.name
  container_access_type = "private"
}
