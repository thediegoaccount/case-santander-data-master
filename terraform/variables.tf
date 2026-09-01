# Variables

variable "resource_group_name" {
  description = "Name of the resource group"
  type        = string
  default     = "rg-case-santander-data"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "eastus2"
}

variable "environment" {
  description = "Environment (hk or prod)"
  type        = string
  default     = "hk"
}

variable "tags" {
  description = "Tags for resources"
  type        = map(string)
  default = {
    Project     = "case-santander-data"
    Environment = "homologation"
    ManagedBy   = "terraform"
  }
}

variable "databricks_host" {
  description = "Databricks workspace host URL"
  type        = string
  sensitive   = true
}

variable "databricks_sku" {
  description = "Databricks workspace SKU"
  type        = string
  default     = "premium"
}

variable "databricks_managed_rg" {
  description = "Managed resource group for Databricks"
  type        = string
  default     = "rg-databricks-managed"
}

variable "event_hub_namespace" {
  description = "Event Hub namespace name"
  type        = string
  default     = "evhcasesantander"
}

variable "event_hub_name" {
  description = "Event Hub name"
  type        = string
  default     = "transacoes-financeiras"
}

variable "unity_catalog_name" {
  description = "Unity Catalog name"
  type        = string
  default     = "case_santander"
}

variable "client_id" {
  description = "Azure AD Client ID"
  type        = string
  sensitive   = true
}

variable "client_secret" {
  description = "Azure AD Client Secret"
  type        = string
  sensitive   = true
}

variable "tenant_id" {
  description = "Azure AD Tenant ID"
  type        = string
  sensitive   = true
}

variable "kaggle_username" {
  description = "Kaggle API username"
  type        = string
  sensitive   = true
}

variable "kaggle_key" {
  description = "Kaggle API key"
  type        = string
  sensitive   = true
}

variable "salt" {
  description = "Salt for data anonymization (SHA256 hashing)"
  type        = string
  sensitive   = true
}
