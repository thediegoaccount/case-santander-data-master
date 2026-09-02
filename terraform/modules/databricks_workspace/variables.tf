variable "resource_group_name" {
  description = "Resource group name"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
}

variable "environment" {
  description = "Environment (hk or prod)"
  type        = string
}

variable "workspace_prefix" {
  description = "Databricks workspace name prefix"
  type        = string
  default     = "databricks-santander"
}

variable "sku" {
  description = "Databricks workspace SKU"
  type        = string
  default     = "premium"
}

variable "managed_resource_group_name" {
  description = "Nome do managed resource group criado pelo Databricks"
  type        = string
  default     = null
}

variable "tags" {
  description = "Tags for resources"
  type        = map(string)
}
