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

variable "storage_account_prefix" {
  description = "Storage account name prefix"
  type        = string
  default     = "stcasesantander"
}

variable "tags" {
  description = "Tags for resources"
  type        = map(string)
}

variable "data_contributor_principal_ids" {
  description = "Object IDs que recebem Storage Blob Data Contributor na conta (SP dos jobs, Access Connector do Unity Catalog)"
  type        = map(string)
  default     = {}
}
