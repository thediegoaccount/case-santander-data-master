variable "catalog_name" {
  description = "Unity Catalog name"
  type        = string
}

variable "storage_account" {
  description = "Storage account name (usado no storage_root do metastore)"
  type        = string
}

variable "location" {
  description = "Azure region do metastore"
  type        = string
}

variable "environment" {
  description = "Environment (hk ou prod)"
  type        = string
}

variable "schema_prefix" {
  description = "Prefixo dos schemas (ex: hk_ ou prod_)"
  type        = string
}

variable "databricks_workspace_id" {
  description = "ID numerico do workspace Databricks para o metastore assignment"
  type        = string
}

variable "existing_metastore_id" {
  description = "ID de metastore ja existente na regiao. Se null, um novo e criado."
  type        = string
  default     = null
}

variable "access_connector_id" {
  description = "Resource ID do Databricks Access Connector usado como credencial de storage do metastore"
  type        = string
}
