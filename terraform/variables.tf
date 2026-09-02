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
  description = "Host do workspace Databricks, com https:// (output databricks_workspace_host da etapa 1)"
  type        = string
  sensitive   = true
}

variable "databricks_account_id" {
  description = "Account ID do Databricks (console de accounts), para recursos account-level"
  type        = string
  sensitive   = true
}

variable "existing_metastore_id" {
  description = "Metastore Unity Catalog ja existente na regiao. Azure permite so um por regiao/account; se ja houver, informe o ID aqui."
  type        = string
  default     = null
}

variable "databricks_sku" {
  description = "Databricks workspace SKU"
  type        = string
  default     = "premium"
}

variable "databricks_managed_rg_name" {
  description = "Nome do managed resource group criado pelo Databricks"
  type        = string
  default     = "rg-databricks-managed"
}

variable "event_hub_namespace_prefix" {
  description = "Prefixo do Event Hub namespace (o ambiente e sufixado). Namespace e DNS global."
  type        = string
  default     = "evhcasesantander"
}

variable "event_hub_prefix" {
  description = "Prefixo do Event Hub (o ambiente e sufixado)"
  type        = string
  default     = "transacoes-financeiras"
}

variable "unity_catalog_name" {
  description = "Unity Catalog name"
  type        = string
  default     = "case_santander"
}

variable "object_id" {
  description = "Object ID do principal que administra o Key Vault. Se null, usa a identidade que roda o Terraform."
  type        = string
  default     = null
  sensitive   = true
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
  description = "Azure AD Tenant ID. Se null, usa o tenant da identidade que roda o Terraform."
  type        = string
  default     = null
  sensitive   = true
}

variable "create_secret_scope" {
  description = "Cria o secret scope Key Vault-backed. Exige credencial de USUARIO AAD; deixe false se o apply roda como service principal e crie o scope manualmente."
  type        = bool
  default     = true
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
