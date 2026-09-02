variable "resource_group_name" {
  description = "Resource group do state remoto (deve bater com o backend em ../main.tf)"
  type        = string
  default     = "rg-terraform-state"
}

variable "storage_account_name" {
  description = "Storage account do state remoto (deve bater com o backend em ../main.tf)"
  type        = string
  default     = "stterrasantander"
}

variable "container_name" {
  description = "Container do state remoto (deve bater com o backend em ../main.tf)"
  type        = string
  default     = "tfstate"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "eastus2"
}
