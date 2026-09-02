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

variable "namespace_prefix" {
  description = "Prefixo do Event Hub namespace (o ambiente e sufixado)"
  type        = string
}

variable "event_hub_prefix" {
  description = "Prefixo do Event Hub (o ambiente e sufixado)"
  type        = string
}

variable "tags" {
  description = "Tags for resources"
  type        = map(string)
}

variable "storage_account_id" {
  description = "Resource ID da storage account destino do Event Hub Capture"
  type        = string
}

variable "capture_container" {
  description = "Container onde o Capture grava (camada bronze)"
  type        = string
  default     = "bronze"
}
