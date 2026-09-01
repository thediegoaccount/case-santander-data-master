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

variable "namespace_name" {
  description = "Event Hub namespace name"
  type        = string
}

variable "event_hub_name" {
  description = "Event Hub name"
  type        = string
}

variable "tags" {
  description = "Tags for resources"
  type        = map(string)
}
