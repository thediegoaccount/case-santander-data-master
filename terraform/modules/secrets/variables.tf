variable "key_vault_id" {
  description = "Key Vault ID"
  type        = string
}

variable "secrets" {
  description = "Map of secrets to store in Key Vault"
  type        = map(string)
  sensitive   = true
}

variable "include_salt" {
  description = "Whether to include salt secret"
  type        = bool
  default     = true
}
