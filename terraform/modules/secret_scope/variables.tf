variable "scope_name" {
  description = "Nome do secret scope (deve bater com config['key_vault'])"
  type        = string
}

variable "key_vault_id" {
  description = "Resource ID do Key Vault"
  type        = string
}

variable "key_vault_uri" {
  description = "URI (dns_name) do Key Vault"
  type        = string
}
