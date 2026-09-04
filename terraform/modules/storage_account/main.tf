# Storage Account Module

resource "azurerm_storage_account" "this" {
  name                = "${var.storage_account_prefix}${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location
  account_tier        = "Standard"
  # RA-GRS: copia geo-replicada assincrona numa regiao secundaria PAREADA
  # (Azure escolhe automaticamente, ver output abaixo), com leitura da
  # secundaria disponivel de imediato -- nao GRS puro, que deixa a
  # secundaria ilegivel ate um failover manual. Isso e o que separa "o
  # dado sobrevive" de "eu consigo ler o dado durante a queda da regiao
  # primaria". RPO tipico <=15min (Geo Priority Replication da Microsoft),
  # nao zero: escrita muito recente pode nao ter chegado na secundaria
  # se a regiao cair no meio do caminho.
  #
  # LRS->GRS/RA-GRS e troca de SKU em vigor (nao forca recriacao). Migrar
  # para uma familia zone-redundant (ZRS/GZRS) depois, numa conta ja
  # existente com dado, pode exigir processo de migracao da Azure em vez
  # de troca direta -- avalie antes de trocar de novo.
  account_replication_type = "RAGRS"
  access_tier              = "Hot"

  # Obrigatorio para ADLS Gen2 / Unity Catalog e para rename atomico de
  # diretorio (commit protocol do Structured Streaming + Delta).
  # ATENCAO: alterar este atributo forca recriacao da storage account.
  is_hns_enabled = true

  tags = var.tags
}

resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.this.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.this.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.this.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "checkpoints" {
  name                  = "checkpoints"
  storage_account_name  = azurerm_storage_account.this.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "catalog" {
  name                  = "catalog"
  storage_account_name  = azurerm_storage_account.this.name
  container_access_type = "private"
}

# Sem isto o service principal autentica no AAD mas nao tem autorizacao no
# dado: todo spark.read/write em abfss:// retorna 403
# AuthorizationPermissionMismatch. A access policy do Key Vault da acesso ao
# SEGREDO, nao ao DADO -- sao coisas diferentes.
resource "azurerm_role_assignment" "blob_contributor" {
  for_each = var.data_contributor_principal_ids

  scope                = azurerm_storage_account.this.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = each.value
}
