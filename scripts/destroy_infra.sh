#!/usr/bin/env bash
#
# Derruba a infraestrutura criada por scripts/deploy_infra.sh.
#
# Um unico `terraform destroy` cobre tudo: Terraform calcula a ordem reversa
# de dependencia a partir do proprio state (unity catalog e secret scope
# antes do workspace, storage e key vault antes do resource group). Nao
# precisa dos dois estagios do apply -- aquela divisao existia so por causa
# do provider databricks exigir `host` no plan, e no destroy o host vem do
# output do state, nao precisa ser descoberto em etapas.
#
# Uso:
#   ./scripts/destroy_infra.sh all         # derruba tudo (pede confirmacao)
#   ./scripts/destroy_infra.sh all -auto-approve   # sem confirmacao (CI)
#   ./scripts/destroy_infra.sh stage2      # so unity catalog + secret scope
#   ./scripts/destroy_infra.sh bootstrap   # derruba o BACKEND do state (raro, cuidado)
#
# O ambiente vem da variavel ENVIRONMENT (default: hk), igual ao deploy:
#   ENVIRONMENT=prod ./scripts/destroy_infra.sh all -var-file=terraform/environments/prod.tfvars
#
# ATENCAO -- metastore compartilhado entre ambientes:
# Azure permite UM metastore Unity Catalog por regiao/account. Se voce
# apontou um segundo ambiente para o metastore deste com
# `existing_metastore_id`, derrubar ESTE ambiente derruba o metastore que o
# outro depende. Terraform nao ve essa dependencia (estados diferentes).
# Nesse cenario, derrube primeiro os ambientes que USAM existing_metastore_id,
# so por ultimo o que criou o metastore.

set -euo pipefail

TF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../terraform" && pwd)"
ENVIRONMENT="${ENVIRONMENT:-hk}"
BACKEND_KEY="case-santander-data-${ENVIRONMENT}.tfstate"

log() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

# Le o host do workspace direto do state (nao precisa do workspace vivo).
# Vazio se o state nao tiver essa etapa aplicada -- nesse caso o destroy nao
# precisa da var mesmo, porque nao ha nada dentro do Databricks a apagar.
databricks_host() {
  terraform output -raw databricks_workspace_host 2>/dev/null || true
}

cmd_all() {
  log "Destruindo tudo (ambiente: $ENVIRONMENT)"
  cd "$TF_DIR"
  terraform init -backend-config="key=$BACKEND_KEY"
  local host
  host="$(databricks_host)"
  if [ -n "$host" ]; then
    log "databricks_host=$host"
    terraform destroy -var="databricks_host=$host" "$@"
  else
    log "Sem workspace no state -- destruindo so o que existir"
    terraform destroy "$@"
  fi
}

cmd_stage2() {
  log "Destruindo so unity catalog + secret scope (ambiente: $ENVIRONMENT)"
  cd "$TF_DIR"
  terraform init -backend-config="key=$BACKEND_KEY"
  local host
  host="$(databricks_host)"
  terraform destroy \
    -var="databricks_host=$host" \
    -target=module.unity_catalog \
    -target=module.secret_scope \
    "$@"
}

cmd_bootstrap() {
  log "Destruindo o BACKEND do state -- isto apaga o historico de todos os ambientes"
  cd "$TF_DIR/bootstrap"
  terraform init
  terraform destroy "$@"
}

case "${1:-}" in
  all)       shift; cmd_all "$@" ;;
  stage2)    shift; cmd_stage2 "$@" ;;
  bootstrap) shift; cmd_bootstrap "$@" ;;
  *) awk 'NR>1 && /^#/{sub(/^# ?/,"");print;next} NR>1{exit}' "$0"; exit 1 ;;
esac
