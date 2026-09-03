#!/usr/bin/env bash
#
# Deploy da infraestrutura em duas etapas.
#
# Por que duas etapas: o provider databricks precisa de `host`, que so existe
# depois do workspace criado. Terraform avalia config de provider no plan, e
# valor desconhecido nessa posicao aborta. Entao a etapa 1 cria o Azure e a
# etapa 2 cria o que vive dentro do Databricks.
#
# Uso:
#   ./scripts/deploy_infra.sh bootstrap   # so na primeira vez (backend do state)
#   ./scripts/deploy_infra.sh stage1      # RG, storage, key vault, workspace, event hub
#   ./scripts/deploy_infra.sh stage2      # secret scope, unity catalog
#   ./scripts/deploy_infra.sh all         # stage1 + stage2, com host automatico
#
# Para derrubar tudo: scripts/destroy_infra.sh (mesma variavel ENVIRONMENT).
#
# O ambiente vem da variavel ENVIRONMENT (default: hk) e define o state key:
#   ENVIRONMENT=prod ./scripts/deploy_infra.sh all -var-file=terraform/environments/prod.tfvars

set -euo pipefail

TF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../terraform" && pwd)"

# State separado por ambiente. O bloco `backend` de main.tf tem `key` fixo e
# nao aceita variaveis; sem este -backend-config, hk e prod compartilham o
# mesmo state e aplicar um planeja destruir o outro.
ENVIRONMENT="${ENVIRONMENT:-hk}"
BACKEND_KEY="case-santander-data-${ENVIRONMENT}.tfstate"
STAGE1_TARGETS=(
  -target=module.resource_group
  -target=module.storage_account
  -target=module.key_vault
  -target=module.databricks_workspace
  -target=module.event_hub
  -target=module.secrets
)

log() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

cmd_bootstrap() {
  log "Bootstrap do backend remoto (state)"
  cd "$TF_DIR/bootstrap"
  terraform init
  terraform apply "$@"
  log "Backend pronto. Agora rode: $0 stage1"
}

cmd_stage1() {
  log "Etapa 1: infraestrutura Azure"
  cd "$TF_DIR"
  terraform init -backend-config="key=$BACKEND_KEY"
  terraform validate
  terraform apply "${STAGE1_TARGETS[@]}" "$@"
  log "Workspace host: $(terraform output -raw databricks_workspace_host)"
  echo "Coloque esse valor em databricks_host no terraform.tfvars, depois rode: $0 stage2"
}

cmd_stage2() {
  log "Etapa 2: recursos dentro do Databricks"
  cd "$TF_DIR"
  terraform apply "$@"
  log "Nomes efetivos (devem bater com src/config/environment.py):"
  terraform output resource_names
}

# Encadeia as duas etapas passando o host da etapa 1 direto por -var,
# sem precisar editar o tfvars no meio.
cmd_all() {
  cmd_stage1 "$@"
  cd "$TF_DIR"
  local host
  host="$(terraform output -raw databricks_workspace_host)"
  log "Etapa 2 com databricks_host=$host"
  terraform apply -var="databricks_host=$host" "$@"
  terraform output resource_names
}

case "${1:-}" in
  bootstrap) shift; cmd_bootstrap "$@" ;;
  stage1)    shift; cmd_stage1 "$@" ;;
  stage2)    shift; cmd_stage2 "$@" ;;
  all)       shift; cmd_all "$@" ;;
  *) awk 'NR>1 && /^#/{sub(/^# ?/,"");print;next} NR>1{exit}' "$0"; exit 1 ;;
esac
