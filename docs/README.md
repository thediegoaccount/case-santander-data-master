# Documentação — Case Santander Data Master

## Trilhas de leitura

Escolha pelo tempo disponível e pelo objetivo.

### Avaliação em 15 minutos

1. [`README.md`](../README.md) — o case, a arquitetura e os resultados
2. [`adr/`](adr/) — as decisões de arquitetura e seus trade-offs · **comece por aqui se o
   interesse for julgar critério de engenharia, não implementação**
3. [`escalabilidade.md`](escalabilidade.md) — a afirmação de 100×, as provas e os limites

### Reproduzir o ambiente

1. [`../README.md`](../README.md) § *Reprodutibilidade da Arquitetura* — passo a passo completo
2. [`terraform-architecture.md`](terraform-architecture.md) + [`../terraform/README.md`](../terraform/README.md) — provisionamento da infraestrutura Azure via IaC
3. [`environment-isolation.md`](environment-isolation.md) — separação dev/hk/prod
4. [`authorization-deployment.md`](authorization-deployment.md) — Service Principal e permissões
5. [`airflow-configuration.md`](airflow-configuration.md) — Airflow local em Docker
6. [`../DEPLOYMENT.md`](../DEPLOYMENT.md) — deploy do Asset Bundle

### Aprofundamento técnico

| Tema | Documentos |
|---|---|
| **Arquitetura** | [`architecture-kappa-vs-lambda.md`](architecture-kappa-vs-lambda.md) · [`architecture-pillars-assessment.md`](architecture-pillars-assessment.md) · [`technical-reference.md`](technical-reference.md) |
| **Streaming e CDC** | [`streaming-configuration.md`](streaming-configuration.md) · [`streaming-continuous-implementation.md`](streaming-continuous-implementation.md) · [`cdc-clientes-implementation.md`](cdc-clientes-implementation.md) · [`eventhub-producer.md`](eventhub-producer.md) |
| **Orquestração** | [`airflow-databricks-sync.md`](airflow-databricks-sync.md) · [`dynamic-pipeline-guide.md`](dynamic-pipeline-guide.md) · [`../CLUSTER_CONFIG.md`](../CLUSTER_CONFIG.md) |
| **Segurança e governança** | [`data-anonymization.md`](data-anonymization.md) · [`api-security-governance.md`](api-security-governance.md) · [`environment-isolation.md`](environment-isolation.md) |
| **CI/CD** | [`ci-cd-multi-environment.md`](ci-cd-multi-environment.md) |

## Diagrama

[`arquitetura-case-santander.drawio`](arquitetura-case-santander.drawio) — abrir em
[app.diagrams.net](https://app.diagrams.net) (*File → Open From → Device*) ou pela extensão
Draw.io Integration no VS Code.

## Arquivo

[`archive/`](archive/) — relatórios de refatorações concluídas e listas de tarefas históricas.
Não refletem o estado atual e não são mantidos.
