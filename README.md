# Case Santander — Data Master
## Pipeline de Dados Financeiros — Corretora Santander

![CI/CD](https://github.com/thediegoaccount/case-santander-data-master/actions/workflows/ci-cd.yml/badge.svg)

---

## Objetivo

Desenvolver uma arquitetura de dados completa simulando o pipeline de uma corretora digital inspirada na Santander Corretora. O projeto contempla ingestão, transformação, análise e governança de dados financeiros reais do mercado brasileiro, com foco em detecção de anomalias, score de risco de clientes e detecção de fraudes.

---

## Arquitetura
[Fontes de Dados]
Yahoo Finance | BCB | World Bank | Kaggle | Event Hub
↓
[Azure Data Factory]
Ingestão batch (05:00 AM)
↓
[ADLS Gen2 — Bronze]
Dados brutos particionados
↓
[Databricks — Silver]
Limpeza, tipagem, Delta Lake
↓
[Databricks — Gold]
Anomalias, fraudes, scores
↓
┌──────────────┼──────────────┐
↓              ↓              ↓
Unity Catalog  Azure SQL DB  Dashboard + Genie AI

---

## Stack Tecnológica

| Componente | Tecnologia |
|---|---|
| Data Lake | Azure ADLS Gen2 |
| Processamento | Azure Databricks |
| Orquestração Batch | Azure Data Factory |
| Orquestração Local | Apache Airflow + Docker |
| Streaming | Azure Event Hub (Kafka) |
| Segurança | Azure Key Vault |
| Governança | Unity Catalog |
| Serving | Azure SQL Database |
| CI/CD | GitHub Actions |
| Formato | Delta Lake |
| IA Conversacional | Databricks Genie |
| Monitoramento | Lakehouse Monitoring |
| Versionamento | Git + Databricks Connect |

---

## Fontes de Dados

| Fonte | Dados | Registros |
|---|---|---|
| Yahoo Finance | 9 ações B3 (PETR4, VALE3, ITUB4, BBDC4, ABEV3, MGLU3, WEGE3, BBAS3, SANB11) | 8.530 |
| Banco Central | Selic, Câmbio USD/BRL, IPCA | 3.068 |
| World Bank | PIB anual, Desemprego | 59 |
| Kaggle | 10.000 clientes reais anonimizados | 10.000 |
| Event Hub | Transações streaming simuladas | 200 |

---

## Pipeline — Databricks Workflow
t0_unity_catalog_bronze
↓
t1_extracao
↓       ↓        ↓
t5_streaming  t6_clientes_ordens
↓               ↓
t2_silver    t7_corretora_analises
↓
t9_sdc
↓        ↓
t3_gold
↓
t8_lakehouse_monitoring
↓
t4_observabilidade
↓
t_carga_sql

---

## Unity Catalog
case_santander/
├── bronze/
│   ├── acoes       → 8.534
│   ├── bcb         → 3.068
│   ├── world_bank  →    59
│   ├── kafka       →   200
│   ├── clientes    → 10.000
│   └── ordens      →  5.341
├── silver/
│   ├── acoes       → 8.530
│   ├── bcb         → 3.068
│   ├── world_bank  →    59
│   ├── streaming   →   200
│   ├── clientes    → 10.000
│   ├── clientes_sdc → histórico SDC Type 2
│   └── ordens      →  5.341
└── gold/
├── performance_acoes
├── anomalias
├── acoes_vs_cambio
├── perfil_clientes
├── ordens_consolidadas
├── ranking_acoes_perfil
├── posicao_clientes
├── score_risco_clientes
├── score_risco_sdc → histórico SDC Type 2
├── deteccao_fraude
└── observabilidade

---

## LGPD — Práticas Adotadas

| Campo | Técnica |
|---|---|
| id_cliente | Hash SHA-256 (pseudonimização) |
| sobrenome | Primeira letra + asteriscos |
| CPF | Mascaramento parcial |
| Credenciais | Azure Key Vault |
| Bronze | Lifecycle Policy 30 dias |
| Dado analítico | Separado do transacional |

---

## Detecção de Anomalias e Fraudes

### Anomalias de mercado (Z-Score)
Z > 2  → Alta Anormal
Z < -2 → Queda Anormal

### Detecção de fraude por cliente
Regra 1: Valor acima do limite operacional
Regra 2: Volume suspeito (quantidade > 9.000)
Regra 3: Preço atípico (> R$90 ou < R$12)
Regra 4: Perfil incompatível com a operação
Score: Normal → Médio → Alto → Crítico

---

## Score de Risco
score = (score_credito * 0.4) +
(score_perfil  * 0.2) +
(score_saldo   * 0.2) +
(score_comportamento * 0.2)
Baixo Risco:    → limite R$ 500.000
Risco Moderado: → limite R$ 200.000
Risco Alto:     → limite R$  50.000

---

## SDC Type 2

Implementado para rastrear mudanças históricas em:
- `silver.clientes_sdc` → evolução do perfil de risco
- `gold.score_risco_sdc` → evolução do score e limite operacional
hash_cliente | perfil_risco | data_inicio | data_fim   | atual
abc123       | Conservador  | 2024-01-01  | 2024-06-01 | false
abc123       | Moderado     | 2024-06-01  | 9999-12-31 | true

---

## CI/CD — Multi-Ambiente

| Ambiente | Branch | Proteção | Path |
|---|---|---|---|
| dev | develop | Automático | /case-santander/dev |
| hk | main | Revisão obrigatória | /case-santander/hk |
| prod | main | Revisão + 5min | /case-santander/prod |

---

## Docker + Airflow

Orquestração local via Apache Airflow em Docker:
```bash
# Inicializar
docker compose -f docker/docker-compose.yml --env-file docker/.env up airflow-init

# Subir
docker compose -f docker/docker-compose.yml --env-file docker/.env up -d

# Acessar
http://localhost:8080
Login: admin / admin
```

---

## Genie AI

Agente conversacional integrado ao Unity Catalog:
"Quais clientes têm maior risco de fraude?"
"Compare performance das ações por setor"
"Qual ação teve maior queda anormal?"
"Qual o score médio de risco por perfil?"

---

## Estrutura do Repositório
case-santander-data-master/
├── src/
│   ├── config/settings.py
│   ├── ingestion/
│   │   ├── yahoo_finance.py
│   │   ├── bcb.py
│   │   └── world_bank.py
│   ├── transformation/
│   │   ├── silver_acoes.py
│   │   ├── silver_bcb.py
│   │   └── silver_world_bank.py
│   ├── gold/
│   │   ├── anomalias.py
│   │   ├── performance.py
│   │   └── fraude.py
│   ├── clients/
│   │   └── sdc.py
│   └── observability/
│       └── monitoring.py
├── jobs/
│   ├── job_unity_catalog.py
│   ├── job_extracao.py
│   ├── job_silver.py
│   ├── job_gold.py
│   ├── job_observabilidade.py
│   ├── job_streaming.py
│   ├── job_clientes_ordens.py
│   ├── job_corretora_analises.py
│   ├── job_lakehouse_monitoring.py
│   ├── job_sdc.py
│   └── job_carga_sql.py
├── dags/
│   └── dag_pipeline_santander.py
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── tests/
│   └── test_pipeline.py
├── config/
│   └── config.py
├── .github/
│   └── workflows/
│       └── ci-cd.yml
└── README.md

---

## Como Reproduzir

### Pré-requisitos
```bash
pip install -r requirements.txt
```

### Azure

Resource Group: gr-data-master
ADLS Gen2: stcasesantander (bronze, silver, gold)
Databricks Workspace: dbw-case-santander
Service Principal: sp-case-santander
Key Vault: kv-case-santander
Event Hub: evhcasesantander
Azure SQL: sqldb-case-santander
Azure Data Factory: adf-case-santander


### Databricks

Cluster: 15.4 LTS, Standard_D4pds_v6
Secret Scope → Key Vault
Unity Catalog: case_santander
Git folder: case-santander-data-master
Workflow: pipeline-case-santander


### Executar
```bash
# Via Databricks Workflow
pipeline-case-santander → Run now

# Via Airflow local
docker compose -f docker/docker-compose.yml --env-file docker/.env up -d
# Acessar localhost:8080 e ativar a DAG
```

---

## Melhorias Futuras
→ Integração com API oficial da B3
→ Motor de matching de ordens em tempo real
→ Relatório de IR automatizado
→ Azure Monitor + Log Analytics
→ Workspaces separados por ambiente
→ Modelo de ML para previsão de churn
→ Power BI integrado ao SQL Database
→ Azure Container Apps para Airflow em produção
→ Delta Sharing para compartilhamento externo

---

## Autor

**Diego Rodrigues da Silva**  
Data Master 2026