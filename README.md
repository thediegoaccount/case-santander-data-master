# Case Santander — Data Master 2026

## Pipeline de Dados Financeiros — Corretora Santander

![CI/CD](https://github.com/thediegoaccount/case-santander-data-master/actions/workflows/ci-cd.yml/badge.svg)

---

## Objetivo do Case

Desenvolver uma arquitetura de dados completa simulando o pipeline de uma corretora digital, inspirada na Santander Corretora. O projeto contempla ingestão, transformação, análise e governança de dados financeiros reais do mercado brasileiro, com foco em detecção de anomalias, score de risco de clientes e detecção de fraudes.

---

## Arquitetura de Solução
```
[Fontes de Dados]
Yahoo Finance | BCB | World Bank | Kaggle | Event Hub (Streaming)
                          ↓
              [Azure Data Factory]
              Ingestão batch diária (05:00)
                          ↓
              [ADLS Gen2 — Bronze]
              Dados brutos particionados por data
                          ↓
              [Databricks — Silver]
              Limpeza, tipagem, enriquecimento, Delta Lake
                          ↓
              [Databricks — Gold]
              Anomalias, cruzamentos, rankings, fraudes
                          ↓
         ┌────────────────┼────────────────┐
         ↓                ↓                ↓
   Unity Catalog    Azure SQL DB    Dashboard + Genie AI
   (Governança)     (Serving)       (Visualização)
```

---

## Arquitetura Técnica

| Componente | Tecnologia | Função |
|---|---|---|
| Data Lake | Azure ADLS Gen2 | Armazenamento Bronze/Silver/Gold |
| Processamento | Azure Databricks | ETL, ML, anomalias |
| Orquestração Batch | Azure Data Factory | Ingestão agendada |
| Streaming | Azure Event Hub (Kafka) | Transações em tempo real |
| Segurança | Azure Key Vault | Gerenciamento de credenciais |
| Identidade | Service Principal | Autenticação entre serviços |
| Governança | Unity Catalog | Catálogo e controle de acesso |
| Serving | Azure SQL Database | Dados prontos para consumo |
| Versionamento | GitHub + GitHub Actions | CI/CD multi-ambiente |
| Formato | Delta Lake | Camadas Silver e Gold |
| IA | Databricks Genie | Agente de análise conversacional |
| Monitoramento | Lakehouse Monitoring | Qualidade de dados automática |

---

## Fontes de Dados

| Fonte | Dados | Frequência |
|---|---|---|
| Yahoo Finance | Ações B3 (PETR4, VALE3, ITUB4, BBDC4, ABEV3, MGLU3, WEGE3, BBAS3, SANB11) | Diária |
| Banco Central | Selic, Câmbio USD/BRL, IPCA | Diária/Mensal |
| World Bank | PIB anual, Desemprego | Anual |
| Kaggle | 10.000 clientes reais anonimizados (Bank Churn Dataset) | Batch |
| Event Hub | Transações simuladas em streaming | Tempo real |

---

## Pipeline de Dados (Databricks Workflow)
```
t0_unity_catalog_bronze
          ↓
      t1_extracao
    ↓       ↓        ↓
t5_streaming  t6_clientes_ordens
    ↓               ↓
  t2_silver    t7_corretora_analises
         ↓        ↓
           t3_gold
              ↓
        t8_lakehouse_monitoring
              ↓
        t4_observabilidade
```

---

## Unity Catalog
```
case_santander/
├── bronze/
│   ├── acoes       → 4.016 registros
│   ├── bcb         → 3.067 registros
│   ├── world_bank  →    59 registros
│   ├── kafka       →   200 registros
│   ├── clientes    → 10.000 registros
│   └── ordens      →  5.445 registros
├── silver/
│   ├── acoes       → 4.014 registros
│   ├── bcb         → 3.067 registros
│   ├── world_bank  →    59 registros
│   ├── streaming   →   200 registros
│   ├── clientes    → 10.000 registros
│   └── ordens      →  5.445 registros
└── gold/
    ├── performance_acoes      →    24
    ├── anomalias              → 4.014
    ├── acoes_vs_cambio        → 4.014
    ├── observabilidade        →    18
    ├── perfil_clientes        →    45
    ├── ordens_consolidadas    →   893
    ├── ranking_acoes_perfil   →    27
    ├── posicao_clientes       → 3.955
    ├── score_risco_clientes   → 1.000
    └── deteccao_fraude        → 5.445
```

---

## LGPD — Práticas Adotadas

| Campo | Técnica | Descrição |
|---|---|---|
| id_cliente | Pseudonimização | Hash SHA-256 do ID original |
| sobrenome | Mascaramento | Primeira letra + asteriscos |
| cpf | Mascaramento | Primeiros 3 dígitos + mascara |
| email | Mascaramento | 2 chars + *** + domínio |
| Credenciais | Key Vault | Nunca expostas no código |
| Dados analíticos | Separação | Dado analítico != transacional |
| Bronze | Lifecycle Policy | Deletar após 30 dias |

---

## Detecção de Anomalias e Fraudes

### Anomalias de mercado (Z-Score)
```
Z-Score > 2  → Alta Anormal
Z-Score < -2 → Queda Anormal
Taxa: 5.31% (213 de 4.014 registros)
```

### Detecção de fraude por cliente
```
Regra 1: Valor acima do limite operacional
Regra 2: Volume suspeito (quantidade > 9.000)
Regra 3: Preço atípico (> R$90 ou < R$12)
Regra 4: Perfil incompatível com operação

Score: Normal / Médio / Alto / Crítico
Ordens críticas: 302 (6% do total)
```

---

## Score de Risco de Clientes
```
Baixo Risco:    304 clientes → limite R$ 500.000
Risco Moderado: 534 clientes → limite R$ 200.000
Risco Alto:     162 clientes → limite R$  50.000

Fórmula:
score = (score_credito * 0.4) +
        (score_perfil  * 0.2) +
        (score_saldo   * 0.2) +
        (score_comportamento * 0.2)
```

---

## CI/CD — Multi-Ambiente
```
feature/* → develop → hk → main → prod
               ↓        ↓         ↓
             CI/CD   revisão   aprovação
              dev     manual    manual
```

### Ambientes
| Ambiente | Branch | Proteção | Deploy Path |
|---|---|---|---|
| dev | develop | Automático | /case-santander/dev |
| hk | main | Revisão obrigatória | /case-santander/hk |
| prod | main | Revisão + 5min timer | /case-santander/prod |

### Proteção da branch main
```
✅ Pull Request obrigatório
✅ 1 aprovação necessária
✅ CI deve passar antes do merge
✅ Force push bloqueado
```

---

## Observabilidade
```
✅ Logs estruturados (Python logging)
✅ Métricas de qualidade por camada
✅ Alertas automáticos por threshold
✅ Métricas gravadas no Gold
✅ Lakehouse Monitoring (6 tabelas)
✅ Dashboard de qualidade automático
```

---

## Genie AI — Agente Conversacional

Agente de IA integrado ao Unity Catalog que responde perguntas em linguagem natural sobre os dados da corretora:
```
"Quais clientes têm maior risco de fraude?"
"Compare performance das ações por setor"
"Qual ação teve maior queda anormal?"
"Quantos clientes são conservadores?"
"Qual o score médio de risco por perfil?"
```

---

## Estrutura do Repositório
```
case-santander-data-master/
├── notebooks/
│   ├── 01_configuracao_adls.py
│   ├── 02_extracao_dados.py
│   ├── 03_silver_transformacao.py
│   ├── 04_gold_analises.py
│   ├── 05_observabilidade.py
│   ├── 06_kafka_producer.py
│   ├── 07_unity_catalog.py
│   ├── 08_carga_sql.py
│   ├── 09_streaming_consumer.py
│   ├── 10_clientes_ordens.py
│   ├── 11_corretora_analises.py
│   └── 12_lakehouse_monitoring.py
├── tests/
│   └── test_pipeline.py
├── config/
│   └── config.py
├── docs/
│   └── cicd.md
├── .github/
│   └── workflows/
│       └── ci-cd.yml
├── requirements.txt
└── README.md
```

---

## Como Reproduzir

### Pré-requisitos
```bash
pip install -r requirements.txt
```

### Configuração Azure
```
1. Criar Resource Group: gr-data-master
2. Criar Storage Account (ADLS Gen2): stcasesantander
   → Containers: bronze, silver, gold
3. Criar Databricks Workspace: dbw-case-santander
4. Criar Service Principal: sp-case-santander
   → Role: Storage Blob Data Contributor no ADLS
5. Criar Key Vault: kv-case-santander
   → Secrets: client-id, tenant-id, client-secret,
              storage-account, eventhub-connection-string,
              sql-connection-string, kaggle-username, kaggle-key
6. Criar Event Hub: evhcasesantander
   → Event Hub: transacoes-financeiras
7. Criar Azure SQL Database: sqldb-case-santander
8. Criar Azure Data Factory: adf-case-santander
```

### Configuração Databricks
```
1. Criar cluster: cluster-case-santander
   → Runtime: 15.4 LTS
   → Node: Standard_D4pds_v6
   → Auto-termination: 20 min
2. Criar Secret Scope → Key Vault
3. Importar notebooks da pasta /notebooks
4. Criar Unity Catalog: case_santander
5. Criar Workflow: pipeline-case-santander
   → 9 tasks em sequência
   → Agendado: 06:00 AM (Brasília)
```

### Executando os testes
```bash
pytest tests/ -v
```

### Executando o pipeline
```bash
# Via Databricks Workflow
pipeline-case-santander → Run now
```

---

## Resultados

| Camada | Tabela | Registros |
|---|---|---|
| Bronze | acoes | 4.016 |
| Bronze | bcb | 3.067 |
| Bronze | clientes | 10.000 |
| Bronze | ordens | 5.445 |
| Silver | acoes | 4.014 |
| Silver | clientes | 10.000 |
| Gold | anomalias | 4.014 |
| Gold | deteccao_fraude | 5.445 |
| Gold | score_risco_clientes | 1.000 |

---

## Autor

Diego Rodrigues da Silva
Data Master 2026
