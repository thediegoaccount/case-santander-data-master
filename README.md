# Case Santander — Data Master DBX Professional

## Pipeline de Dados Financeiros com Azure e Databricks

![CI/CD](https://github.com/thediegoaccount/case-santander-data-master/actions/workflows/ci-cd.yml/badge.svg)

---

## Objetivo do Case

Desenvolver uma arquitetura de dados completa para ingestão, transformação e análise de dados financeiros do mercado brasileiro, utilizando ferramentas enterprise de engenharia de dados na nuvem Azure.

O pipeline coleta dados reais de ações da B3, indicadores econômicos do Banco Central e World Bank, processa em camadas Bronze/Silver/Gold e detecta anomalias em transações financeiras em tempo real.

---

## Arquitetura de Solução
```
[Fontes de Dados]
Yahoo Finance (B3) | Banco Central (BCB) | World Bank | Event Hub (Streaming)
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
                    Anomalias, cruzamentos, rankings
                              ↓
                    [Observabilidade]
                    Qualidade, alertas, métricas
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
| Versionamento | GitHub + GitHub Actions | CI/CD automatizado |
| Formato | Delta Lake | Camadas Silver e Gold |

---

## Fontes de Dados

| Fonte | Dados | Frequência |
|---|---|---|
| Yahoo Finance | Ações B3 (PETR4, VALE3, ITUB4, BBDC4, ABEV3, MGLU3, WEGE3, BBAS3) | Diária |
| Banco Central | Selic, Câmbio USD/BRL, IPCA | Diária/Mensal |
| World Bank | PIB anual, Desemprego | Anual |
| Event Hub | Transações simuladas em streaming | Tempo real |

---

## Estrutura do Projeto
```
case-santander-data-master/
├── notebooks/
│   ├── 01_configuracao_adls.py
│   ├── 02_extracao_dados.py
│   ├── 03_silver_transformacao.py
│   ├── 04_gold_analises.py
│   ├── 05_observabilidade.py
│   └── 06_kafka_producer.py
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

## Requisitos Cobertos

| Requisito | Solução |
|---|---|
| Extração de dados | Yahoo Finance, BCB, World Bank, Event Hub |
| Ingestão em lote | Azure Data Factory (05:00 diário) |
| Ingestão streaming | Azure Event Hub + Kafka Producer/Consumer |
| Armazenamento | ADLS Gen2 com Medallion Architecture |
| Observabilidade | Logs estruturados, métricas de qualidade, alertas |
| Segurança | Key Vault, Service Principal, TLS 1.2, RBAC |
| Mascaramento | Credenciais nunca expostas no código |
| Arquitetura | Delta Lake, particionamento, Bronze/Silver/Gold |
| Escalabilidade | Databricks auto-scaling, ADLS lifecycle policies |

---

## Como Executar

### Pré-requisitos
```bash
pip install -r requirements.txt
```

### Configuração Azure
1. Criar Resource Group: `gr-data-master`
2. Criar Storage Account com ADLS Gen2: `stcasesantander`
3. Criar containers: `bronze`, `silver`, `gold`
4. Criar Databricks Workspace: `dbw-case-santander`
5. Criar Service Principal: `sp-case-santander`
6. Atribuir role `Storage Blob Data Contributor` no ADLS
7. Criar Key Vault: `kv-case-santander`
8. Adicionar secrets: `client-id`, `tenant-id`, `client-secret`, `storage-account`

### Configuração Databricks
1. Criar cluster: `cluster-case-santander` (15.4 LTS, Standard_D4pds_v6)
2. Criar Secret Scope apontando para o Key Vault
3. Importar notebooks da pasta `/notebooks`
4. Criar Workflow com as 4 tasks em sequência

### Executando o Pipeline
```bash
# Via Databricks Workflow
pipeline-case-santander → Run now

# Via CLI
databricks jobs run-now --job-id <JOB_ID>
```

### Executando os Testes
```bash
pytest tests/ -v
```

---

## Resultados

| Camada | Tabela | Registros |
|---|---|---|
| Bronze | acoes | 4.016 |
| Bronze | bcb | 3.067 |
| Bronze | world_bank | 59 |
| Bronze | kafka | 200 |
| Silver | acoes | 4.014 |
| Silver | bcb | 3.067 |
| Silver | world_bank | 59 |
| Gold | performance_acoes | 24 |
| Gold | anomalias | 213 (5.31%) |
| Gold | acoes_vs_cambio | 4.014 |
| Gold | observabilidade | 6 |

---

## Detecção de Anomalias

O algoritmo de detecção usa **Z-Score** para identificar variações anormais:

- Z-Score > 2 → Alta Anormal
- Z-Score < -2 → Queda Anormal
- Taxa de anomalias: 5.31% (213 de 4.014 registros)

Eventos reais detectados como anomalias:
- Queda do mercado em abril/2025 (tarifas Trump)
- Volatilidade do setor financeiro em dezembro/2025

---

## CI/CD

O pipeline CI/CD é executado automaticamente a cada push na branch `main`:

1. **Integração Contínua** — testes unitários + validação de notebooks
2. **Deploy Contínuo** — publicação automática dos notebooks no Databricks

---

## Melhorias e Considerações Finais

### Melhorias futuras
- Implementar streaming contínuo com Structured Streaming do Spark
- Adicionar camada de serving com Delta Sharing
- Implementar modelo de ML para previsão de preços
- Dashboard interativo com Power BI conectado ao Gold
- Alertas via Teams/Email quando anomalias detectadas
- Implementar Data Quality com Great Expectations

### Considerações
- Credenciais gerenciadas via Azure Key Vault (nunca expostas no código)
- Arquitetura Medallion garante rastreabilidade e qualidade dos dados
- Pipeline reproduzível em qualquer ambiente Azure via scripts documentados
- CI/CD garante qualidade do código a cada alteração

---

## Autor

Diego Rodrigues da Silva
Data Master — DBX Professional
Academia Santander 2026
