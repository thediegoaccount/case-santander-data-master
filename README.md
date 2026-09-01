# Case Santander — Data Master
## Pipeline de Dados Financeiros — Corretora Santander

![CI/CD](https://github.com/thediegoaccount/case-santander-data-master/actions/workflows/ci-cd.yml/badge.svg)

---

## Objetivo do Case

O presente case tem por objetivo desenvolver uma arquitetura de dados completa para
uma corretora digital inspirada na Santander Corretora, cobrindo desde a ingestão de dados
brutos até a entrega de inteligência analítica para tomada de decisão.
O pipeline processa dados reais do mercado financeiro brasileiro — cotações de ações da
B3, indicadores econômicos do Banco Central e do World Bank, dados de clientes e
transações simuladas em tempo real — aplicando as melhores práticas de engenharia de
dados em ambiente de nuvem Azure com Databricks

---

## Requisitos atendidos

| Requisito | Solução implementada |
|---|---|
| Extração de dados | Yahoo Finance, BCB API, World Bank API, Kaggle, Azure Event Hub |
| Ingestão em lote | Azure Data Factory — agendado diariamente às 05:00 |
| Ingestão em streaming | Azure Event Hub (Kafka) + Structured Streaming |
| Armazenamento | Azure ADLS Gen2 — Medallion Architecture (Bronze/Silver/Gold) |
| Observabilidade | Tabela Gold de qualidade + Databricks Lakehouse Monitoring |
| Segurança | Azure Key Vault, Service Principal OAuth2, RBAC |
| Mascaramento LGPD | Hash SHA-256, mascaramento de sobrenome, sem CPF no pipeline |
| Arquitetura escalável | Delta Lake, particionamento, auto-scaling Databricks |
| Governança | Unity Catalog com catálogo, schemas e comentários |
| CI/CD | GitHub Actions — três ambientes: dev, hk, prod |
| Orquestração | Databricks Workflow (produção) + Apache Airflow Docker (dev) |
| Análises financeiras | Anomalias Z-Score, score de risco, detecção de fraude, carteira |
| IA Conversacional | Databricks Genie AI com instruções em português |
| SCD Type 2 | Histórico de mudanças em perfil de risco e score de clientes |

---

## II. Arquitetura de Solução e Arquitetura Técnica

### Visão geral da solução

```

                        FONTES DE DADOS                              
  Yahoo Finance  BCB API  World Bank  Kaggle  Azure Event Hub    

                                  Batch (ADF 05:00)      Streaming (Kafka)
                                                        

                    BRONZE — ADLS Gen2                               
         Dados brutos · Parquet/Delta · Particionado por data        

                                                 Databricks Spark
                                                

                    SILVER — Delta Lake                              
    Limpeza · Tipagem · Enriquecimento · LGPD · SCD Type 2           

                                                 Spark SQL · Window Functions
                                                

                     GOLD — Delta Lake                               
   Anomalias · Fraude · Score Risco · Carteira · Observabilidade     

                                          
                                          
  
   Unity Catalog          Azure SQL Database · Dashboard · Genie AI
   Governança · RBAC      Serving layer para consumo analítico      
  
```

### Arquitetura técnica

| Componente | Tecnologia | Nome no projeto |
|---|---|---|
| Data Lake | Azure ADLS Gen2 | stcasesantander |
| Processamento | Azure Databricks 15.4 LTS | dbw-case-santander |
| Orquestração Batch | Azure Data Factory | adf-case-santander |
| Orquestração Streaming | Azure Event Hub (Kafka Standard) | evhcasesantander |
| Segredos | Azure Key Vault | kv-case-santander |
| Serving | Azure SQL Database | sqldb-case-santander |
| Governança | Unity Catalog | case_santander |
| CI/CD | GitHub Actions | .github/workflows/ci-cd.yml |
| Orquestração local | Apache Airflow + Docker | docker/docker-compose.yml |
| Formato de dados | Delta Lake (Open Format) | Camadas Silver e Gold |
| IA Conversacional | Databricks Genie AI | Space: Corretora Santander |
| Qualidade | Lakehouse Monitoring | 6 tabelas monitoradas |

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

## Fontes de dados

| Fonte | Dados | Volume | Frequência |
|---|---|---|---|
| Yahoo Finance (yfinance) | 9 ações B3: PETR4, VALE3, ITUB4, BBDC4, ABEV3, MGLU3, WEGE3, BBAS3, SANB11 | 8.534 registros | Diária |
| Banco Central — SGS API | Selic (cód. 11), Câmbio USD/BRL (cód. 1), IPCA (cód. 433) | 3.068 registros | Diária/Mensal |
| World Bank API v2 | PIB crescimento anual, Taxa de desemprego Brasil | 59 registros | Anual |
| Kaggle API | 10.000 clientes bancários reais anonimizados (Bank Churn) | 10.000 registros | Carga inicial |
| Azure Event Hub | Transações financeiras simuladas (compra/venda de ações) | 200 eventos/lote | Streaming |

## Unity Catalog — Estrutura de dados

```text
case_santander/
 bronze/
    acoes        → 8.534 reg  · Parquet · Yahoo Finance
    bcb          → 3.068 reg  · Delta   · BCB API
    world_bank   →    59 reg  · Delta   · World Bank API
    kafka        →   200 reg  · Delta   · Azure Event Hub
    clientes     → 10.000 reg · Delta   · Kaggle (LGPD aplicada)
    ordens       →  5.341 reg · Delta   · Simulado Python
 silver/
    acoes        → 8.530 reg  · variação%, empresa, setor
    bcb          → 3.068 reg  · data tipada, 6 casas decimais
    world_bank   →    59 reg  · ano int, mergeSchema
    clientes     → 10.000 reg · faixa_etaria, score_categoria
    clientes_scd → histórico  · SCD Type 2 (perfil_risco, score)
    ordens       →  5.341 reg · data_ordem tipada
    streaming    →   200 reg  · alertas volume/preço (CDC habilitado)
 gold/
      anomalias              → 4.524 reg  · Z-Score por ticker
      performance_acoes      →    27 reg  · por setor/ano
      acoes_vs_cambio        → 4.507 reg  · cruzamento BCB
      posicao_clientes       → 3.931 reg  · P&L, situação
      score_risco_clientes   →  1.000 reg · score ponderado
      score_risco_scd        → histórico  · SCD Type 2
      deteccao_fraude        → 5.341 reg  · 4 regras, Normal→Crítico
      perfil_clientes        →    45 reg  · segmentação
      ordens_consolidadas    →   893 reg  · volume por ticker
      ranking_acoes_perfil   → variável
      fraude_streaming       → variável   · fraude em tempo real (via silver.streaming)
      anomalias_intraday     → variável   · Z-Score intraday (via silver.streaming)
      volume_intraday        → variável   · volume agregado por ticker/hora (via silver.streaming)
      ranking_acoes_realtime → variável   · ranking por volume em tempo real (via silver.streaming)
      observabilidade        → crescente  · métricas de qualidade
```

## Fluxo de orquestração — Databricks Workflow

Fluxo de orquestração — Databricks Workflow

t0_unity_catalog_bronze
             
             
        t1_extracao
                          
               
t5_streaming  t6_clientes_ordens

                            
                            
  t2_silver    t2_silver  t7_corretora_analises
                              
                              
                           t9_scd              
                             
                             
                t2_silver    t3_gold
                              
                   
                                       
             t8_lakehouse   t_sql
             _monitoring        
                             
                   
                        
                  t4_observabilidade

## Orquestração — dois ambientes com papéis distintos

| Orquestrador | Ambiente | Trigger | Propósito |
|---|---|---|---|
| Databricks Workflow | Produção | Agendado 06:00 | Pipeline diário automatizado |
| Apache Airflow + Docker | Desenvolvimento | Manual | Demonstração multi-cloud |

Os jobs Python são os mesmos em ambos — zero duplicação de código. O Airflow chama os mesmos `jobs/*.py` via API REST do Databricks.

## CI/CD — Multi-ambiente

`feature/*` → `develop` → PR → `main`

- `dev`: `/case-santander/dev` — deploy automático
- `hk`: `/case-santander/hk` — revisão
- `prod`: `/case-santander/prod` — revisão + 5 min

Branch `main` protegida:
- PR obrigatório
- 1 aprovação necessária
- CI deve passar
- force push bloqueado


## III. Explicação sobre o Case Desenvolvido

### Pipeline de dados — camada Bronze

A camada Bronze recebe os dados brutos de cada fonte sem transformações, preservando total fidelidade à origem. Os dados são particionados por data de extração e retidos por 30 dias via ADLS Lifecycle Policy.

- Yahoo Finance: coleta 2 anos de histórico das 9 ações B3 monitoradas.
- Banco Central: séries temporais de Selic, Câmbio USD/BRL e IPCA via API SGS. A partição usa `extracao=` (não `data=`) para evitar conflito com a coluna `data`.
- World Bank: indicadores macroeconômicos anuais do Brasil.
- Kaggle: 10.000 registros de clientes bancários reais, pseudonimizados na ingestão (`CustomerId → hash SHA-256`, `Surname → primeira letra + asteriscos`).
- Azure Event Hub: transações financeiras simuladas via Producer Python, persistidas no Bronze via Consumer SDK e processadas posteriormente pelo Structured Streaming do Spark.

### Pipeline de dados — camada Silver

A camada Silver aplica limpeza, tipagem correta, enriquecimento e remoção de duplicatas.

- Ações: cálculo de `variacao_diaria_pct` e `amplitude_diaria`, mapeamento de nome de empresa e setor B3.
- BCB: conversão de datas de `dd/MM/yyyy` para `DateType`.
- World Bank: uso de `mergeSchema: true` para compatibilidade entre execuções.
- `silver.clientes_scd`: implementa SCD Type 2, rastreando mudanças em `perfil_risco`, `score_credito`, `faixa_saldo` e churn com campos `data_inicio`, `data_fim` (`9999-12-31` se atual) e `atual` (boolean).

### Análises financeiras — camada Gold

**Batch (processamento diário):**
- `gold.anomalias`: Z-Score diário por ticker para identificar alta e queda anormal.
- `gold.score_risco_clientes` / `gold.score_risco_scd`: score de risco agregado com pesos 40/20/20/20 e limites de crédito por categoria.
- `gold.deteccao_fraude`: regras batch de fraude usando limite operacional, volume, preço e perfil de cliente.
- `gold.posicao_clientes`: posições líquidas por cliente/ticker, P&L estimado e status da carteira.
- `gold.observabilidade`: métricas de qualidade e monitoramento das tabelas Gold.

**Streaming (derivadas de `silver.streaming` via CDC):**
- `gold.fraude_streaming`: detecção de fraude em tempo real com as mesmas 4 regras do batch, aplicadas sobre transações do Event Hub.
- `gold.anomalias_intraday`: Z-Score calculado por ticker dentro do dia, identificando picos de preço ou volume atípicos em janelas curtas.
- `gold.volume_intraday`: volume total negociado agregado por ticker e hora, base para alertas de liquidez.
- `gold.ranking_acoes_realtime`: ranking de ações por volume intraday em tempo real, enriquecido com dados de performance histórica via Broadcast Join.

---

## SCD Type 2

Implementado para rastrear mudanças históricas em:
- `silver.clientes_scd` → evolução do perfil de risco
- `gold.score_risco_scd` → evolução do score e limite operacional
hash_cliente | perfil_risco | data_inicio | data_fim   | atual
abc123       | Conservador  | 2024-01-01  | 2024-06-01 | false
abc123       | Moderado     | 2024-06-01  | 9999-12-31 | true

---

## Docker + Airflow

O projeto possui **dois orquestradores com papéis distintos** — sem duplicação de execução:

| Orquestrador | Ambiente | Trigger | Propósito |
|---|---|---|---|
| Databricks Workflow | Produção | Agendado 06:00 | Pipeline diário automatizado |
| Airflow + Docker (LocalExecutor) | Desenvolvimento | Manual | Leve, 1 processo, ideal para dev local |
| Airflow + Docker (CeleryExecutor) | Enterprise | Manual/Agendado | 7 containers, 1 por serviço, escalável |

> Em produção, apenas o **Databricks Workflow** é executado automaticamente.
> O **Airflow** demonstra como o pipeline seria orquestrado em um ambiente
> externo ao Databricks — empresa com infraestrutura Airflow ou multi-cloud.
> Os **jobs Python são os mesmos** — zero duplicação de código.

### Desenvolvimento local — LocalExecutor (leve)
```bash
# Inicializar (primeira vez)
docker compose -f docker/docker-compose.yml --env-file docker/.env up airflow-init

# Subir stack completa
docker compose -f docker/docker-compose.yml --env-file docker/.env up -d
```

### Enterprise — CeleryExecutor (7 containers, 1 por serviço)

| Container | Responsabilidade |
|---|---|
| `postgres` | Metadata DB do Airflow |
| `redis` | Message broker — fila Celery |
| `airflow-webserver` | UI apenas — nunca executa tasks |
| `airflow-scheduler` | Orquestra e enfileira — nunca executa tasks |
| `airflow-worker` | Executa as tasks (escalável horizontalmente) |
| `airflow-triggerer` | Operadores assíncronos/deferidos |
| `flower` | Dashboard de monitoramento dos workers (`:5555`) |

```bash
# Inicializar (primeira vez)
docker compose -f docker/docker-compose.prod.yml --env-file docker/.env up airflow-init

# Subir stack enterprise completa
docker compose -f docker/docker-compose.prod.yml --env-file docker/.env up -d

# Escalar workers horizontalmente
docker compose -f docker/docker-compose.prod.yml --env-file docker/.env up -d --scale airflow-worker=3
```

Airflow UI: http://localhost:8080 | Flower (workers): http://localhost:5555

```bash
# Dev local (padrão)

### Uso recorrente
```bash
# Subir (já inicializado anteriormente)
docker compose -f docker/docker-compose.yml --env-file docker/.env up -d

# Verificar status dos containers
docker compose -f docker/docker-compose.yml --env-file docker/.env ps

# Ver logs em tempo real
docker compose -f docker/docker-compose.yml --env-file docker/.env logs -f

# Derrubar a stack
docker compose -f docker/docker-compose.yml --env-file docker/.env down
```

# Acessar
http://localhost:8080
Login: admin / admin
```

### Configurar conexão Databricks no Airflow
```
Admin → Connections → Add Connection:
  Connection Id:   databricks_default
  Connection Type: Databricks
  Host:            https://adb-7405606224366149.9.azuredatabricks.net
  Password:        <token>
```

---

## Genie AI

Agente conversacional integrado ao Unity Catalog. Consultas pré-configuradas:

- "Quais clientes têm maior risco de fraude?"
- "Compare performance das ações por setor"
- "Qual ação teve maior queda anormal?"
- "Qual o score médio de risco por perfil?"

---

## IV. Reprodutibilidade da Arquitetura

### Pré-requisitos

- Conta Azure ativa com permissões para criar recursos
- Databricks Workspace (Standard ou Premium tier)
- Python 3.11+
- Docker Desktop
- Git

### 1. Clonar o repositório

```bash
git clone https://github.com/thediegoaccount/case-santander-data-master.git
cd case-santander-data-master
pip install -r requirements.txt
```

### 2. Criar infraestrutura Azure

Execute os seguintes recursos no Portal Azure ou via CLI:

```bash
# Resource Group
az group create --name gr-data-master --location eastus2

# Storage Account com ADLS Gen2
az storage account create \
     --name stcasesantander \
     --resource-group gr-data-master \
     --location eastus2 \
     --sku Standard_LRS \
     --kind StorageV2 \
     --hierarchical-namespace true

# Containers Bronze, Silver, Gold
az storage container create --name bronze --account-name stcasesantander
az storage container create --name silver --account-name stcasesantander
az storage container create --name gold   --account-name stcasesantander

# Key Vault
az keyvault create \
     --name kv-case-santander \
     --resource-group gr-data-master \
     --location eastus2
```

### 3. Criar Service Principal e configurar permissões

```bash
# Criar Service Principal
az ad sp create-for-rbac --name sp-case-santander --skip-assignment
# Anote: appId (client_id), password (client_secret), tenant (tenant_id)

# Atribuir role no ADLS
az role assignment create \
     --assignee <client_id> \
     --role "Storage Blob Data Contributor" \
     --scope "/subscriptions/<sub_id>/resourceGroups/gr-data-master/providers/Microsoft.Storage/storageAccounts/stcasesantander"
```

### 4. Adicionar segredos no Key Vault

```bash
az keyvault secret set --vault-name kv-case-santander --name client-id       --value "<client_id>"
az keyvault secret set --vault-name kv-case-santander --name tenant-id       --value "<tenant_id>"
az keyvault secret set --vault-name kv-case-santander --name client-secret   --value "<client_secret>"
az keyvault secret set --vault-name kv-case-santander --name storage-account --value "stcasesantander"
az keyvault secret set --vault-name kv-case-santander --name kaggle-username  --value "<seu_usuario_kaggle>"
az keyvault secret set --vault-name kv-case-santander --name kaggle-key       --value "<sua_chave_kaggle>"
```

A chave Kaggle pode ser obtida em: https://www.kaggle.com/settings → API → Create New Token

### 5. Configurar o cluster Databricks

No Databricks Workspace, crie um cluster com as configurações abaixo:

- Nome: cluster-case-santander
- Databricks Runtime: 15.4 LTS (Spark 3.5.0, Scala 2.12)
- Node type: Standard_D4pds_v6
- Auto-termination: 20 minutos

Adicione as seguintes configurações em Advanced Options → Spark Config:

```
spark.databricks.delta.schema.autoMerge.enabled true
spark.hadoop.fs.azure.account.auth.type.stcasesantander.dfs.core.windows.net OAuth
spark.hadoop.fs.azure.account.oauth.provider.type.stcasesantander.dfs.core.windows.net org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
spark.hadoop.fs.azure.account.oauth2.client.id.stcasesantander.dfs.core.windows.net {{secrets/kv-case-santander/client-id}}
spark.hadoop.fs.azure.account.oauth2.client.secret.stcasesantander.dfs.core.windows.net {{secrets/kv-case-santander/client-secret}}
spark.hadoop.fs.azure.account.oauth2.client.endpoint.stcasesantander.dfs.core.windows.net https://login.microsoftonline.com/<tenant_id>/oauth2/token
```

Adicione as bibliotecas em Libraries → Install New → PyPI:

- yfinance
- requests
- azure-eventhub

### 6. Criar Secret Scope no Databricks

Acesse a URL abaixo (substituindo pelo seu host):

https://<seu-workspace>.azuredatabricks.net/#secrets/createScope

Preencha:

- Scope Name: kv-case-santander
- DNS Name: https://kv-case-santander.vault.azure.net/
- Resource ID: <resource_id_do_key_vault>

### 7. Configurar Unity Catalog

Execute no Databricks via notebook SQL:

```sql
CREATE CATALOG IF NOT EXISTS case_santander
COMMENT 'Catalogo principal do case Academia Santander';

CREATE SCHEMA IF NOT EXISTS case_santander.bronze COMMENT 'Dados brutos extraidos das fontes';
CREATE SCHEMA IF NOT EXISTS case_santander.silver COMMENT 'Dados limpos e transformados';
CREATE SCHEMA IF NOT EXISTS case_santander.gold   COMMENT 'Dados prontos para analise e consumo';
```

### 8. Conectar repositório Git ao Databricks

Databricks Workspace → Repos → Add Repo

- URL: https://github.com/thediegoaccount/case-santander-data-master
- Branch: main

### 9. Criar o Databricks Workflow

Jobs e Pipelines → Create Job

Nome: pipeline-case-santander

Tasks (em ordem):

- t0_unity_catalog_bronze → jobs/job_unity_catalog.py (Git)
- t1_extracao_acoes → jobs/job_extracao_acoes.py (Git) depende: t0
- t1_extracao_bcb → jobs/job_extracao_bcb.py (Git) depende: t0
- t1_extracao_world_bank → jobs/job_extracao_world_bank.py (Git) depende: t0
- t5_streaming → jobs/job_streaming.py (Git) depende: t1_extracao_acoes, t1_extracao_bcb, t1_extracao_world_bank
- t6_clientes_ordens → jobs/job_clientes_ordens.py (Git) depende: t1_extracao_acoes, t1_extracao_bcb, t1_extracao_world_bank
- t6_clientes_silver → jobs/job_clientes_silver.py (Git) depende: t6_clientes_ordens
- t2_silver_acoes → jobs/job_silver_acoes.py (Git) depende: t5, t6_clientes_silver
- t2_silver_bcb → jobs/job_silver_bcb.py (Git) depende: t5, t6_clientes_silver
- t2_silver_world_bank → jobs/job_silver_world_bank.py (Git) depende: t5, t6_clientes_silver
- t7_corretora_analises → jobs/job_corretora_analises.py (Git) depende: t6_clientes_silver
- t9_scd → jobs/job_scd.py (Git) depende: t7
- t3_gold → jobs/job_gold.py (Git) depende: t2_silver_acoes, t2_silver_bcb, t2_silver_world_bank, t9
- t10_streaming_gold → jobs/job_streaming_to_gold.py (Git) depende: t3_gold
- t8_lakehouse_monitoring → jobs/job_lakehouse_monitoring.py (Git) depende: t10_streaming_gold
- t_sql → jobs/job_carga_sql.py (Git) depende: t10_streaming_gold
- t4_observabilidade → jobs/job_observabilidade.py (Git) depende: t8_lakehouse_monitoring, t_sql
- t_sql → jobs/job_carga_sql.py (Git) depende: t3
- t4_observabilidade → jobs/job_observabilidade.py (Git) depende: t8, t_sql

Agendamento: 0 6 * * * (06:00, America/Sao_Paulo)

Padrão obrigatório em todos os jobs: cada jobs/*.py inicia com:

```python
import sys
# Setup do sys.path
from src.config.environment import setup_python_path
setup_python_path()
```

### 10. Executar o pipeline

```bash
# Via Databricks CLI
databricks jobs run-now --job-id <job_id>

# Via UI
pipeline-case-santander → Run now
```

### 11. Configurar Databricks Connect (desenvolvimento local)

```bash
# Criar ambiente virtual
python3 -m venv ~/.venv/databricks
source ~/.venv/databricks/bin/activate

# Instalar dependências
pip install -r requirements.txt

# Configurar
databricks configure --token
# Host: https://<workspace>.azuredatabricks.net
# Token: <seu_token>

echo "cluster_id = <cluster_id>" >> ~/.databrickscfg

# Testar
databricks-connect test

# Executar testes
pytest tests/ -v
```

### 12. Configurar Apache Airflow com Docker

Crie o arquivo docker/.env:

```bash
cat > docker/.env << 'EOF'
DATABRICKS_HOST=https://<seu-workspace>.azuredatabricks.net
DATABRICKS_TOKEN=<seu_token>
EOF
```

Execute:

```bash
# Primeira vez — inicializar banco e criar usuário admin
docker compose -f docker/docker-compose.yml --env-file docker/.env up airflow-init

# Subir a stack completa
docker compose -f docker/docker-compose.yml --env-file docker/.env up -d

# Acessar
# http://localhost:8080
# Login: admin / admin
```

Após acessar, configure a conexão Databricks:

Admin → Connections → Add Connection

- Connection Id: databricks_default
- Connection Type: Databricks
- Host: https://<seu-workspace>.azuredatabricks.net
- Password: <seu_token>

Comandos de uso recorrente:

```bash
# Verificar status
docker compose -f docker/docker-compose.yml --env-file docker/.env ps

# Ver logs
docker compose -f docker/docker-compose.yml --env-file docker/.env logs -f

# Derrubar
docker compose -f docker/docker-compose.yml --env-file docker/.env down
```

### 13. Configurar CI/CD no GitHub

Crie três ambientes no repositório GitHub:

Settings → Environments → New environment

Ambiente: dev

- Secrets: DATABRICKS_HOST, DATABRICKS_TOKEN
- Variables: DEPLOY_PATH = /case-santander/dev

Ambiente: hk

- Secrets: DATABRICKS_HOST, DATABRICKS_TOKEN
- Variables: DEPLOY_PATH = /case-santander/hk
- Protection: Required reviewers

Ambiente: prod

- Secrets: DATABRICKS_HOST, DATABRICKS_TOKEN
- Variables: DEPLOY_PATH = /case-santander/prod
- Protection: Required reviewers + Wait timer 5 min

O CI/CD dispara automaticamente a cada push nas branches develop (→ dev) e main (→ hk e prod).

### Estrutura do Repositório

```
case-santander-data-master/
 .github/
    workflows/
        ci-cd.yml
 config/
    config.py
 dags/
    dag_pipeline_santander.py
 docs/
    technical-reference.md
    unity-catalog.md
 docker/
    Dockerfile
    docker-compose.yml
 jobs/
    job_unity_catalog.py
    job_extracao.py
    job_streaming.py
    job_clientes_ordens.py
    job_silver.py
    job_corretora_analises.py
    job_scd.py
    job_gold.py
    job_lakehouse_monitoring.py
    job_carga_sql.py
    job_observabilidade.py
 requirements-airflow.txt
 requirements.txt
 src/
    config/
       settings.py
    ingestion/
       bcb.py
       world_bank.py
       yahoo_finance.py
    gold/
       anomalias.py
       fraude.py
       performance.py
    clients/
       scd.py
    observability/
       monitoring.py
    transformation/
        silver_acoes.py
        silver_bcb.py
        silver_world_bank.py
 tests/
      test_pipeline.py

### Dependências

**requirements.txt**

- yfinance>=0.2.37
- requests>=2.31.0
- azure-eventhub>=5.15.1
- databricks-connect==15.4
- databricks-sdk>=0.20.0
- pytest>=7.4.0

**requirements-airflow.txt** (usado no Dockerfile)

- apache-airflow-providers-databricks==4.7.0
- databricks-sdk>=0.20.0

---

## Boas Práticas de Engenharia de Dados

| Prática | Onde | Benefício |
|---|---|---|
| **Auto Loader** (`cloudFiles`) | `job_streaming.py` | Rastreamento incremental de arquivos no ADLS com schema evolution automática; escala para bilhões de arquivos sem listar diretório |
| **Liquid Clustering** | `job_unity_catalog.py` | Substitui `partitionBy` estático por clustering dinâmico — Databricks reorganiza layout dos arquivos incrementalmente via OPTIMIZE, sem reescrita da tabela |
| **OPTIMIZE + ZORDER + VACUUM** | `job_observabilidade.py` | Compacta small files e reordena dados pelas colunas de filtro mais usadas (data skipping no Photon); VACUUM remove versões além de 7 dias de time travel |
| **Delta Change Data Feed (CDC)** | `job_unity_catalog.py` → `job_streaming_to_gold.py` | Rastreamento de mudanças a nível de linha (insert/update/delete) em `silver.streaming`, `silver.ordens` e `silver.clientes`; leitura incremental por versão evita full scan diário |
| **Broadcast Join** | `streaming_gold.py`, `fraude.py`, `job_corretora_analises.py` | `F.broadcast()` em tabelas pequenas (9 linhas `df_perf`, <1 MB `df_score` e `df_clientes`) elimina sort-merge shuffle distribuindo a tabela inteira em cada executor |

---

## Melhorias Futuras
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