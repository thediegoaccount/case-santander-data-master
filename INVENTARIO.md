# INVENTÁRIO — Fase de Reconhecimento (CONTRATO)

> Documento de engenharia reversa. **Só contém o que o código comprova**, com
> `arquivo:linha`. README, docs/ e wiki NÃO foram usados como fonte — apenas
> como termo de comparação para registrar divergências (seção 9).
>
> Este arquivo é o **contrato** para A1, A2, A3 e para o revisor. Os números
> aqui valem os da coluna "REAIS" da seção 7.

---

## 0. PREMISSAS DO PROMPT QUE NÃO SE APLICARAM

Registrado no topo porque muda a leitura de todas as seções seguintes.

| Premissa do prompt | Situação real | Como foi tratado |
|---|---|---|
| "Repositórios" (plural), multi-repo | **Um único repositório.** `git remote -v` → apenas `origin https://github.com/thediegoaccount/case-santander-data-master.git` | Seção 1 tem uma única ficha. Seção 5 registra dependências **entre módulos**, não entre repos. |
| Branch `master` | Branch atual é **`release/segunda-chance-dm`**. Não existe branch `master` (nem local nem remota); o default remoto é `origin/main` | Inventário levantado em `release/segunda-chance-dm`. Divergência registrada em 9.1. |
| Control-M com `.yaml/.yml` de job | **Não existe Control-M.** A orquestração é **Databricks Asset Bundles** (`databricks.yml`) + um DAG Airflow **gerado** a partir dele | Seção 3 lê `databricks.yml` como fonte da ordem de execução, no papel que o prompt atribui ao Control-M. Mapeamento de vocabulário na seção 3.0. |
| Entrypoint parametrizado `app.py` | **Não existe `app.py`** (`find . -name app.py` → vazio) | A parametrização de tabelas está em `src/config/tables.py` + `src/config/environment.py`. Os entrypoints executáveis são os 22 `console_scripts` de `setup.py`. Seção 2 mapeia essa camada. |
| Tópicos/filas | Existe Azure Event Hub, mas **nenhum consumidor lê do Event Hub diretamente** — a ingestão é por Auto Loader sobre arquivos Avro do Event Hub Capture | Registrado em 4.5 com a ressalva. |
| Campos de tabelas finais consumidas por BI/API | **Não há API nem BI conectado ao lakehouse.** Os dashboards Grafana apontam para o Postgres do Airflow, não para as tabelas | Seção 6 usa como "consumo externo" o notebook de apresentação + Unity Catalog. Critério explicitado em 6.0. |

---

## 1. FICHA DO REPOSITÓRIO

### 1.1 Identificação e procedência

| Item | Valor |
|---|---|
| Caminho | `C:\Users\thedi\OneDrive\Desktop\GIT\case-santander-data-master` |
| Remote | `https://github.com/thediegoaccount/case-santander-data-master.git` |
| **Branch lida** (`git rev-parse --abbrev-ref HEAD`) | **`release/segunda-chance-dm`** |
| **Commit lido** (`git rev-parse --short HEAD`) | **`6394bf3`** |
| Commit completo | `6394bf3a4eb7fd2e9f1059053fcae832f6e9bb14` |
| Data / autor | `Wed Sep 2 23:02:03 2026 -0300` — `thediegoaccount` |
| Mensagem | `docs: pacote portatil com multi-repo, Control-M, varredura e Master` |
| Working tree | **NÃO limpo** — `M README.md`, `M docs/README.md`, `?? EXECUTIVE_SUMMARY.md`. Nenhum arquivo de **código** modificado; as divergências abaixo são todas sobre o commit `6394bf3`. |

### 1.2 Papel do repositório

O repo é **monolítico e cobre as três funções** — não há separação ingestão /
transformação / publicação em repos distintos:

- **Ingestão** — `src/ingestion/` (Yahoo Finance, BCB, World Bank) e
  `jobs/job_clientes_ordens.py` (Kaggle) gravam Parquet/Delta no ADLS Bronze.
- **Transformação** — `src/transformation/` (Bronze→Silver) e `src/gold/` +
  `jobs/job_corretora_analises.py` (Silver→Gold), arquitetura medallion.
- **Publicação** — registro das tabelas no Unity Catalog
  (`src/config/tables.py:48`, `jobs/job_unity_catalog.py`), Liquid Clustering,
  Change Data Feed e Lakehouse Monitoring.
- **Infra** — `terraform/` provisiona Azure (Storage, Key Vault, Event Hub,
  Databricks workspace, Unity Catalog).
- **Orquestração** — `databricks.yml` (fonte) e `dags/` (Airflow, derivado).

### 1.3 Linguagens

| Linguagem | Onde | Arquivos |
|---|---|---|
| Python 3.11+ (`setup.py:21`) | `src/`, `jobs/`, `scripts/`, `dags/`, `tests/`, `notebooks/` | 66 `.py` |
| HCL / Terraform | `terraform/` | 25 `.tf` |
| YAML | `databricks.yml`, `.github/workflows/`, `docker/` | 9 |
| Dockerfile | `docker/` | 2 |
| SQL | embutido em f-strings Python e células `%sql` do notebook | — |

### 1.4 Entrypoints e como se executa

**Não há `app.py`.** Os entrypoints são os 22 `console_scripts` declarados em
`setup.py:25-50`, resolvidos pelos `python_wheel_task` de `databricks.yml`.

Execução:

```bash
# 1. Build do wheel (declarado em databricks.yml:29-33)
python setup.py bdist_wheel

# 2. Validar e implantar o bundle (.github/workflows/ci-cd.yml:87-96 / 129-138)
databricks bundle validate -t hk      # ou -t prod
databricks bundle deploy   -t hk --force

# 3. Job isolado
databricks bundle run pipeline_completo -t hk

# 4. Testes locais
pytest tests/ -v                      # .github/workflows/ci-cd.yml:49
```

Variável de ambiente obrigatória: `ENVIRONMENT` (`hk` | `prod`), lida em
`src/config/environment.py:54`; default `hk`. `prod` exige adicionalmente
`CONFIRM_PRODUCTION=true` (`src/config/environment.py:171-176`) — mas ver
divergência 9.6, essa validação não é chamada por nenhum job.

---

## 2. MAPA DE PARAMETRIZAÇÃO DE TABELAS

**Não existe `app.py`.** A camada equivalente — a que resolve nome de variável
para `schema.tabela` real — é:

- `src/config/environment.py:84-115` — configs por ambiente (`catalog`, `schema_prefix`)
- `src/config/tables.py:24-32` — resolve os símbolos na **importação do módulo**
- `src/config/environment.py:120-156` — `get_paths()`, paths ADLS + alguns FQNs

### 2.1 Raiz da resolução

| Símbolo | Definição | Valor `hk` | Valor `prod` | Valor `dev` |
|---|---|---|---|---|
| `ENVIRONMENT` (env var) | `src/config/environment.py:54` | `hk` (default) | `prod` | **inexistente** |
| `CATALOG` | `src/config/tables.py:24` ← `environment.py:87,102` | `case_santander` | `case_santander` | — |
| `SCHEMA_PREFIX` | `src/config/tables.py:25` ← `environment.py:88,103` | `hk_` | `prod_` | — |
| `SCHEMA_BRONZE` | `src/config/tables.py:28` | `case_santander.hk_bronze` | `case_santander.prod_bronze` | — |
| `SCHEMA_SILVER` | `src/config/tables.py:29` | `case_santander.hk_silver` | `case_santander.prod_silver` | — |
| `SCHEMA_GOLD` | `src/config/tables.py:30` | `case_santander.hk_gold` | `case_santander.prod_gold` | — |

> **Só existem DOIS ambientes**, não três. `src/config/environment.py:13`
> declara `ENVIRONMENTS = ["hk", "prod"]` e `:56` levanta `ValueError` para
> qualquer outro valor. Não há `dev`. `hk` = homologação.

Helpers de resolução: `schema_fqn(layer)` — `src/config/tables.py:35`;
`table_fqn(layer, name)` — `:43`; `register_external_table(spark, layer, name, path)` — `:48`.

### 2.2 Mapa completo parâmetro → nome real

Toda referência a tabela no código executável usa `{SCHEMA_*}.<nome>`. A tabela
abaixo resolve **todas**, nos dois ambientes.

#### Camada Bronze

| Referência no código | `hk` | `prod` | Escrita em | Leitura em |
|---|---|---|---|---|
| `{SCHEMA_BRONZE}.acoes` | `case_santander.hk_bronze.acoes` | `case_santander.prod_bronze.acoes` | `jobs/job_unity_catalog.py:71` (loop `:43`) | `notebooks/case_presentation.py:163` |
| `{SCHEMA_BRONZE}.bcb` | `case_santander.hk_bronze.bcb` | `case_santander.prod_bronze.bcb` | `jobs/job_unity_catalog.py:71` (loop `:44`) | `notebooks/case_presentation.py:164` |
| `{SCHEMA_BRONZE}.world_bank` | `case_santander.hk_bronze.world_bank` | `case_santander.prod_bronze.world_bank` | `jobs/job_unity_catalog.py:71` (loop `:45`) | `notebooks/case_presentation.py:165` |
| `{SCHEMA_BRONZE}.kafka` | `case_santander.hk_bronze.kafka` | `case_santander.prod_bronze.kafka` | `jobs/job_unity_catalog.py:71` (loop `:46`) | `notebooks/case_presentation.py:168` |
| `{SCHEMA_BRONZE}.clientes` | `case_santander.hk_bronze.clientes` | `case_santander.prod_bronze.clientes` | `jobs/job_clientes_ordens.py:135,156` | `jobs/job_clientes_ordens.py:231`; `jobs/job_clientes_silver.py:38` |
| `{SCHEMA_BRONZE}.ordens` | `case_santander.hk_bronze.ordens` | `case_santander.prod_bronze.ordens` | `jobs/job_clientes_ordens.py:204,225` | `jobs/job_clientes_ordens.py:271`; `jobs/job_clientes_silver.py:57` |

#### Camada Silver

| Referência no código | `hk` | `prod` | Escrita em | Leitura em |
|---|---|---|---|---|
| `{SCHEMA_SILVER}.acoes` | `case_santander.hk_silver.acoes` | `case_santander.prod_silver.acoes` | `src/transformation/silver_acoes.py:71` (path) + registro `:89` | `src/gold/correlacao_acoes_cambio.py:31`; `src/observability/monitoring.py:91` |
| `{SCHEMA_SILVER}.bcb` | `case_santander.hk_silver.bcb` | `case_santander.prod_silver.bcb` | `src/transformation/silver_bcb.py:41` + registro `:58` | `src/gold/bcb_analise.py:24`; `src/gold/correlacao_acoes_cambio.py:36` |
| `{SCHEMA_SILVER}.world_bank` | `case_santander.hk_silver.world_bank` | `case_santander.prod_silver.world_bank` | `src/transformation/silver_world_bank.py:38` + registro `:55` | `src/gold/world_bank_analise.py:24` |
| `{SCHEMA_SILVER}.clientes` | `case_santander.hk_silver.clientes` | `case_santander.prod_silver.clientes` | `jobs/job_clientes_ordens.py:243,265` | `jobs/job_corretora_analises.py:36`; `src/clients/scd.py:79` |
| `{SCHEMA_SILVER}.ordens` | `case_santander.hk_silver.ordens` | `case_santander.prod_silver.ordens` | `jobs/job_clientes_ordens.py:277,299` | `src/gold/fraude.py:22`; `jobs/job_corretora_analises.py:32,147,162` |
| `{SCHEMA_SILVER}.streaming` | `case_santander.hk_silver.streaming` | `case_santander.prod_silver.streaming` | `jobs/job_streaming_continuous.py:113` | `src/gold/streaming_gold.py:37,113,182,253` |
| `{SCHEMA_SILVER}.clientes_scd` | `case_santander.hk_silver.clientes_scd` | `case_santander.prod_silver.clientes_scd` | `src/clients/scd.py:82` | `src/clients/scd.py:86`; `notebooks/case_presentation.py:852` |

#### Camada Gold

| Referência no código | `hk` | `prod` | Escrita em |
|---|---|---|---|
| `{SCHEMA_GOLD}.anomalias` | `case_santander.hk_gold.anomalias` | `case_santander.prod_gold.anomalias` | `src/gold/anomalias.py:45` (path) + registro `jobs/job_unity_catalog.py:113` |
| `{SCHEMA_GOLD}.performance_acoes` | `case_santander.hk_gold.performance_acoes` | `case_santander.prod_gold.performance_acoes` | `src/gold/performance.py:45` (path) + registro `jobs/job_unity_catalog.py:113` |
| `{SCHEMA_GOLD}.indicadores_bcb` | `case_santander.hk_gold.indicadores_bcb` | `case_santander.prod_gold.indicadores_bcb` | `src/gold/bcb_analise.py:70-71` |
| `{SCHEMA_GOLD}.contexto_macroeconomico` | `case_santander.hk_gold.contexto_macroeconomico` | `case_santander.prod_gold.contexto_macroeconomico` | `src/gold/world_bank_analise.py:93-94` |
| `{SCHEMA_GOLD}.acoes_vs_cambio` | `case_santander.hk_gold.acoes_vs_cambio` | `case_santander.prod_gold.acoes_vs_cambio` | `src/gold/correlacao_acoes_cambio.py:112-113` |
| `{SCHEMA_GOLD}.deteccao_fraude` | `case_santander.hk_gold.deteccao_fraude` | `case_santander.prod_gold.deteccao_fraude` | `src/gold/fraude.py:85` |
| `{SCHEMA_GOLD}.fraude_streaming` | `case_santander.hk_gold.fraude_streaming` | `case_santander.prod_gold.fraude_streaming` | `src/gold/streaming_gold.py:93` |
| `{SCHEMA_GOLD}.anomalias_intraday` | `case_santander.hk_gold.anomalias_intraday` | `case_santander.prod_gold.anomalias_intraday` | `src/gold/streaming_gold.py:159` |
| `{SCHEMA_GOLD}.volume_intraday` | `case_santander.hk_gold.volume_intraday` | `case_santander.prod_gold.volume_intraday` | `src/gold/streaming_gold.py:231` |
| `{SCHEMA_GOLD}.ranking_acoes_realtime` | `case_santander.hk_gold.ranking_acoes_realtime` | `case_santander.prod_gold.ranking_acoes_realtime` | `src/gold/streaming_gold.py:299` |
| `{SCHEMA_GOLD}.posicao_clientes` | `case_santander.hk_gold.posicao_clientes` | `case_santander.prod_gold.posicao_clientes` | `jobs/job_corretora_analises.py:65` |
| `{SCHEMA_GOLD}.score_risco_clientes` | `case_santander.hk_gold.score_risco_clientes` | `case_santander.prod_gold.score_risco_clientes` | `jobs/job_corretora_analises.py:118` |
| `{SCHEMA_GOLD}.perfil_clientes` | `case_santander.hk_gold.perfil_clientes` | `case_santander.prod_gold.perfil_clientes` | `jobs/job_corretora_analises.py:137` |
| `{SCHEMA_GOLD}.ordens_consolidadas` | `case_santander.hk_gold.ordens_consolidadas` | `case_santander.prod_gold.ordens_consolidadas` | `jobs/job_corretora_analises.py:153` |
| `{SCHEMA_GOLD}.ranking_acoes_perfil` | `case_santander.hk_gold.ranking_acoes_perfil` | `case_santander.prod_gold.ranking_acoes_perfil` | `jobs/job_corretora_analises.py:169` |
| `{SCHEMA_GOLD}.observabilidade` | `case_santander.hk_gold.observabilidade` | `case_santander.prod_gold.observabilidade` | `jobs/job_observabilidade.py:37` |
| `{SCHEMA_GOLD}.score_risco_scd` | `case_santander.hk_gold.score_risco_scd` | `case_santander.prod_gold.score_risco_scd` | `src/clients/scd.py:108` |

### 2.3 Parâmetros de infraestrutura (não-tabela)

Todos com override por env var — `src/config/environment.py:77-82`:

| Parâmetro | Env var de override | `hk` (default) | `prod` (default) |
|---|---|---|---|
| `storage_account` | `STORAGE_ACCOUNT` | `stcasesantanderhk` | `stcasesantanderprod` |
| `key_vault` (= secret scope) | `KEY_VAULT_NAME` | `kv-case-santander-hk` | `kv-case-santander-prod` |
| `eventhub_ns` | `EVENTHUB_NAMESPACE` | `evhcasesantander-hk` | `evhcasesantander-prod` |
| `eventhub_name` | `EVENTHUB_NAME` | `transacoes-financeiras-hk` | `transacoes-financeiras-prod` |
| `databricks_workspace` | `DATABRICKS_HOST_HK` / `DATABRICKS_HOST_PROD` | `""` | `""` |
| `enable_streaming` | — | `False` (`:97`) | `True` (`:112`) |
| `data_retention_days` | — | `30` (`:96`) | `90` (`:111`) |
| `is_production` | — | `False` (`:98`) | `True` (`:113`) |

Variáveis do bundle — `databricks.yml:6-25`, override por target `:717-749`:

| Variável | default | `hk` | `prod` |
|---|---|---|---|
| `spark_version` | `14.3.x-scala2.12` | `14.3.x-scala2.12` | `14.3.x-scala2.12` |
| `gold_workers` | `2` | `2` | `4` |
| `sql_workers` | `1` | `1` | `2` |
| `environment` | `hk` | `hk` | `prod` |
| `enable_streaming` | `false` | `false` | `true` |

### 2.4 Parâmetros [NÃO RESOLVIDOS]

| Parâmetro | Local de uso | Por que não resolve |
|---|---|---|
| `enable_streaming` (bundle) | `databricks.yml:23-25`, `:721-726`, `:745-748` | Declarada e definida nos dois targets, mas **nenhuma task, `condition_task` ou bloco a consome**. `grep "var.enable_streaming" databricks.yml` → 0 ocorrências. O comentário `:250-251` afirma "Condicionado à variável enable_streaming", mas não há condicional. O único freio real de streaming em `hk` é o `pause_status: PAUSED` de `:734-738` — que cobre apenas `streaming_to_gold_continuous`, não `streaming_continuous`. |
| Colunas de pivot de `gold.indicadores_bcb` | `src/gold/bcb_analise.py:28-32` | `.pivot("indicador")` sem lista explícita → as colunas dependem dos dados. **Resolvidas por inferência** a partir de `src/ingestion/bcb.py:158,161,164`: `selic`, `cambio_usd_brl`, `ipca`. Referenciadas nominalmente em `:43,47,53`, o que confirma. Marcado como inferência, não como leitura direta de schema. |
| Colunas de pivot de `gold.contexto_macroeconomico` | `src/gold/world_bank_analise.py:28-32` | Idem. Inferidas de `src/ingestion/world_bank.py:80-81`: `pib_anual`, `desemprego`. Confirmadas pelo uso em `:40,45`. |
| `STORAGE_ACCOUNT_HK`, `KEY_VAULT_HK`, `EVENTHUB_NS_HK`, `SQL_SERVER_HK` (e `_PROD`) | `.env.example:19-32` | **Nenhuma é lida pelo código.** `os.getenv` em `environment.py:78-81` procura `STORAGE_ACCOUNT`, `KEY_VAULT_NAME`, `EVENTHUB_NAMESPACE`, `EVENTHUB_NAME` — sem sufixo. Ver divergência 9.3. |

---

## 3. ORQUESTRAÇÃO — DEFINIÇÕES DE JOB

### 3.0 Mapeamento de vocabulário

Não há Control-M. Equivalências assumidas:

| Conceito Control-M | Equivalente neste repo |
|---|---|
| Job / folder | `resources.jobs.<id>` em `databricks.yml` |
| Script executado | `python_wheel_task.entry_point` → `setup.py:25-50` → `jobs/*.py` |
| Calendário / cron | `schedule.quartz_cron_expression` |
| IN / OUT condition | `depends_on[].task_key` (dependência intra-job; grafo de tasks) |
| Recurso / lock | Não há. Cada task sobe seu próprio **job cluster** efêmero (`new_cluster`) |

**Fonte da ordem de execução: `databricks.yml`.** `dags/dag_pipeline_santander.py`
é **gerado** a partir dele (`dags/dag_pipeline_santander.py:1-6` declara
"NÃO EDITE MANUALMENTE"; gerador em `scripts/sync_airflow_from_databricks.py`,
disparado por `.github/workflows/ci-cd.yml:162-173`). O grafo de ambos foi
comparado e **é idêntico** (ver 3.4).

### 3.1 Jobs definidos — 9 no total

| # | Job (id) | Nome | Schedule | Tasks |
|---|---|---|---|---|
| 1 | `pipeline_completo` | `[${var.environment}] Pipeline Completo - Corretora Santander` | `0 0 6 * * ?` America/Sao_Paulo, `UNPAUSED` (`:712-715`) | 19 |
| 2 | `streaming_continuous` | `[${var.environment}] Streaming Contínuo` | **sem schedule** — serviço 24/7 (`:250-251`) | 1 |
| 3 | `streaming_to_gold_continuous` | `[${var.environment}] Streaming to Gold (Agendado)` | `0 */5 * * * ?` America/Sao_Paulo, `UNPAUSED` (`:281-284`); **`PAUSED` em `hk`** (`:734-738`) | 1 |
| 4 | `t3_gold_anomalias` | `[…] t3 - Gold Anomalias` | sem schedule (`:37`) | 1 |
| 5 | `t3_gold_performance` | `[…] t3 - Gold Performance Ações` | sem schedule | 1 |
| 6 | `t3_gold_bcb` | `[…] t3 - Gold BCB Indicadores` | sem schedule | 1 |
| 7 | `t3_gold_world_bank` | `[…] t3 - Gold World Bank Contexto` | sem schedule | 1 |
| 8 | `t3_gold_acoes_cambio` | `[…] t3 - Gold Ações vs Câmbio` | sem schedule | 1 |
| 9 | `t3_gold_fraude` | `[…] t3 - Gold Fraude` | sem schedule | 1 |

Jobs 4–9 são **duplicatas avulsas** das tasks `t3_*` que já existem dentro de
`pipeline_completo` — mesmo `entry_point`, sem schedule, sem predecessor. Ver
risco 9.5.

### 3.2 `pipeline_completo` — grafo completo (fonte da ORDEM REAL)

Cada linha: task, entry_point, predecessores (IN conditions) e sucessores (OUT).

| Task (`task_key`) | Linha | `entry_point` | Predecessores (IN) | Sucessores (OUT) |
|---|---|---|---|---|
| `t0_unity_catalog` | `:297` | `job_unity_catalog_schemas` | — (raiz) | `t1_extracao_acoes`, `t1_extracao_bcb`, `t1_extracao_world_bank`, `t6_clientes_ordens` |
| `t1_extracao_acoes` | `:317` | `job_extracao_acoes` | `t0_unity_catalog` | `t2_silver_acoes` |
| `t1_extracao_bcb` | `:338` | `job_extracao_bcb` | `t0_unity_catalog` | `t2_silver_bcb` |
| `t1_extracao_world_bank` | `:359` | `job_extracao_world_bank` | `t0_unity_catalog` | `t2_silver_world_bank` |
| `t6_clientes_ordens` | `:380` | `job_clientes_ordens` | `t0_unity_catalog` | `t7_corretora_analises` |
| `t2_silver_acoes` | `:402` | `job_silver_acoes` | `t1_extracao_acoes` | `t3_anomalias`, `t3_performance`, `t3_acoes_cambio` |
| `t2_silver_bcb` | `:423` | `job_silver_bcb` | `t1_extracao_bcb` | `t3_bcb`, `t3_acoes_cambio` |
| `t2_silver_world_bank` | `:444` | `job_silver_world_bank` | `t1_extracao_world_bank` | `t3_world_bank` |
| `t3_anomalias` | `:465` | `job_gold_anomalias` | `t2_silver_acoes` | `t8_lakehouse_monitoring` |
| `t3_performance` | `:486` | `job_gold_performance` | `t2_silver_acoes` | `t8_lakehouse_monitoring` |
| `t3_bcb` | `:507` | `job_gold_bcb` | `t2_silver_bcb` | `t8_lakehouse_monitoring` |
| `t3_world_bank` | `:528` | `job_gold_world_bank` | `t2_silver_world_bank` | `t8_lakehouse_monitoring` |
| `t3_acoes_cambio` | `:549` | `job_gold_acoes_vs_cambio` | `t2_silver_acoes` **E** `t2_silver_bcb` | `t8_lakehouse_monitoring` |
| `t7_corretora_analises` | `:572` | `job_corretora_analises` | `t6_clientes_ordens` | `t9_scd`, `t3_fraude` |
| `t9_scd` | `:593` | `job_scd` | `t7_corretora_analises` | `t8_lakehouse_monitoring` |
| `t3_fraude` | `:614` | `job_gold_fraude` | `t7_corretora_analises` | `t8_lakehouse_monitoring` |
| `t8_lakehouse_monitoring` | `:636` | `job_lakehouse_monitoring` | `t3_anomalias`, `t3_performance`, `t3_bcb`, `t3_world_bank`, `t3_acoes_cambio`, `t3_fraude`, `t9_scd` (7) | `t8b_uc_registro` |
| `t8b_uc_registro` | `:665` | `job_unity_catalog` | `t8_lakehouse_monitoring` | `t4_observabilidade` |
| `t4_observabilidade` | `:688` | `job_observabilidade` | `t8b_uc_registro` | — (folha) |

> **19 tasks**, verificado: `grep -c "^      - task_key:" databricks.yml` → 19
> (`task_key` a 6 espaços = definição de task; a 10 espaços = entrada de
> `depends_on`, que são 25 e representam as arestas, não tasks).
> As 19 linhas da tabela acima são as 19 tasks, sem repetição.
>
> **25 arestas de dependência**, verificado:
> `grep -c "^          - task_key:" databricks.yml` → 25. O DAG Airflow expressa
> as mesmas 25 arestas em 18 linhas `>>` (`:168-185`), porque `:179` declara 2
> arestas e `:183` declara 7 — não confundir contagem de linhas com contagem de
> arestas.

**Ordem topológica real (5 níveis):**
```
N0: t0_unity_catalog
N1: t1_extracao_acoes │ t1_extracao_bcb │ t1_extracao_world_bank │ t6_clientes_ordens
N2: t2_silver_acoes │ t2_silver_bcb │ t2_silver_world_bank │ t7_corretora_analises
N3: t3_anomalias │ t3_performance │ t3_bcb │ t3_world_bank │ t3_acoes_cambio │ t3_fraude │ t9_scd
N4: t8_lakehouse_monitoring → t8b_uc_registro → t4_observabilidade
```

### 3.3 Recursos, timeouts, retries

Não há recursos nomeados nem locks. Cada task cria seu próprio cluster efêmero:

| Atributo | Valor | Onde |
|---|---|---|
| `spark_version` | `${var.spark_version}` = `14.3.x-scala2.12` | `databricks.yml:8-9` |
| `node_type_id` | `Standard_DS3_v2` | todas as tasks |
| `num_workers` | `${var.gold_workers}` (2 hk / 4 prod) ou `${var.sql_workers}` (1 hk / 2 prod) | por task |
| `availability` | `SPOT_WITH_FALLBACK_AZURE`, `first_on_demand: 1` | todas |
| `timeout_seconds` | `3600` na maioria; **`0`** em `streaming_continuous` (`:246`) | — |
| `max_retries` | `2` na maioria; **`0`** em `streaming_continuous` (`:247`) | — |
| `spark_env_vars` | `ENVIRONMENT: "${var.environment}"` | todas as tasks |

### 3.4 DAG Airflow (derivado)

`dags/dag_pipeline_santander.py` — `dag_id="pipeline_corretora_santander"` (`:44`),
`schedule_interval="0 6 * * *"` (`:47`), `start_date=datetime(2026,1,1)` (`:48`),
`catchup=False` (`:49`), `retries=2`, `retry_delay=5min` (`:23-24`).

19 tasks (`grep -c 'task_id="'` → 19) e 25 arestas, expressas em 18 linhas `>>`
(`:168-185`) — **grafo idêntico** ao de
`pipeline_completo`. Diferença de mecânica: o DAG usa
`DatabricksSubmitRunOperator` com `existing_cluster_id` +
`spark_python_task.python_file` (`:29-40`), ou seja, **cluster interativo fixo e
arquivo `.py` do Workspace**, e não job cluster + wheel. Ver risco 9.4.

---

## 4. INVENTÁRIO DE ARTEFATOS

Todos os nomes de tabela abaixo são **resolvidos pelo mapa da seção 2** e
apresentados na forma `<catalog>.<prefixo><camada>.<nome>`. Por economia, uso a
forma `hk`; `prod` substitui `hk_` por `prod_`.

### 4.1 Tabelas por camada — 30 no total (6 bronze + 7 silver + 17 gold)

#### Bronze — 6

| # | Nome resolvido (`hk`) | Formato | Produtor |
|---|---|---|---|
| 1 | `case_santander.hk_bronze.acoes` | Parquet → registrada Delta | `src/ingestion/yahoo_finance.py:71-73`; registro `jobs/job_unity_catalog.py:43,71` |
| 2 | `case_santander.hk_bronze.bcb` | Parquet particionado `extracao=` | `src/ingestion/bcb.py:181-183`; registro `jobs/job_unity_catalog.py:44,71` |
| 3 | `case_santander.hk_bronze.world_bank` | Parquet particionado `extracao=` | `src/ingestion/world_bank.py:92-94`; registro `jobs/job_unity_catalog.py:45,71` |
| 4 | `case_santander.hk_bronze.kafka` | Avro (Event Hub Capture) | Externo (Azure Capture); registro `jobs/job_unity_catalog.py:46,71` |
| 5 | `case_santander.hk_bronze.clientes` | Delta gerenciada (MERGE) | `jobs/job_clientes_ordens.py:135-159` |
| 6 | `case_santander.hk_bronze.ordens` | Delta gerenciada (MERGE) | `jobs/job_clientes_ordens.py:204-228` |

#### Silver — 7

| # | Nome resolvido (`hk`) | Tipo | Produtor |
|---|---|---|---|
| 1 | `case_santander.hk_silver.acoes` | Delta externa, particionada `ano,mes` | `src/transformation/silver_acoes.py:66-71`, registro `:89` |
| 2 | `case_santander.hk_silver.bcb` | Delta externa | `src/transformation/silver_bcb.py:37-41`, registro `:58` |
| 3 | `case_santander.hk_silver.world_bank` | Delta externa | `src/transformation/silver_world_bank.py:34-38`, registro `:55` |
| 4 | `case_santander.hk_silver.clientes` | Delta gerenciada (MERGE/CDC) | `jobs/job_clientes_ordens.py:243-268` |
| 5 | `case_santander.hk_silver.ordens` | Delta gerenciada (MERGE/CDC) | `jobs/job_clientes_ordens.py:277-302` |
| 6 | `case_santander.hk_silver.streaming` | Delta, `toTable` de Structured Streaming | `jobs/job_streaming_continuous.py:105-113` |
| 7 | `case_santander.hk_silver.clientes_scd` | Delta, SCD Type 2 | `src/clients/scd.py:82` (via `aplicar_scd_type2:44-61`) |

#### Gold — 17

| # | Nome resolvido (`hk`) | Produtor | Task |
|---|---|---|---|
| 1 | `case_santander.hk_gold.anomalias` | `src/gold/anomalias.py:41-45` (path) + registro `jobs/job_unity_catalog.py:104,113` | `t3_anomalias` |
| 2 | `case_santander.hk_gold.performance_acoes` | `src/gold/performance.py:42-45` (path) + registro `jobs/job_unity_catalog.py:103,113` | `t3_performance` |
| 3 | `case_santander.hk_gold.indicadores_bcb` | `src/gold/bcb_analise.py:70-71` | `t3_bcb` |
| 4 | `case_santander.hk_gold.contexto_macroeconomico` | `src/gold/world_bank_analise.py:93-94` | `t3_world_bank` |
| 5 | `case_santander.hk_gold.acoes_vs_cambio` | `src/gold/correlacao_acoes_cambio.py:112-113` | `t3_acoes_cambio` |
| 6 | `case_santander.hk_gold.deteccao_fraude` | `src/gold/fraude.py:81-85` | `t3_fraude` |
| 7 | `case_santander.hk_gold.posicao_clientes` | `jobs/job_corretora_analises.py:63-65` | `t7_corretora_analises` |
| 8 | `case_santander.hk_gold.score_risco_clientes` | `jobs/job_corretora_analises.py:116-118` | `t7_corretora_analises` |
| 9 | `case_santander.hk_gold.perfil_clientes` | `jobs/job_corretora_analises.py:135-137` | `t7_corretora_analises` |
| 10 | `case_santander.hk_gold.ordens_consolidadas` | `jobs/job_corretora_analises.py:151-153` | `t7_corretora_analises` |
| 11 | `case_santander.hk_gold.ranking_acoes_perfil` | `jobs/job_corretora_analises.py:167-169` | `t7_corretora_analises` |
| 12 | `case_santander.hk_gold.score_risco_scd` | `src/clients/scd.py:108` | `t9_scd` |
| 13 | `case_santander.hk_gold.observabilidade` | `jobs/job_observabilidade.py:33-37` | `t4_observabilidade` |
| 14 | `case_santander.hk_gold.fraude_streaming` | `src/gold/streaming_gold.py:89-93` | `streaming_to_gold_continuous` |
| 15 | `case_santander.hk_gold.anomalias_intraday` | `src/gold/streaming_gold.py:155-159` | `streaming_to_gold_continuous` |
| 16 | `case_santander.hk_gold.volume_intraday` | `src/gold/streaming_gold.py:227-231` | `streaming_to_gold_continuous` |
| 17 | `case_santander.hk_gold.ranking_acoes_realtime` | `src/gold/streaming_gold.py:295-299` | `streaming_to_gold_continuous` |

### 4.2 Paths ADLS de entrada/saída — 17 distintos

Definidos em `src/config/environment.py:127-146` e `src/config/settings.py:16-33`
(duplicados — ver 9.7). `{sa}` = `storage_account` resolvido.

| # | Chave | Path | Uso |
|---|---|---|---|
| 1 | `bronze_acoes` | `abfss://bronze@{sa}.dfs.core.windows.net/acoes/` | escrita `yahoo_finance.py:71` (subpath `data={data}/`), leitura `silver_acoes.py:22` |
| 2 | `bronze_bcb` | `abfss://bronze@{sa}…/bcb/` | escrita `bcb.py:181` (`extracao={data}/`), leitura `silver_bcb.py:11,16` |
| 3 | `bronze_world_bank` | `abfss://bronze@{sa}…/world_bank/` | escrita `world_bank.py:92`, leitura `silver_world_bank.py:11,16` |
| 4 | `bronze_kafka` | `abfss://bronze@{sa}…/kafka/` | leitura Auto Loader `job_streaming_continuous.py:51,78` |
| 5 | `bronze_clientes` | `abfss://bronze@{sa}…/clientes/` | **declarado, não usado** (ver 9.8) |
| 6 | `bronze_ordens` | `abfss://bronze@{sa}…/ordens/` | **declarado, não usado** |
| 7 | `silver_acoes` | `abfss://silver@{sa}…/acoes/` | escrita `silver_acoes.py:71`, leitura `anomalias.py:10`, `performance.py:19` |
| 8 | `silver_bcb` | `abfss://silver@{sa}…/bcb/` | escrita `silver_bcb.py:41` |
| 9 | `silver_world_bank` | `abfss://silver@{sa}…/world_bank/` | escrita `silver_world_bank.py:38` |
| 10 | `silver_streaming` | `abfss://silver@{sa}…/streaming/` | escrita `job_streaming_continuous.py:52,112` |
| 11 | `silver_clientes` | `abfss://silver@{sa}…/clientes/` | **declarado, não usado** |
| 12 | `silver_ordens` | `abfss://silver@{sa}…/ordens/` | **declarado, não usado** |
| 13 | (checkpoint) | `abfss://silver@{sa}…/checkpoints/streaming_continuous/` | `job_streaming_continuous.py:53` |
| 14 | `gold_anomalias` | `abfss://gold@{sa}…/anomalias/` | escrita `anomalias.py:11,45`; registro `job_unity_catalog.py:104` |
| 15 | `gold_performance` | `abfss://gold@{sa}…/performance_acoes/` | escrita `performance.py:20,45`; registro `job_unity_catalog.py:103` |
| 16 | `gold_cambio` | `abfss://gold@{sa}…/acoes_vs_cambio/` | **declarado, não usado** — a tabela virou gerenciada (`job_unity_catalog.py:96-101` documenta a remoção) |
| 17 | `gold_observ` | `abfss://gold@{sa}…/observabilidade/` | **declarado, não usado** — tabela gerenciada |

Arquivo local temporário: `/tmp/kaggle_{pid}/churn.zip` → `churn.csv`
(`jobs/job_clientes_ordens.py:88-101`).

### 4.3 Módulos com lógica de negócio — 14

| # | Módulo | Responsabilidade | LOC-chave |
|---|---|---|---|
| 1 | `src/ingestion/yahoo_finance.py` | Extração de 9 tickers B3, 2 anos de histórico | `:13-77` |
| 2 | `src/ingestion/bcb.py` | Séries SGS 11 (selic), 1 (câmbio), 433 (ipca); validação em 8 etapas + retry | `:39-191` |
| 3 | `src/ingestion/world_bank.py` | Indicadores `NY.GDP.MKTP.KD.ZG` e `SL.UEM.TOTL.ZS` | `:11-98` |
| 4 | `src/transformation/silver_acoes.py` | Enriquecimento (empresa/setor), variação diária, amplitude, gate de qualidade | `:13-92` |
| 5 | `src/transformation/silver_bcb.py` | Cast de data `dd/MM/yyyy`, dedup por (data, indicador) | `:9-61` |
| 6 | `src/transformation/silver_world_bank.py` | Compatibilidade de schema (`ano` vs `data`) | `:9-58` |
| 7 | `src/gold/anomalias.py` | Z-score de variação diária por ticker, limiar ±2 | `:8-49` |
| 8 | `src/gold/performance.py` | Agregação por ticker/empresa/setor/ano | `:11-49` |
| 9 | `src/gold/bcb_analise.py` | SMA 7d, volatilidade 30d, IPCA acum. 12m, alertas | `:13-75` |
| 10 | `src/gold/world_bank_analise.py` | Cenário macro, alerta de risco, impacto na bolsa | `:13-98` |
| 11 | `src/gold/correlacao_acoes_cambio.py` | Correlação de Pearson 90d ação × câmbio, recomendação | `:14-117` |
| 12 | `src/gold/fraude.py` | 4 regras de fraude batch, score Crítico/Alto/Médio/Normal | `:12-89` |
| 13 | `src/gold/streaming_gold.py` | 4 análises RT: fraude, anomalia intraday, volume, ranking | `:18-303` |
| 14 | `src/clients/scd.py` | SCD Type 2 (merge fecha versão + append) | `:12-117` |

Módulos **transversais** (não são lógica de negócio; contados à parte): 9 —
`src/config/{environment,logging,secrets,settings,tables}.py`,
`src/security/hashing.py`, `src/quality/data_quality.py`,
`src/utils/retry.py`, `src/ingestion/api_wrapper.py`.

Regra de negócio também vive em `jobs/job_corretora_analises.py:30-170`
(5 tabelas gold, incluindo o modelo ponderado de score de risco `:98-113`) —
única `jobs/*.py` com lógica própria, contada em 4.4.

### 4.4 Jobs / entrypoints executáveis — 24 arquivos, 22 registrados, 20 orquestrados

| # | Arquivo | `setup.py` | `databricks.yml` | DAG | Status |
|---|---|---|---|---|---|
| 1 | `jobs/job_unity_catalog_schemas.py` | ✔ `:48` | ✔ `t0` | ✔ | orquestrado |
| 2 | `jobs/job_extracao_acoes.py` | ✔ `:30` | ✔ `t1` | ✔ | orquestrado |
| 3 | `jobs/job_extracao_bcb.py` | ✔ `:31` | ✔ `t1` | ✔ | orquestrado |
| 4 | `jobs/job_extracao_world_bank.py` | ✔ `:32` | ✔ `t1` | ✔ | orquestrado |
| 5 | `jobs/job_clientes_ordens.py` | ✔ `:27` | ✔ `t6` | ✔ | orquestrado |
| 6 | `jobs/job_silver_acoes.py` | ✔ `:42` | ✔ `t2` | ✔ | orquestrado |
| 7 | `jobs/job_silver_bcb.py` | ✔ `:43` | ✔ `t2` | ✔ | orquestrado |
| 8 | `jobs/job_silver_world_bank.py` | ✔ `:44` | ✔ `t2` | ✔ | orquestrado |
| 9 | `jobs/job_gold_anomalias.py` | ✔ `:34` | ✔ `t3` + job avulso | ✔ | orquestrado |
| 10 | `jobs/job_gold_performance.py` | ✔ `:37` | ✔ `t3` + job avulso | ✔ | orquestrado |
| 11 | `jobs/job_gold_bcb.py` | ✔ `:35` | ✔ `t3` + job avulso | ✔ | orquestrado |
| 12 | `jobs/job_gold_world_bank.py` | ✔ `:38` | ✔ `t3` + job avulso | ✔ | orquestrado |
| 13 | `jobs/job_gold_acoes_vs_cambio.py` | ✔ `:33` | ✔ `t3` + job avulso | ✔ | orquestrado |
| 14 | `jobs/job_gold_fraude.py` | ✔ `:36` | ✔ `t3` + job avulso | ✔ | orquestrado |
| 15 | `jobs/job_corretora_analises.py` | ✔ `:29` | ✔ `t7` | ✔ | orquestrado |
| 16 | `jobs/job_scd.py` | ✔ `:41` | ✔ `t9` | ✔ | orquestrado |
| 17 | `jobs/job_lakehouse_monitoring.py` | ✔ `:39` | ✔ `t8` | ✔ | orquestrado |
| 18 | `jobs/job_unity_catalog.py` | ✔ `:47` | ✔ `t8b` | ✔ | orquestrado |
| 19 | `jobs/job_observabilidade.py` | ✔ `:40` | ✔ `t4` | ✔ | orquestrado |
| 20 | `jobs/job_streaming_continuous.py` | ✔ `:45` | ✔ job próprio | ✘ | orquestrado (fora do DAG) |
| 21 | `jobs/job_streaming_to_gold_continuous.py` | ✔ `:46` | ✔ job próprio | ✘ | orquestrado (fora do DAG) |
| 22 | `jobs/job_clientes_silver.py` | ✔ `:28` | ✘ | ✘ | **órfão** — registrado, nunca chamado |
| 23 | `jobs/job_streaming.py` | ✘ | ✘ | ✘ | **morto** |
| 24 | `jobs/job_streaming_to_gold.py` | ✘ | ✘ | ✘ | **morto** |

### 4.5 Tópicos / filas — 1 real

| # | Recurso | Nome resolvido | Onde |
|---|---|---|---|
| 1 | Azure Event Hub | `transacoes-financeiras-hk` / `transacoes-financeiras-prod` | `src/config/environment.py:81`; namespace `evhcasesantander-{env}` `:80`; provisionado em `terraform/modules/event_hub/` |

**Ressalva importante:** nenhum job consome o Event Hub por conector. Os
produtores (`scripts/eventhub_producer.py:86-98`,
`scripts/eventhub_producer_advanced.py:167-221`) publicam no hub; o **Event Hub
Capture** grava Avro em `abfss://bronze@{sa}…/kafka/`; e
`jobs/job_streaming_continuous.py:73-85` lê esse **path** via Auto Loader
(`cloudFiles`, formato `avro`), desembrulhando o envelope Capture pelo campo
`Body`. O acoplamento job↔hub é indireto, por storage.

Os dois produtores usam nomes **sem sufixo de ambiente**:
`scripts/eventhub_producer.py:24` fixa `EVENT_HUB_NAME = "transacoes-financeiras"`;
`scripts/eventhub_producer_advanced.py:42` usa default `"transacoes-financeiras"`.
Não batem com `transacoes-financeiras-{env}` de `environment.py:81`. Ver 9.9.

### 4.6 Integrações externas — 6

| # | Integração | Endpoint / SDK | Onde | Auth |
|---|---|---|---|---|
| 1 | Yahoo Finance | `yfinance.Ticker(...).history(period="2y")` | `src/ingestion/yahoo_finance.py:50-51` | nenhuma |
| 2 | BCB SGS | `https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados` | `src/ingestion/bcb.py:50-52` | nenhuma |
| 3 | World Bank | `https://api.worldbank.org/v2/country/BR/indicator/{ind}` | `src/ingestion/world_bank.py:46` | nenhuma |
| 4 | Kaggle | `https://www.kaggle.com/api/v1/datasets/download/mathchi/churn-for-bank-customers` | `jobs/job_clientes_ordens.py:92-93` | HTTP Basic (`kaggle-username`/`kaggle-key` do Key Vault) |
| 5 | Azure Key Vault (via Databricks secret scope) | `dbutils.secrets.get(scope=key_vault, key=...)` | `src/config/secrets.py:32-37` | scope = `kv-case-santander-{env}` |
| 6 | Azure Event Hub | `EventHubProducerClient.from_connection_string` | `scripts/eventhub_producer.py:86`, `_advanced.py:167` | connection string (`EVENTHUB_CONNECTION_STRING`) |

Adjacentes (SDK, não fonte de dado): `databricks.sdk.WorkspaceClient` em
`jobs/job_lakehouse_monitoring.py:25,44` (API Lakehouse Monitoring) e
`databricks.connect.DatabricksSession` em todos os jobs.

### 4.7 Segredos consumidos — 7

`src/config/secrets.py:40-70` + call sites: `client-id`, `client-secret`,
`tenant-id`, `storage-account`, `kaggle-username`, `kaggle-key`, `salt`
(`src/security/hashing.py:15`).

---

## 5. INVENTÁRIO DE RELAÇÕES

### 5.1 Joins — 7 reais

| # | Arquivo:linha | Esquerda | Direita | Chave | Tipo | Produz |
|---|---|---|---|---|---|---|
| 1 | `src/gold/correlacao_acoes_cambio.py:56` | `hk_silver.acoes` (agregada por date/ticker, `:29-32`) | `hk_silver.bcb` filtrada `indicador='cambio_usd_brl'` (`:34-38`) | `date` | `left` | `hk_gold.acoes_vs_cambio` |
| 2 | `src/gold/fraude.py:46-47` | `hk_silver.ordens` (`:22`) | `hk_gold.score_risco_clientes` (`:26`) | `hash_cliente` | `left` + `broadcast` | `hk_gold.deteccao_fraude` |
| 3 | `src/gold/fraude.py:50` | idem | idem | `hash_cliente` | `left` sort-merge (ramo alternativo do `if` de `:45`) | idem |
| 4 | `src/gold/streaming_gold.py:52` | `hk_silver.streaming` (`:37`) | `hk_gold.performance_acoes` ano máx. (`:41-45`) | `ticker` | `left` + `broadcast` | `hk_gold.fraude_streaming` |
| 5 | `src/gold/streaming_gold.py:138` | `hk_silver.streaming` agregada por ticker/hora (`:116-123`) | `hk_gold.performance_acoes` (`:127-131`) | `ticker` | `left` + `broadcast` | `hk_gold.anomalias_intraday` |
| 6 | `src/gold/streaming_gold.py:209` | `hk_silver.streaming` agregada (`:185-196`) | `hk_gold.performance_acoes` (`:200-204`) | `ticker` | `left` + `broadcast` | `hk_gold.volume_intraday` |
| 7 | `src/gold/streaming_gold.py:280` | `hk_silver.streaming` agregada por ticker (`:256-267`) | `hk_gold.performance_acoes` (`:271-275`) | `ticker` | `left` + `broadcast` | `hk_gold.ranking_acoes_realtime` |
| 8 | `jobs/job_corretora_analises.py:61` | posição agregada de `hk_silver.ordens` | `hk_silver.clientes` (`:34-37`) | `hash_cliente` | `left` + `broadcast` | `hk_gold.posicao_clientes` |

> São 8 linhas; **7 joins reais** porque `fraude.py:46` e `fraude.py:50` são os
> dois ramos do mesmo `if/else` — o mesmo join lógico, materializado uma vez por
> execução. Contado como 1.

### 5.2 MERGE / upsert (chaves de negócio) — 5

| # | Arquivo:linha | Tabela alvo | Condição de match | Semântica |
|---|---|---|---|---|
| 1 | `jobs/job_clientes_ordens.py:142-148` | `hk_bronze.clientes` | `target.hash_cliente = source.hash_cliente` | `whenMatchedUpdateAll` + `whenNotMatchedInsertAll` |
| 2 | `jobs/job_clientes_ordens.py:211-217` | `hk_bronze.ordens` | `target.id_ordem = source.id_ordem` | idem |
| 3 | `jobs/job_clientes_ordens.py:250-256` | `hk_silver.clientes` | `target.hash_cliente = source.hash_cliente` | idem |
| 4 | `jobs/job_clientes_ordens.py:284-290` | `hk_silver.ordens` | `target.id_ordem = source.id_ordem` | idem |
| 5 | `src/clients/scd.py:33-41` | `hk_silver.clientes_scd` **e** `hk_gold.score_risco_scd` | `antigo.{chave} = novo.{chave} AND antigo.atual = true` (chave = `hash_cliente` nos dois casos, `:82` e `:108`) | fecha versão (`data_fim`, `atual=false`), depois append |

### 5.3 Chaves de negócio (não há FK declarada — Delta/UC não as impõe)

| Chave | Origem | Propaga para |
|---|---|---|
| `hash_cliente` (SHA-256+salt de `CustomerId`) | `jobs/job_clientes_ordens.py:110` via `hash_customer_id` (`src/security/hashing.py:48`) | `bronze.clientes`, `bronze.ordens`, `silver.clientes`, `silver.ordens`, `silver.clientes_scd`, `gold.posicao_clientes`, `gold.score_risco_clientes`, `gold.score_risco_scd`, `gold.deteccao_fraude` |
| `id_ordem` (`ORD{hash_cliente}-{YYYYMMDD}-{seq:04d}`) | `jobs/job_clientes_ordens.py:185` | `bronze.ordens`, `silver.ordens`, `gold.deteccao_fraude` |
| `ticker` | `src/config/settings.py:2-12` (9 valores) | quase toda a linhagem de mercado |
| `id_transacao` | payload Event Hub, schema `jobs/job_streaming_continuous.py:69` | `silver.streaming`, `gold.fraude_streaming` |
| `indicador` | `src/ingestion/bcb.py:123`, `world_bank.py:66` | `silver.bcb`, `silver.world_bank` (vira coluna via pivot no gold) |

### 5.4 Dependências entre módulos — arestas de import relevantes

| Consumidor | Consumido | Linha |
|---|---|---|
| todos os 24 `jobs/*.py` | `src/config/environment.setup_python_path` | ex. `jobs/job_scd.py:6` |
| todos os `jobs/*.py` | `src/config/logging.{info,error,warning}` | ex. `jobs/job_scd.py:9` |
| 31 referências | `src/config/tables` | ex. `src/gold/fraude.py:9` |
| `src/config/tables.py:20` | `src/config/environment.EnvironmentConfig` | `:20` |
| `src/config/secrets.py:7` | `src/config/environment.get_config` | `:7` |
| `src/security/hashing.py:9` | `src/config/secrets.get_secret` | `:9` |
| `src/config/settings.py:60` | `src/config/secrets.get_secret` | `:60` (import tardio) |
| `src/transformation/silver_*.py` | `src/quality/data_quality.DataQualityValidator` | `:10`, `:6`, `:6` |
| `src/ingestion/{yahoo_finance,bcb,world_bank}.py` | `src/ingestion/api_wrapper.rate_limiter` | `:9`, `:8`, `:7` |
| `src/ingestion/{yahoo_finance,world_bank}.py` | `src/utils/retry.retry_on_connection_error` | `:10`, `:8` |
| `jobs/job_scd.py:15` | `src/clients/scd.{aplicar_scd_clientes,aplicar_scd_score_risco}` | `:15` |
| `jobs/job_observabilidade.py:15` | `src/observability/monitoring.executar_monitoramento` | `:15` |
| `jobs/job_streaming_to_gold_continuous.py` | as 4 funções de `src/gold/streaming_gold.py` | chamadas `:83,87,91,95` |
| `scripts/auto_generate_dag.py:14` | `src/pipeline/dynamic_pipeline.auto_generate_dag` | `:14` |

### 5.5 Dependências de dado entre jobs (arestas implícitas, não declaradas na orquestração)

| Produtor | Consumidor | Tabela | Declarado no grafo? |
|---|---|---|---|
| `t7_corretora_analises` → `gold.score_risco_clientes` | `t3_fraude` (`src/gold/fraude.py:26`) | `gold.score_risco_clientes` | **Sim** (`databricks.yml:616-617`) |
| `t3_performance` → `gold.performance_acoes` | `streaming_to_gold_continuous` (`streaming_gold.py:43,129,202,273`) | `gold.performance_acoes` | **NÃO** — jobs distintos, sem dependência. Ver risco 9.10 |
| `t4_observabilidade` → `gold.observabilidade` | `streaming_to_gold_continuous` (`:56-59`, watermark CDC) | `gold.observabilidade` | **NÃO** — idem |
| `t2_silver_acoes` → `silver.acoes` | `t3_anomalias`, `t3_performance`, `t3_acoes_cambio` | `silver.acoes` | Sim |
| `t6_clientes_ordens` → `silver.{clientes,ordens}` | `t7_corretora_analises` | — | Sim |
| `streaming_continuous` → `silver.streaming` | `streaming_to_gold_continuous` | `silver.streaming` | **NÃO** — jobs independentes por design (serviço 24/7 × agendado 5min) |

---

## 6. INVENTÁRIO DE CAMPOS DAS TABELAS FINAIS

### 6.0 Critério de "tabela final"

O prompt pede "consumidas por fora: BI, API, cliente, outro sistema". Neste repo
**não há API nem BI ligado ao lakehouse** (os dashboards Grafana em
`docker/grafana/provisioning/dashboards/` consultam o Postgres do Airflow —
`docker/grafana/provisioning/datasources/airflow-postgres.yml:5-8` — e não contêm
uma única referência a `case_santander`, `silver.` ou `gold.`).

Critério adotado: **toda tabela da camada Gold** (17), por serem o produto
publicado no Unity Catalog, mais as **2 tabelas SCD** — `silver.clientes_scd`
(consumida por `notebooks/case_presentation.py:852`) e `gold.score_risco_scd`
(`:892`), que existem para auditoria regulatória externa e não alimentam nenhuma
tabela a jusante.

**19 tabelas finais · 269 campos.** Sem regra — regra é trabalho de A2/A3.

### 6.1 `case_santander.<env>_gold.anomalias` — 12 campos
`src/gold/anomalias.py:33-39` (`.select` explícito)

`date`, `ticker`, `empresa`, `setor`, `open`, `close`, `volume`,
`variacao_diaria_pct`, `zscore`, `anomalia`, `tipo_anomalia`, `data_processamento`

### 6.2 `case_santander.<env>_gold.performance_acoes` — 13 campos
`src/gold/performance.py:27-39`

`ticker`, `empresa`, `setor`, `ano`, `preco_medio`, `preco_minimo`,
`preco_maximo`, `variacao_media_pct`, `volatilidade`, `volume_medio`,
`volume_total`, `dias_negociados`, `data_processamento`

### 6.3 `case_santander.<env>_gold.indicadores_bcb` — 13 campos
`src/gold/bcb_analise.py:28-66`. As 3 colunas de pivot são **inferidas** de
`src/ingestion/bcb.py:158,161,164` e confirmadas pelo uso nominal em `:43,47,53`.

`data`, `selic` *(pivot)*, `cambio_usd_brl` *(pivot)*, `ipca` *(pivot)*,
`selic_media_7d`, `selic_volatilidade_30d`, `cambio_media_7d`,
`cambio_variacao_pct`, `ipca_acumulado_12m`, `tendencia_selic`,
`alerta_cambio`, `alerta_inflacao`, `data_processamento`

### 6.4 `case_santander.<env>_gold.contexto_macroeconomico` — 11 campos
`src/gold/world_bank_analise.py:28-89`. Pivot inferido de
`src/ingestion/world_bank.py:80-81`, confirmado em `:40,45`.

`ano`, `pib_anual` *(pivot)*, `desemprego` *(pivot)*, `pib_variacao_pct`,
`desemprego_variacao_pct`, `tendencia_pib`, `tendencia_desemprego`,
`cenario_macro`, `alerta_risco`, `impacto_bolsa`, `data_processamento`

### 6.5 `case_santander.<env>_gold.acoes_vs_cambio` — 13 campos
`src/gold/correlacao_acoes_cambio.py:102-108` (`.select` explícito)

`date`, `ticker`, `preco_medio`, `variacao_media_pct`, `cambio`,
`cambio_variacao_pct`, `correlacao_cambio`, `sensibilidade_cambio`,
`alerta_desacoplamento`, `recomendacao`, `ano`, `mes`, `data_processamento`

### 6.6 `case_santander.<env>_gold.deteccao_fraude` — 26 campos
`src/gold/fraude.py:22-79`. **Sem `.select` final** — herda `SELECT *` de
`silver.ordens` (16 campos, resolvidos de `jobs/job_clientes_ordens.py:187-201`
+ `:272-275`), mais 3 do join, mais 7 derivados.

Herdados de `silver.ordens` (16): `id_ordem`, `hash_cliente`, `perfil_risco`,
`faixa_saldo`, `ticker`, `preco`, `quantidade`, `valor_total`, `tipo`,
`corretora`, `status`, `data_ordem`, `data_extracao`, `ano`, `mes`,
`data_processamento`
Do join com `gold.score_risco_clientes` (3): `score_risco`, `categoria_risco`,
`limite_operacional`
Derivados (7): `alerta_valor_alto`, `alerta_volume_suspeito`,
`alerta_preco_atipico`, `alerta_perfil_incompativel`, `total_alertas`,
`score_fraude`, `requer_revisao`

> `data_processamento` é sobrescrito em `:79`, não duplicado.

### 6.7 `case_santander.<env>_gold.fraude_streaming` — 17 campos
`src/gold/streaming_gold.py:79-87` (`.select` explícito)

`id_transacao`, `timestamp`, `ticker`, `tipo`, `preco`, `quantidade`,
`valor_total`, `preco_medio`, `volatilidade`, `alerta_volume_suspeito`,
`alerta_preco_atipico`, `alerta_valor_elevado`, `alerta_desvio_historico`,
`total_alertas`, `score_fraude`, `requer_revisao`, `data_processamento`

### 6.8 `case_santander.<env>_gold.anomalias_intraday` — 13 campos
`src/gold/streaming_gold.py:116-153` (sem `.select` final)

`ticker`, `hora`, `preco_medio_hora`, `valor_total_hora`, `volume_hora`,
`total_transacoes_hora`, `preco_medio`, `volatilidade`, `desvio_historico_rs`,
`zscore_intraday`, `anomalia`, `tipo_anomalia`, `data_processamento`

### 6.9 `case_santander.<env>_gold.volume_intraday` — 13 campos
`src/gold/streaming_gold.py:185-225`

`ticker`, `hora`, `total_transacoes`, `volume_hora`, `valor_total_hora`,
`preco_medio_hora`, `volume_compras`, `volume_vendas`, `volume_medio`,
`pct_volume_diario`, `alerta_volume_intraday`, `pressao_compradora`,
`data_processamento`

### 6.10 `case_santander.<env>_gold.ranking_acoes_realtime` — 16 campos
`src/gold/streaming_gold.py:256-293`

`ticker`, `total_transacoes`, `volume_total`, `valor_total`,
`preco_medio_atual`, `total_compras`, `total_vendas`, `preco_minimo`,
`preco_maximo`, `empresa`, `setor`, `preco_medio_historico`,
`variacao_vs_historico_pct`, `tendencia`, `data_processamento`, `rank_volume`

### 6.11 `case_santander.<env>_gold.posicao_clientes` — 15 campos
`jobs/job_corretora_analises.py:41-61`

`hash_cliente`, `ticker`, `quantidade_liquida`, `total_comprado`,
`total_vendido`, `total_ordens`, `ordens_executadas`, `ordens_canceladas`,
`valor_investido`, `resultado_estimado`, `situacao`, `data_processamento`,
`perfil_risco`, `faixa_saldo`, `score_credito`

### 6.12 `case_santander.<env>_gold.score_risco_clientes` — 19 campos
`jobs/job_corretora_analises.py:70-114`

`hash_cliente`, `perfil_risco`, `faixa_saldo`, `score_credito`, `num_ativos`,
`total_ordens`, `total_canceladas`, `valor_total_investido`,
`posicoes_descoberto`, `resultado_medio`, `taxa_cancelamento_pct`,
`score_credito_norm`, `score_perfil`, `score_saldo`, `score_comportamento`,
`score_risco`, `categoria_risco`, `limite_operacional`, `data_processamento`

### 6.13 `case_santander.<env>_gold.perfil_clientes` — 10 campos
`jobs/job_corretora_analises.py:122-134`

`perfil_risco`, `faixa_etaria`, `score_categoria`, `pais`, `total_clientes`,
`saldo_medio`, `score_medio`, `salario_medio`, `total_churn`, `taxa_churn_pct`

### 6.14 `case_santander.<env>_gold.ordens_consolidadas` — 10 campos
`jobs/job_corretora_analises.py:141-150`

`ticker`, `perfil_risco`, `faixa_saldo`, `tipo`, `status`, `ano`,
`total_ordens`, `volume_total`, `preco_medio`, `qtd_media`

### 6.15 `case_santander.<env>_gold.ranking_acoes_perfil` — 5 campos
`jobs/job_corretora_analises.py:157-166`

`ticker`, `perfil_risco`, `total_ordens`, `volume_total`, `preco_medio`

### 6.16 `case_santander.<env>_gold.observabilidade` — 9 campos
`src/observability/monitoring.py:54-64`

`camada`, `tabela`, `data_verificacao`, `total_registros`, `total_nulos`,
`total_duplicatas`, `versao_cdf`, `qualidade_pct`, `tempo_seg`

### 6.17 `case_santander.<env>_gold.score_risco_scd` — 12 campos
`src/clients/scd.py:100-108` + colunas SCD de `aplicar_scd_type2:22-24`

`hash_cliente`, `perfil_risco`, `faixa_saldo`, `score_credito`, `score_risco`,
`categoria_risco`, `limite_operacional`, `num_ativos`, `total_ordens`,
`data_inicio`, `data_fim`, `atual`

### 6.18 `case_santander.<env>_silver.clientes_scd` — 13 campos
`src/clients/scd.py:74-82` + colunas SCD de `:22-24`

`id_cliente`, `hash_cliente`, `sobrenome_masked`, `perfil_risco`,
`score_credito`, `faixa_saldo`, `faixa_etaria`, `score_categoria`, `ativo`,
`churn`, `data_inicio`, `data_fim`, `atual`

### 6.19 `case_santander.<env>_silver.streaming` — 13 campos
Incluída porque é consumida diretamente por `notebooks/case_presentation.py:415`
e é a raiz das 4 gold de streaming. `jobs/job_streaming_continuous.py:62-101`

`timestamp`, `ticker`, `preco`, `quantidade`, `tipo`, `corretora`,
`id_transacao`, `hora`, `minuto`, `valor_total`, `alerta_volume`,
`alerta_preco`, `processado_em`

### 6.20 Totalização

| Tabela | Campos |
|---|---|
| gold.anomalias | 12 |
| gold.performance_acoes | 13 |
| gold.indicadores_bcb | 13 |
| gold.contexto_macroeconomico | 11 |
| gold.acoes_vs_cambio | 13 |
| gold.deteccao_fraude | 26 |
| gold.fraude_streaming | 17 |
| gold.anomalias_intraday | 13 |
| gold.volume_intraday | 13 |
| gold.ranking_acoes_realtime | 16 |
| gold.posicao_clientes | 15 |
| gold.score_risco_clientes | 19 |
| gold.perfil_clientes | 10 |
| gold.ordens_consolidadas | 10 |
| gold.ranking_acoes_perfil | 5 |
| gold.observabilidade | 9 |
| gold.score_risco_scd | 12 |
| silver.clientes_scd | 13 |
| silver.streaming | 13 |
| **TOTAL** | **269** |

> **Denominador da Fase V: 269 campos em 19 tabelas.** A varredura deve exigir
> regra completa para cada um. Atenção aos 26 de `gold.deteccao_fraude`
> (16 herdados por `SELECT *`, sem `.select` explícito) e às 5 colunas de pivot
> (`selic`, `cambio_usd_brl`, `ipca`, `pib_anual`, `desemprego`), que são
> data-driven e podem variar se a ingestão mudar.

---

## 7. VALIDAÇÃO DO INVENTÁRIO

Regra: o contrato leva a coluna **REAIS**. Cada falso positivo é nomeado.

| Contagem | Bruto | **REAIS** | FP | Falsos positivos nomeados |
|---|---|---|---|---|
| Tabelas totais | 31 | **30** | 1 | `case_santander.bronze.{tabela}` — literal em bloco **comentado** `jobs/job_unity_catalog.py:79,86,88,89,91`. Não executa e usa schema sem prefixo de ambiente. |
| Tabelas Gold | 17 | **17** | 0 | — |
| Tabelas Silver | 7 | **7** | 0 | — |
| Tabelas Bronze | 7 | **6** | 1 | `bronze.clientes` / `bronze.ordens` do dicionário `tabelas_bronze_delta` **comentado** em `jobs/job_unity_catalog.py:50-53` — não somam; as tabelas reais de mesmo nome vêm de `job_clientes_ordens.py:135,204` e **já estão contadas**. O FP é a entrada duplicada. **Falso NEGATIVO a evitar:** o grep por `SCHEMA_BRONZE}.<nome>` acha só 2 — `acoes`, `bcb`, `world_bank` e `kafka` são gravadas por `f"{SCHEMA_BRONZE}.{tabela}"` (`:71`), com o nome vindo das chaves do dict `:42-47`. Contar só pelo literal subconta bronze em 4. |
| Jobs orquestrados | 24 arquivos | **20** | 4 | `job_clientes_silver.py` (em `setup.py:28`, ausente de `databricks.yml` e do DAG — órfão); `job_streaming.py` e `job_streaming_to_gold.py` (ausentes dos três registros — código morto, substituídos pelas variantes `_continuous`); e a **4ª** é `job_unity_catalog_schemas` vs `job_unity_catalog`, que são 2 jobs distintos e ambos contam — não é FP. **Correção: 3 FP, 21 orquestrados.** Ver nota abaixo. |
| Jobs `databricks.yml` | 9 | **9** | 0 | Os 6 `t3_gold_*` avulsos são definições reais implantadas, ainda que redundantes. Contam como jobs; ver risco 9.5. |
| Tasks em `pipeline_completo` | 19 | **19** | 0 | Cuidado com a indentação: `task_key` a **6 espaços** é definição de task (19); a **10 espaços** é entrada de `depends_on` (25 = arestas). Contar sem ancorar a indentação mistura os dois e dá 44. |
| Arestas de dependência | 25 | **25** | 0 | No DAG Airflow são 18 **linhas** `>>` para as mesmas 25 arestas (`:179` tem 2, `:183` tem 7). Contar linhas em vez de arestas subconta em 7. |
| Joins | 8 ocorrências de `.join(` em `src/`+`jobs/` | **7** | 2 | (a) `src/pipeline/dynamic_pipeline.py:279` — `", ".join(self.jobs.keys())` é **`str.join` do Python**, dentro de uma f-string que **gera código de DAG**, não é join de DataFrame; (b) `src/gold/fraude.py:46` e `:50` são os dois ramos do mesmo `if/else` (`:45`) — 1 join lógico, contado uma vez. |
| MERGE Delta | 5 | **5** | 0 | — |
| Módulos com lógica de negócio | 23 arquivos em `src/` | **14** | 9 | Transversais, não são regra de negócio: `src/config/environment.py`, `logging.py`, `secrets.py`, `settings.py`, `tables.py`, `src/security/hashing.py`, `src/quality/data_quality.py`, `src/utils/retry.py`, `src/ingestion/api_wrapper.py`. |
| Tópicos / filas | 3 nomes | **1** | 2 | `transacoes-financeiras-{env}` é o hub real. FPs: (a) o path `bronze_kafka` — é **storage**, não fila, apesar do nome "kafka"; (b) `transacoes-financeiras` sem sufixo em `scripts/eventhub_producer.py:24` e `_advanced.py:42` — mesmo recurso, nome divergente, não um segundo hub. |
| Integrações externas | 8 | **6** | 2 | FPs: (a) `scripts/diagnostics/{apis,apis_rest,apis_simple,kaggle,yahoo}.py` — batem nas **mesmas** 4 APIs já contadas, e `scripts/diagnostics/README.md:1-8` declara que são verificações manuais, não código de produção; (b) `tests/test_github_connection.py` — 0 funções `test_`, script de verificação de conectividade GitHub, fora do domínio de dados. |
| Paths ADLS | 17 | **11 em uso** / 17 declarados | 6 | Declarados e **sem nenhum call site**: `bronze_clientes`, `bronze_ordens`, `silver_clientes`, `silver_ordens` (essas 4 tabelas são gerenciadas, gravadas por `saveAsTable`, sem path), `gold_cambio` e `gold_observ` (idem — a remoção do `gold_cambio` está documentada em `jobs/job_unity_catalog.py:96-101`). |
| Tabelas finais (Fase V) | 30 | **19** | 11 | Excluídas por não serem consumo externo: as 6 bronze e as 5 silver de trabalho (`acoes`, `bcb`, `world_bank`, `clientes`, `ordens`) — todas são insumo de camada seguinte. `silver.streaming` e `silver.clientes_scd` **foram incluídas** (critério em 6.0). |
| Campos das tabelas finais | 269 | **269** | 0 | — |
| Testes automatizados | 6 arquivos | **5 arquivos / 31 testes** | 1 | `tests/test_github_connection.py` — 0 funções `test_`. Não é suíte. |
| Ambientes | 3 (premissa do prompt: dev/hml/prd) | **2** | 1 | Não existe `dev`. `src/config/environment.py:13` fixa `["hk","prod"]`; `:56` levanta `ValueError` para qualquer outro. |

**Nota sobre "jobs orquestrados":** revisando item a item, os falsos positivos
são **3** (`job_clientes_silver.py`, `job_streaming.py`,
`job_streaming_to_gold.py`), logo **21 jobs orquestrados** de 24 arquivos.
Destes 21, **19 estão no DAG Airflow** — `job_streaming_continuous` e
`job_streaming_to_gold_continuous` existem só em `databricks.yml`.

### 7.1 Números finais do contrato

| Métrica | Valor |
|---|---|
| Repositórios | **1** |
| Ambientes | **2** (`hk`, `prod`) |
| Tabelas totais | **30** (6 bronze + 7 silver + 17 gold) |
| Tabelas finais (denominador Fase V) | **19** |
| Campos das tabelas finais | **269** |
| Jobs em `databricks.yml` | **9** |
| Tasks em `pipeline_completo` | **19** |
| Arestas de dependência | **25** |
| Arquivos de job | **24** (21 orquestrados, 3 mortos/órfãos) |
| Entrypoints em `setup.py` | **22** |
| Joins | **7** |
| MERGE Delta | **5** |
| Módulos com lógica de negócio | **14** |
| Tópicos / filas | **1** |
| Integrações externas | **6** |
| Segredos | **7** |
| Paths ADLS | **17 declarados / 11 em uso** |
| Testes automatizados | **31** em 5 arquivos |

---

## 8. COMANDOS DE REGENERAÇÃO

Executar da raiz do repositório. Shell: Git Bash / POSIX.

```bash
# ---- 0. PROCEDÊNCIA (sempre primeiro) ----
git rev-parse --abbrev-ref HEAD
git rev-parse --short HEAD
git status --short

# ---- 2. MAPA DE PARAMETRIZAÇÃO ----
# Raiz da resolução
sed -n '20,45p' src/config/tables.py
sed -n '84,115p' src/config/environment.py
# Toda referência a tabela por variável (insumo do mapa 2.2)
grep -rn -E "SCHEMA_(BRONZE|SILVER|GOLD)\}\.[a-z_0-9]+" --include=*.py src jobs tests notebooks
# Nomes distintos de tabela
grep -rhoE "SCHEMA_(BRONZE|SILVER|GOLD)\}\.[a-z_0-9]+" --include=*.py src jobs | sort -u
# Parametrização via helper (não casa com o padrão acima)
grep -rn -E "register_external_table|table_fqn|schema_fqn" --include=*.py src jobs
# Variáveis do bundle não consumidas
grep -c "var.enable_streaming" databricks.yml    # esperado: 0 -> [NÃO RESOLVIDO]

# ---- 3. ORQUESTRAÇÃO ----
# Jobs: restringir ao bloco `resources:`; sem o sed, o grep pega tambem as
# chaves de 4 espacos sob `targets:` (variables/workspace/resources) e da 14.
sed -n '38,716p' databricks.yml | grep -c "^    [a-z0-9_]*:$"   # jobs (9)
# Tasks: ancorar em 6 espacos. A 10 espacos sao entradas de depends_on.
grep -c "^      - task_key:" databricks.yml                     # tasks (19)
grep -c "^          - task_key:" databricks.yml                 # arestas (25)
grep -n -E "^    [a-z0-9_]+:|task_key:|entry_point:|depends_on|quartz_cron_expression|pause_status" databricks.yml
# Grafo do DAG Airflow, para comparar com o bundle
grep -c 'task_id="' dags/dag_pipeline_santander.py              # tasks (19)
grep -n ">>" dags/dag_pipeline_santander.py                     # 18 LINHAS = 25 arestas

# ---- 4.1 TABELAS POR CAMADA ----
# ATENCAO no bronze: este grep retorna apenas 2 (clientes, ordens). As outras 4
# nao aparecem porque `job_unity_catalog.py:71` grava via `f"{SCHEMA_BRONZE}.{tabela}"`
# — nome vindo da chave do dict `tabelas_bronze_parquet` (:42-47). Some as duas listas.
grep -rhoE "SCHEMA_BRONZE\}\.[a-z_0-9]+" --include=*.py src jobs | sort -u          # 2 literais
sed -n '42,47p' jobs/job_unity_catalog.py                                           # +4 via loop = 6
grep -rhoE "SCHEMA_SILVER\}\.[a-z_0-9]+" --include=*.py src jobs | sort -u          # 7
grep -rhoE "SCHEMA_GOLD\}\.[a-z_0-9]+"   --include=*.py src jobs | sort -u          # 17
# Escritas (produtores)
grep -rn "saveAsTable\|\.toTable(\|\.save(" --include=*.py src jobs

# ---- 4.2 PATHS ADLS ----
grep -rhoE "abfss://[a-z]+@\{[a-z_]+\}\.dfs\.core\.windows\.net/[a-z_/]*" --include=*.py src jobs | sort -u

# ---- 4.4 JOBS: orquestrado vs órfão vs morto ----
for f in jobs/job_*.py; do b=$(basename $f .py); \
  echo "$b | yml=$(grep -c "entry_point: $b\$" databricks.yml)" \
       "| setup=$(grep -c "\"$b = " setup.py)" \
       "| dag=$(grep -c "$b.py" dags/dag_pipeline_santander.py)"; done
ls jobs/job_*.py | wc -l                          # 24
grep -c "jobs\.job_" setup.py                     # 22

# ---- 4.5 / 4.6 FILAS E INTEGRAÇÕES ----
grep -rn -E "EventHub|eventhub|transacoes-financeiras" --include=*.py src jobs scripts
grep -rn -E "https?://[a-z0-9./_-]+" --include=*.py src/ingestion jobs/job_clientes_ordens.py

# ---- 4.7 SEGREDOS ----
grep -rhoE "get_secret\(\"[a-z-]+\"\)" --include=*.py src jobs | sort -u

# ---- 5.1 JOINS (revisar FP de str.join) ----
grep -rn "\.join(" --include=*.py src jobs
# ---- 5.2 MERGE ----
grep -rn "\.merge(" --include=*.py src jobs

# ---- 6. CAMPOS DAS TABELAS FINAIS ----
# .alias() e .withColumn() são as duas formas que nomeiam coluna neste repo
grep -rhoE "\.alias\(\"[a-z_0-9]+\"\)|withColumn\(\"[a-z_0-9]+\"" --include=*.py src/gold jobs/job_corretora_analises.py | sort -u
# .select explícitos (delimitam a lista final onde existem)
grep -rn -A15 "\.select($" --include=*.py src/gold

# ---- 7. FALSOS POSITIVOS ----
grep -rn "case_santander\." --include=*.py . | grep -v '/\.git/'      # literais; conferir se comentados
grep -rn "^\s*#.*saveAsTable\|^\s*#.*DROP TABLE" --include=*.py jobs src
grep -c "def test_" tests/*.py                                        # test_github_connection.py -> 0

# ---- 9. ZONAS DE RISCO ----
grep -rn "except Exception" --include=*.py src jobs | wc -l
grep -rn "TODO\|FIXME\|XXX\|HACK" --include=*.py src jobs
```

---

## 9. ZONAS DE RISCO

### 9.1 Branch do prompt não existe
O prompt pede branch `master`. `git branch -a` lista `main`,
`release/segunda-chance-dm` e 12 remotas — **nenhuma `master`**. O default remoto
é `origin/main`. Inventário levantado em `release/segunda-chance-dm` @ `6394bf3`.
**Se o contrato deveria descrever `main`, este inventário está na branch errada** —
confirmar antes de A1/A2/A3 consumirem.

### 9.2 Working tree suja no momento da leitura
`README.md` e `docs/README.md` modificados, `EXECUTIVE_SUMMARY.md` não rastreado.
Nenhum arquivo de código afetado, então as afirmações sobre código valem para
`6394bf3`. Mas o commit lido **não reproduz o estado do diretório**.

### 9.3 `.env.example` documenta variáveis que o código não lê
`.env.example:19-32` define `STORAGE_ACCOUNT_HK`, `STORAGE_ACCOUNT_PROD`,
`KEY_VAULT_HK`, `KEY_VAULT_PROD`, `EVENTHUB_NS_HK`, `EVENTHUB_NS_PROD`.
`src/config/environment.py:78-81` lê `STORAGE_ACCOUNT`, `KEY_VAULT_NAME`,
`EVENTHUB_NAMESPACE`, `EVENTHUB_NAME` — **sem sufixo**. Preencher o `.env` como
o exemplo instrui não tem efeito nenhum.
Pior: os **valores** também divergem. `.env.example:20` diz
`STORAGE_ACCOUNT_HK=stcasesantander-hk` (com hífen); o default do código é
`stcasesantanderhk` (sem hífen — e o comentário `environment.py:69-70` explica
que Azure só aceita minúsculas e dígitos, ou seja, o valor do `.env.example` é
inválido). `.env.example:24` diz `KEY_VAULT_PROD=kv-case-santander`; o código
gera `kv-case-santander-prod`. Como o Key Vault é também o **secret scope**
(`environment.py:71`, `secrets.py:35`), errar aqui derruba todo `get_secret`.
`SQL_SERVER_HK`/`SQL_SERVER_PROD` (`:29-31`) não têm **nenhuma** referência no
código ou no Terraform.

### 9.4 DAG Airflow e bundle divergem no mecanismo de execução
Grafo idêntico, mecânica incompatível:
- `databricks.yml` → `python_wheel_task` + `new_cluster` (job cluster efêmero) + `spark_env_vars.ENVIRONMENT`
- `dags/dag_pipeline_santander.py:29-40` → `spark_python_task.python_file` + `existing_cluster_id`

Consequências: (a) o DAG aponta para um cluster **hardcoded**
`0401-150803-wefgy1hc` (`:17`, default do `os.getenv`) — se esse cluster não
existir, todas as tasks falham; (b) o DAG **não injeta `ENVIRONMENT`** no
cluster — lê `os.getenv("ENVIRONMENT","hk")` no `:19` só para o `description` e
as `tags` (`:46,50`), nunca passando ao job. Rodando pelo Airflow, os jobs caem
no default `hk` do `environment.py:54` **independentemente do target**; (c) o DAG
executa `.py` do Workspace, não o wheel — o `sys.path` depende de
`REPO_PATH`/`DATABRICKS_REPO_PATH`, que apontam para um caminho pessoal
(`/Workspace/Users/diego.silva0001@gmail.com/...`, `environment.py:32` e
`dags/dag_pipeline_santander.py:18`).

### 9.5 Seis jobs Gold duplicados no bundle
`t3_gold_anomalias`, `t3_gold_performance`, `t3_gold_bcb`, `t3_gold_world_bank`,
`t3_gold_acoes_cambio`, `t3_gold_fraude` (`databricks.yml:39-219`) têm o mesmo
`entry_point` das tasks `t3_*` dentro de `pipeline_completo`. Sem schedule e sem
predecessor. São implantados e podem ser disparados manualmente **sem as
dependências de dado** — `t3_gold_fraude` avulso, por exemplo, lê
`gold.score_risco_clientes` que só `t7_corretora_analises` produz. Rodar avulso
reescreve gold com dado velho ou falha.

### 9.6 `validate_environment` nunca é chamada
`src/config/environment.py:159-177` implementa o gate de produção
(`CONFIRM_PRODUCTION=true`). `grep -rn "validate_environment"` retorna **apenas a
definição** — nenhum call site em `jobs/`, `src/`, `dags/` ou `scripts/`.
A proteção de produção documentada em `.env.example:11` é inerte.

### 9.7 `get_paths` duplicado e divergente
`src/config/environment.py:120-156` e `src/config/settings.py:15-33` definem
`get_paths` com os mesmos 16 paths ADLS. A de `environment.py` acrescenta 8
chaves de Unity Catalog (`schema_bronze`…`table_silver_streaming`, `:148-155`);
a de `settings.py` não. Duas fontes da verdade para a mesma informação. Agrava:
`environment.py:151-155` expõe `table_bronze_clientes`, `table_silver_ordens`
etc. que **duplicam** o que `src/config/tables.py` resolve — e nenhuma dessas 8
chaves tem call site (`grep -rn "table_bronze_clientes\|table_silver_streaming"`
→ só a definição).

### 9.8 Seis paths ADLS declarados e nunca usados
`bronze_clientes`, `bronze_ordens`, `silver_clientes`, `silver_ordens`,
`gold_cambio`, `gold_observ` (`environment.py:133-146`, `settings.py:21-32`).
Correspondem a tabelas que migraram para gerenciadas (`saveAsTable`, sem path).
Resíduo de refatoração. `jobs/job_unity_catalog.py:96-101` documenta a remoção
do `gold_cambio` da lista de registro, mas o path continua declarado.

### 9.9 Produtores Event Hub usam nome sem sufixo de ambiente
`scripts/eventhub_producer.py:24` fixa `EVENT_HUB_NAME = "transacoes-financeiras"`;
`_advanced.py:42` usa o mesmo como default de `EVENTHUB_NAME`. O código de
runtime espera `transacoes-financeiras-{env}` (`environment.py:81`) e o Terraform
provisiona com sufixo. Rodar o produtor sem exportar `EVENTHUB_NAME` publica em
um hub que não existe (ou, se existir, não é o que o pipeline lê).

### 9.10 CDC de streaming: lido e descartado
`jobs/job_streaming_to_gold_continuous.py:61-68` monta `df_cdf` com
`readChangeFeed` a partir da watermark `MAX(versao_cdf)` de
`gold.observabilidade`. Mas `df_cdf` **só é usado em `.count()`** (`:69`). As 4
funções de `src/gold/streaming_gold.py` chamadas em seguida (`:83,87,91,95`)
fazem cada uma o seu `SELECT * FROM {SCHEMA_SILVER}.streaming`
(`streaming_gold.py:37,113,182,253`) — **full scan**, ignorando o CDC. O CDC
serve apenas de guarda "há dado novo?". O mesmo padrão está em
`jobs/job_streaming_to_gold.py:66-76` (job morto). O comentário
`monitoring.py:39-45` afirma que o problema da coluna `versao_cdf` foi corrigido,
mas a leitura incremental continua sem efeito prático.

Agrava: `gold.observabilidade` é produzida por `t4_observabilidade`, a **última**
task de `pipeline_completo`, enquanto `streaming_to_gold_continuous` roda a cada
5 minutos como job **independente**. Na primeira execução do dia a watermark
ainda é a de ontem (ou 0 pelo `COALESCE`).

### 9.11 Estimativa de broadcast em `fraude.py` é código quebrado
`src/gold/fraude.py:35-38`:
```python
df_score_size = spark.sql(f"""
    SELECT SUM(LENGTH(CAST(struct(*) AS STRING))) as total_bytes
    FROM {df_score._sc.parallelize([]).toDF().name}
""").collect()[0][0] or 0
```
`df_score._sc` usa API privada; `parallelize([])` sobre lista vazia não infere
schema; `.toDF().name` não é um nome de tabela SQL. A expressão lança sempre, o
`except` de `:41` engole e `use_broadcast` fica `True` incondicionalmente
(`:43`). O fallback sort-merge de `:50` é **inalcançável**. O comentário
`:29-30` afirma que a proteção existe para quando `df_score` crescer com SCD
Type 2 — ela não existe.

### 9.12 Dependência de dado entre jobs não declarada
Três tabelas atravessam fronteira de job sem aresta de orquestração:

| Produtor | Consumidor | Tabela |
|---|---|---|
| `pipeline_completo` / `t3_performance` | `streaming_to_gold_continuous` | `gold.performance_acoes` (`streaming_gold.py:43,129,202,273`) |
| `pipeline_completo` / `t4_observabilidade` | `streaming_to_gold_continuous` | `gold.observabilidade` (`:56-59`) |
| `streaming_continuous` | `streaming_to_gold_continuous` | `silver.streaming` |

`streaming_to_gold_continuous` roda a cada 5 min (`databricks.yml:282`) e
`pipeline_completo` uma vez por dia às 06:00 (`:713`). Entre 00:00 e 06:00 o job
de streaming lê `gold.performance_acoes` do dia anterior. Como
`src/gold/performance.py:44` grava em `mode("overwrite")`, existe janela em que
`t3_performance` está reescrevendo a tabela enquanto o job de 5 minutos a lê.

### 9.13 Código morto e órfão
| Item | Evidência |
|---|---|
| `jobs/job_streaming.py` | ausente de `setup.py`, `databricks.yml` e do DAG. Substituído por `job_streaming_continuous.py` |
| `jobs/job_streaming_to_gold.py` | idem. Substituído por `job_streaming_to_gold_continuous.py` |
| `jobs/job_clientes_silver.py` | em `setup.py:28`, mas em nenhuma orquestração. Sua lógica (`:38-65`) é subconjunto de `job_clientes_ordens.py:231-302`, **sem** o MERGE/CDC — usa `saveAsTable` em `overwrite`. Se alguém rodar o entrypoint, perde o histórico CDC |
| `src/health/health_check.py` | **0 imports** em todo o repo. 190 linhas, inclusive um `health_check_decorator` (`:166`) nunca aplicado. Os hits de "health" em `.github/workflows/ci-cd.yml:257` e `docker-compose.prod.yml:104` são de infra (job do CI, healthcheck HTTP do Airflow), não deste módulo |
| `src/pipeline/dynamic_pipeline.py` | 1 único importador, `scripts/auto_generate_dag.py:14`, que por sua vez **não é chamado por nenhum workflow** — o CI usa `scripts/sync_airflow_from_databricks.py` (`.github/workflows/ci-cd.yml:162-165`). Gerador de DAG paralelo e não utilizado |
| Blocos comentados | `jobs/job_unity_catalog.py:50-53` (`tabelas_bronze_delta`) e `:78-92` (loop de registro). Referenciam `case_santander.bronze.*` sem prefixo de ambiente — se descomentados, quebram o isolamento hk/prod |
| `scripts/fix_keyvault_hardcoding.py`, `fix_syspath_hardcoding.py`, `replace_print_with_logging.py` | scripts de refatoração one-shot, sem chamador |

### 9.14 `enable_streaming` declarada e ignorada
Já detalhado em 2.4. Impacto operacional: `streaming_continuous` **não tem
override de pausa** em `hk` (`databricks.yml:734-738` cobre só
`streaming_to_gold_continuous`) e não tem schedule — é um serviço 24/7. O único
freio em `hk` é o efeito colateral de `mode: development` (`:720`), que o próprio
comentário `:729-733` reconhece como frágil. Se `hk` virar `mode: production`, um
cluster de streaming sobe 24/7 em homologação.

### 9.15 Duas convenções de escrita convivendo
Parte das tabelas é gravada por **path** (`.save(...)` + registro externo
posterior): `silver.acoes`, `silver.bcb`, `silver.world_bank`, `gold.anomalias`,
`gold.performance_acoes`. O resto é **gerenciada** (`saveAsTable`). O comentário
`src/config/tables.py:49-58` documenta que essa fratura já causou três tasks
gold morrendo com `TABLE_OR_VIEW_NOT_FOUND`. A mitigação (quem escreve registra)
está aplicada nas silver (`silver_acoes.py:89`, `silver_bcb.py:58`,
`silver_world_bank.py:55`), mas **não** nas duas gold por path —
`gold.anomalias` e `gold.performance_acoes` continuam dependendo de
`t8b_uc_registro` (`jobs/job_unity_catalog.py:113`), que roda **depois** de
`t8_lakehouse_monitoring` e das próprias tasks `t3_*`. Na primeira execução do
ambiente, os consumidores dessas duas tabelas não as encontram.

### 9.16 Erros engolidos em massa
`grep -c "except Exception"` em `src/` + `jobs/` → padrão recorrente de
`except Exception as e: info(...)` que registra e segue. Casos com impacto de
dado: `jobs/job_unity_catalog.py:75-76` (falha ao registrar tabela bronze não
aborta), `:135-136` (Liquid Clustering), `jobs/job_observabilidade.py:63-64`
(OPTIMIZE/VACUUM), `src/observability/monitoring.py:80-82` (uma tabela ilegível
vira `{}` e some das métricas — `gold.observabilidade` fica silenciosamente
incompleta), `src/ingestion/world_bank.py:75-78` (indicador que falha vira
DataFrame vazio; se ambos falharem, `:85-87` retorna 0 e o job **passa** sem
dado). Um pipeline verde não é evidência de dado presente.

### 9.17 Divergências documentação × código
| Documento | Afirma | Código |
|---|---|---|
| `databricks.yml:250-251` | "Condicionado à variável `enable_streaming`" | Nenhuma condicional existe (2.4) |
| `.env.example:19-32` | Variáveis com sufixo `_HK`/`_PROD` | Código lê nomes sem sufixo (9.3) |
| `.env.example:11` | `CONFIRM_PRODUCTION` protege produção | `validate_environment` nunca chamada (9.6) |
| `src/config/tables.py:4-9` | "219 referências hardcoded ... por 24 arquivos" — passado | Não verificável neste commit; restam 5 literais, todos em comentário (`job_unity_catalog.py:79-91`) |
| `src/gold/fraude.py:29-30` | Fallback sort-merge protege contra `df_score` > 2GB | Código quebrado, `use_broadcast` sempre `True` (9.11) |
| `src/observability/monitoring.py:39-45` | CDC corrigido, `versao_cdf` agora gravada | A coluna é gravada, mas o consumidor descarta o CDF (9.10) |
| `dags/dag_pipeline_santander.py:1-6` | "Sincronizado com Databricks Asset Bundles" | Grafo sim; mecanismo de execução e propagação de `ENVIRONMENT` não (9.4) |
| `docker/grafana/.../*.json` (6 dashboards, nomes `santander-data-metrics`, `santander-pipeline`, `santander-streaming`) | sugerem monitoramento do lakehouse | Datasource único é o Postgres do Airflow (`airflow-postgres.yml:5-8`); zero referências a `case_santander`/`silver.`/`gold.` |

### 9.18 O que NÃO foi possível determinar
| Item | Por quê |
|---|---|
| Schema exato de `bronze.acoes` | Vem do `yfinance` em runtime; `src/ingestion/yahoo_finance.py:68` normaliza os nomes (`lower()`, espaço→`_`). As colunas listadas em 6.x foram inferidas do que `silver_acoes.py:30-64` consome (`date/open/high/low/close/volume/dividends/stock_splits`) — não lidas de um schema declarado |
| Schema de `bronze.kafka` | Avro produzido pelo Event Hub Capture, externo ao repo. Só o envelope é conhecido, pelo comentário `job_streaming_continuous.py:80-82` e pelo uso de `Body` em `:84` |
| Colunas de pivot (5) | Data-driven (2.4). Inferidas dos módulos de ingestão |
| Se `silver.world_bank` retém a coluna de partição `extracao` | `src/transformation/silver_bcb.py:33` faz `.drop("extracao")`; `silver_world_bank.py` **não** faz. Lê com `basePath` (`:16`), então `extracao` provavelmente entra como coluna — mas isso depende do comportamento do reader em runtime, não é afirmável pelo código |
| Volume/cardinalidade real de qualquer tabela | Todos os jobs retornam `0` com o comentário "Metrics available in Spark UI" (`anomalias.py:49`, `performance.py:49`, `fraude.py:89`, `bcb_analise.py:75`, etc.). Os `info()` que reportam totais no fim dos jobs de streaming recebem esses zeros |
| Quem consome as tabelas gold fora do repo | Nenhum consumidor externo encontrado. O notebook cobre 10 das 17 gold; **7 gold não têm nenhum leitor no repo**: `indicadores_bcb`, `contexto_macroeconomico`, `acoes_vs_cambio`, `posicao_clientes`, `perfil_clientes`, `ordens_consolidadas`, `ranking_acoes_perfil`. Produzidas e não lidas por nada rastreável |

---

*Levantado em `release/segunda-chance-dm` @ `6394bf3`. Todas as afirmações têm
`arquivo:linha`. Contagens reprodutíveis pela seção 8.*
