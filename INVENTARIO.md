# INVENTÁRIO — Fase de Reconhecimento (CONTRATO)

> Levantamento feito **do zero, a partir do código**, em engenharia reversa.
> Toda afirmação está ancorada em `arquivo:linha`.
> README, docs/, EXECUTIVE_SUMMARY.md e a versão anterior deste arquivo **não
> foram usados como fonte** — divergências entre eles e o código estão na §9.

---

## 0. PREMISSAS DO PROMPT QUE NÃO SE APLICARAM

| Premissa do prompt | O que o repositório mostra | Como foi tratado |
|---|---|---|
| Branch `master` | `git rev-parse --abbrev-ref HEAD` → **`release/segunda-chance-dm`**. Não existe branch `master` local; o nome "master" aparece apenas no nome da pasta (`case-santander-data-master`) e no path do Workspace (`src/config/environment.py:32`) | Documentei a branch **realmente lida**. §1.1. |
| Repositórios (plural) | **Um único repositório**, sem submódulos (`.gitmodules` ausente) | §1 traz uma ficha só. |
| Control-M (`.yaml`/`.yml` de definição de job) | **Não há Control-M.** Nenhum `.yaml/.yml/.xml/.json` de Control-M. A orquestração é **Databricks Asset Bundles** (`databricks.yml`) com um DAG Airflow **derivado** (`dags/dag_pipeline_santander.py`) | §3 documenta `databricks.yml` como a fonte da ORDEM REAL, e o DAG Airflow como espelho gerado. |
| Entrypoint parametrizado `app.py` | **`app.py` não existe** (`find . -name app.py` → vazio; `find . -name main.py` → vazio) | A parametrização de tabelas vive em `src/config/tables.py` + `src/config/environment.py`. Os entrypoints executáveis são os **24 `console_scripts`** de `setup.py:25-52`. §2 mapeia essa camada inteira. |
| Domínio corretora / mercado financeiro | Confere: ordens, carteira, score de risco, fraude, câmbio, Selic/IPCA, ações B3 | — |

---

## 1. FICHA DO REPOSITÓRIO

### 1.1 Identificação e procedência (auditabilidade)

| Item | Valor | Comando |
|---|---|---|
| Caminho | `c:\Users\thedi\OneDrive\Desktop\GIT\case-santander-data-master` | — |
| Branch lida | **`release/segunda-chance-dm`** | `git rev-parse --abbrev-ref HEAD` |
| Commit lido | **`f7265c7`** | `git rev-parse --short HEAD` |
| Commit completo | `f7265c7df876ee52f2f33bb9427283e51e887010` | `git rev-parse HEAD` |
| Data / assunto | `2026-09-03 07:17:47 -0300` — `refactor: uma tabela por job, sem misturar camadas` | `git log -1 --format="%H %ad %s"` |
| Working tree | Sujo: `M README.md`, `M docs/README.md`, `?? EXECUTIVE_SUMMARY.md` | `git status --short` |

> **O código lido é o da working tree.** As três modificações pendentes são de
> documentação; nenhum arquivo `.py`, `databricks.yml` ou `setup.py` está sujo,
> então a leitura do pipeline equivale ao commit `f7265c7`.

### 1.2 Papel do repositório

Repositório **único e completo** — cobre as três funções:

- **Ingestão** — `src/ingestion/` (5 módulos): Yahoo Finance, BCB, World Bank,
  Kaggle e geração de ordens simuladas gravam Parquet/Delta no ADLS Bronze e em
  tabelas Delta bronze.
- **Transformação** — `src/transformation/` (Bronze→Silver, 5 módulos) e
  `src/gold/` (Silver→Gold, 7 módulos), arquitetura medallion.
- **Publicação** — registro das tabelas no Unity Catalog
  (`src/config/tables.py:54-69`, `jobs/job_unity_catalog.py`), Liquid Clustering
  (`jobs/job_unity_catalog.py:122-138`), Change Data Feed
  (`jobs/job_unity_catalog.py:144-160`), Lakehouse Monitoring
  (`jobs/job_lakehouse_monitoring.py:27-56`) e manutenção Delta OPTIMIZE/VACUUM
  (`jobs/job_observabilidade.py:44-64`).
- **Infraestrutura como código** — `terraform/` (7 módulos Azure) e `docker/`.

### 1.3 Linguagens

| Linguagem | Onde | Evidência |
|---|---|---|
| Python ≥ 3.11 | `src/`, `jobs/`, `scripts/`, `dags/`, `tests/`, `notebooks/` | `setup.py:21` (`python_requires=">=3.11"`) |
| PySpark / Delta / Databricks Connect 14.3.\* | todo o pipeline | `setup.py:14,17` |
| SQL (Spark SQL) | dentro de f-strings `spark.sql(...)` | ex. `jobs/job_corretora_analises.py:122-134` |
| YAML | `databricks.yml`, `.github/workflows/*.yml`, `docker/*.yml` | — |
| HCL (Terraform) | `terraform/**/*.tf` | `terraform/main.tf` |
| Bash | `scripts/deploy_infra.sh`, `scripts/destroy_infra.sh` | — |

### 1.4 Entrypoints e como se executa

**Entrypoints declarados: 24** (`setup.py:25-52`, `console_scripts`), consumidos
por `python_wheel_task.entry_point` no `databricks.yml`.

```bash
# 1. Build do wheel (declarado em databricks.yml:29-33)
python setup.py bdist_wheel

# 2. Validar e implantar o bundle
databricks bundle validate -t hk
databricks bundle deploy  -t hk        # ou -t prod

# 3. Disparar o workflow pai
databricks bundle run pipeline_completo -t hk

# 4. Job isolado
databricks bundle run t3_gold_fraude -t hk

# 5. Testes locais
pytest tests/ -v
```

Ambiente é escolhido pela env var `ENVIRONMENT` (`hk` | `prod`),
default `hk` (`src/config/environment.py:54`), injetada no cluster via
`spark_env_vars.ENVIRONMENT` em **todas as 30 tasks** do `databricks.yml`.

---

## 2. MAPA DE PARAMETRIZAÇÃO DE TABELAS

> **Não existe `app.py`.** A camada de parametrização equivalente é
> `src/config/tables.py` (nomes de tabela) apoiada em
> `src/config/environment.py` (nomes de recurso por ambiente).

### 2.1 Raiz da resolução

```
src/config/environment.py:60-117   EnvironmentConfig.get_config(env)
        ├─ catalog        = "case_santander"   (igual nos dois ambientes)  :87,:102
        └─ schema_prefix  = "hk_" | "prod_"                                :88,:103
                     ↓
src/config/tables.py:28-36
        CATALOG        = _config["catalog"]                                :30
        SCHEMA_PREFIX  = _config["schema_prefix"]                          :31
        SCHEMA_BRONZE  = f"{CATALOG}.{SCHEMA_PREFIX}bronze"                :34
        SCHEMA_SILVER  = f"{CATALOG}.{SCHEMA_PREFIX}silver"                :35
        SCHEMA_GOLD    = f"{CATALOG}.{SCHEMA_PREFIX}gold"                  :36
```

Helpers de resolução:

| Símbolo | Local | Resolve para |
|---|---|---|
| `schema_fqn(layer)` | `src/config/tables.py:41-46` | `SCHEMA_BRONZE/SILVER/GOLD` conforme `'bronze'\|'silver'\|'gold'` |
| `table_fqn(layer, name)` | `src/config/tables.py:49-51` | `catalog.<prefixo><camada>.<name>` |
| `register_external_table(spark, layer, name, path)` | `src/config/tables.py:54-69` | cria schema + `CREATE TABLE IF NOT EXISTS <fqn> USING DELTA LOCATION '<path>'` |

**Resolução por ambiente (os dois ambientes registrados):**

| Símbolo | `ENVIRONMENT=hk` | `ENVIRONMENT=prod` |
|---|---|---|
| `SCHEMA_BRONZE` | `case_santander.hk_bronze` | `case_santander.prod_bronze` |
| `SCHEMA_SILVER` | `case_santander.hk_silver` | `case_santander.prod_silver` |
| `SCHEMA_GOLD` | `case_santander.hk_gold` | `case_santander.prod_gold` |

Valores inválidos de `ENVIRONMENT` levantam `ValueError`
(`src/config/environment.py:55-56`). Ambientes permitidos: `["hk", "prod"]`
(`src/config/environment.py:13`).

### 2.2 Mapa completo parâmetro → nome real da tabela

Nas colunas de nome real, `hk_` ↔ `prod_` é a única diferença; escrevo o `hk`
e o leitor troca o prefixo.

#### Bronze — 6

| Referência no código | Nome real (hk) | Produtor (arquivo:linha) | Consumidor(es) |
|---|---|---|---|
| `f"{SCHEMA_BRONZE}.{tabela}"` com `tabela="acoes"` | `case_santander.hk_bronze.acoes` | `jobs/job_unity_catalog.py:44` (chave) → `:71` (`saveAsTable`) — dado bruto em `src/ingestion/yahoo_finance.py:71-73` | `notebooks/case_presentation.py:163` |
| idem, `tabela="bcb"` | `case_santander.hk_bronze.bcb` | `jobs/job_unity_catalog.py:45` → `:71` — dado em `src/ingestion/bcb.py:181-183` | `notebooks/case_presentation.py:164` |
| idem, `tabela="world_bank"` | `case_santander.hk_bronze.world_bank` | `jobs/job_unity_catalog.py:46` → `:71` — dado em `src/ingestion/world_bank.py:92-94` | `notebooks/case_presentation.py:165` |
| idem, `tabela="kafka"` | `case_santander.hk_bronze.kafka` | `jobs/job_unity_catalog.py:47` → `:71` — dado gravado pelo **Event Hub Capture** (`terraform/modules/event_hub/main.tf:30-43`) | `notebooks/case_presentation.py:168` |
| `f"{SCHEMA_BRONZE}.clientes"` | `case_santander.hk_bronze.clientes` | `src/ingestion/clientes_kaggle.py:104-105` (`merge_ou_cria`) | `src/ingestion/ordens_simuladas.py:46`; `src/transformation/silver_clientes.py:24` |
| `f"{SCHEMA_BRONZE}.ordens"` | `case_santander.hk_bronze.ordens` | `src/ingestion/ordens_simuladas.py:87-88` (`merge_ou_cria`) | `src/transformation/silver_ordens.py:24` |

#### Silver — 7

| Referência no código | Nome real (hk) | Produtor | Consumidor(es) |
|---|---|---|---|
| `register_external_table(spark,"silver","acoes",…)` | `case_santander.hk_silver.acoes` | `src/transformation/silver_acoes.py:89` (dado: `:66-71`) | `src/gold/correlacao_acoes_cambio.py:31`; `jobs/job_observabilidade.py:45`; `jobs/job_unity_catalog.py:123`; `src/observability/monitoring.py:91` |
| `register_external_table(spark,"silver","bcb",…)` | `case_santander.hk_silver.bcb` | `src/transformation/silver_bcb.py:58` (dado: `:37-41`) | `src/gold/bcb_analise.py:24`; `src/gold/correlacao_acoes_cambio.py:36`; `src/observability/monitoring.py:92` |
| `register_external_table(spark,"silver","world_bank",…)` | `case_santander.hk_silver.world_bank` | `src/transformation/silver_world_bank.py:55` (dado: `:34-38`) | `src/gold/world_bank_analise.py:24`; `src/observability/monitoring.py:93` |
| `f"{SCHEMA_SILVER}.clientes"` | `case_santander.hk_silver.clientes` | `src/transformation/silver_clientes.py:37` (`merge_ou_cria`) | `jobs/job_corretora_analises.py:36,131`; `src/clients/scd.py:79`; `jobs/job_lakehouse_monitoring.py:36`; `jobs/job_observabilidade.py:47`; `jobs/job_unity_catalog.py:125,147`; `src/observability/monitoring.py:94` |
| `f"{SCHEMA_SILVER}.ordens"` | `case_santander.hk_silver.ordens` | `src/transformation/silver_ordens.py:31` (`merge_ou_cria`) | `jobs/job_corretora_analises.py:32,147,162`; `src/gold/fraude.py:22`; `jobs/job_lakehouse_monitoring.py:37`; `jobs/job_observabilidade.py:46`; `jobs/job_unity_catalog.py:124,146`; `src/observability/monitoring.py:95` |
| `f"{SCHEMA_SILVER}.streaming"` | `case_santander.hk_silver.streaming` | `jobs/job_streaming_continuous.py:113` (`.toTable`) · também `register_external_table` em `jobs/job_streaming.py:116` (job órfão, §9) | `src/gold/streaming_gold.py:37,113,182,253`; `jobs/job_streaming_to_gold_continuous.py:66,76`; `jobs/job_lakehouse_monitoring.py:38`; `jobs/job_observabilidade.py:48`; `jobs/job_unity_catalog.py:145`; `src/observability/monitoring.py:96` |
| `f"{SCHEMA_SILVER}.clientes_scd"` | `case_santander.hk_silver.clientes_scd` | `src/clients/scd.py:82` (`aplicar_scd_type2`) | `src/clients/scd.py:86` (contagem); `notebooks/case_presentation.py:852,870,873` |

#### Gold — 17

| Referência no código | Nome real (hk) | Produtor | Consumidor(es) |
|---|---|---|---|
| `register_external_table(spark,"gold",tabela,path)` com `tabela="anomalias"` | `case_santander.hk_gold.anomalias` | `jobs/job_unity_catalog.py:106` (chave) → `:113` — dado em `src/gold/anomalias.py:41-45` | `jobs/job_lakehouse_monitoring.py:28`; `jobs/job_observabilidade.py:49`; `jobs/job_unity_catalog.py:126`; `src/observability/monitoring.py:97` |
| idem, `tabela="performance_acoes"` | `case_santander.hk_gold.performance_acoes` | `jobs/job_unity_catalog.py:105` → `:113` — dado em `src/gold/performance.py:42-45` | `src/gold/streaming_gold.py:43,44,129,130,202,203,273,274`; `jobs/job_observabilidade.py:50`; `jobs/job_unity_catalog.py:127` |
| `f"{SCHEMA_GOLD}.indicadores_bcb"` | `case_santander.hk_gold.indicadores_bcb` | `src/gold/bcb_analise.py:70-72` | *(nenhum no código)* |
| `f"{SCHEMA_GOLD}.contexto_macroeconomico"` | `case_santander.hk_gold.contexto_macroeconomico` | `src/gold/world_bank_analise.py:93-95` | *(nenhum no código)* |
| `f"{SCHEMA_GOLD}.acoes_vs_cambio"` | `case_santander.hk_gold.acoes_vs_cambio` | `src/gold/correlacao_acoes_cambio.py:112-114` | *(nenhum no código)* |
| `f"{SCHEMA_GOLD}.deteccao_fraude"` | `case_santander.hk_gold.deteccao_fraude` | `src/gold/fraude.py:85` | `jobs/job_lakehouse_monitoring.py:31`; `jobs/job_observabilidade.py:51`; `jobs/job_unity_catalog.py:128`; `src/observability/monitoring.py:100`; `tests/test_data_quality.py:20` |
| `f"{SCHEMA_GOLD}.posicao_clientes"` | `case_santander.hk_gold.posicao_clientes` | `jobs/job_corretora_analises.py:65` | `jobs/job_lakehouse_monitoring.py:29`; `jobs/job_observabilidade.py:53`; `src/observability/monitoring.py:98` |
| `f"{SCHEMA_GOLD}.score_risco_clientes"` | `case_santander.hk_gold.score_risco_clientes` | `jobs/job_corretora_analises.py:118` | `src/gold/fraude.py:26`; `src/clients/scd.py:105`; `jobs/job_lakehouse_monitoring.py:30`; `jobs/job_observabilidade.py:54`; `jobs/job_unity_catalog.py:129`; `src/observability/monitoring.py:99` |
| `f"{SCHEMA_GOLD}.perfil_clientes"` | `case_santander.hk_gold.perfil_clientes` | `jobs/job_corretora_analises.py:137` | *(nenhum no código)* |
| `f"{SCHEMA_GOLD}.ordens_consolidadas"` | `case_santander.hk_gold.ordens_consolidadas` | `jobs/job_corretora_analises.py:153` | *(nenhum no código)* |
| `f"{SCHEMA_GOLD}.ranking_acoes_perfil"` | `case_santander.hk_gold.ranking_acoes_perfil` | `jobs/job_corretora_analises.py:169` | *(nenhum no código)* |
| `f"{SCHEMA_GOLD}.observabilidade"` | `case_santander.hk_gold.observabilidade` | `jobs/job_observabilidade.py:37` | `jobs/job_streaming_to_gold.py:62`; `jobs/job_streaming_to_gold_continuous.py:58` (marca d'água CDC) |
| `f"{SCHEMA_GOLD}.fraude_streaming"` | `case_santander.hk_gold.fraude_streaming` | `src/gold/streaming_gold.py:93` | `jobs/job_lakehouse_monitoring.py:32`; `jobs/job_observabilidade.py:52`; `src/observability/monitoring.py:101` |
| `f"{SCHEMA_GOLD}.anomalias_intraday"` | `case_santander.hk_gold.anomalias_intraday` | `src/gold/streaming_gold.py:159` | `jobs/job_lakehouse_monitoring.py:33`; `src/observability/monitoring.py:102` |
| `f"{SCHEMA_GOLD}.volume_intraday"` | `case_santander.hk_gold.volume_intraday` | `src/gold/streaming_gold.py:231` | `jobs/job_lakehouse_monitoring.py:34`; `src/observability/monitoring.py:103` |
| `f"{SCHEMA_GOLD}.ranking_acoes_realtime"` | `case_santander.hk_gold.ranking_acoes_realtime` | `src/gold/streaming_gold.py:299` | `jobs/job_lakehouse_monitoring.py:35`; `src/observability/monitoring.py:104` |
| `f"{SCHEMA_GOLD}.score_risco_scd"` | `case_santander.hk_gold.score_risco_scd` | `src/clients/scd.py:108` (`aplicar_scd_type2`) | `src/clients/scd.py:112`; `notebooks/case_presentation.py:892,907` |

### 2.3 Parâmetros de infraestrutura (não-tabela)

| Símbolo | Local | `hk` | `prod` | Override |
|---|---|---|---|---|
| `storage_account` | `src/config/environment.py:78` | `stcasesantanderhk` | `stcasesantanderprod` | env `STORAGE_ACCOUNT` |
| `key_vault` (= secret scope) | `:79` | `kv-case-santander-hk` | `kv-case-santander-prod` | env `KEY_VAULT_NAME` |
| `eventhub_ns` | `:80` | `evhcasesantander-hk` | `evhcasesantander-prod` | env `EVENTHUB_NAMESPACE` |
| `eventhub_name` | `:81` | `transacoes-financeiras-hk` | `transacoes-financeiras-prod` | env `EVENTHUB_NAME` |
| `databricks_workspace` | `:89,:104` | env `DATABRICKS_HOST_HK` | env `DATABRICKS_HOST_PROD` | — |
| `data_retention_days` | `:96,:111` | 30 | 90 | — |
| `enable_streaming` | `:97,:112` | `False` | `True` | — |
| `is_production` | `:98,:113` | `False` | `True` | — |
| rate limits (yahoo/bcb/world_bank/kaggle) | `:90-95`, `:105-110` | 15/45/90/5 req·min | 30/120/300/20 req·min | — |
| `REPO_PATH` | `:26-35` | env `REPO_PATH`, senão `/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master` dentro do Databricks, senão `os.getcwd()` | idem | env `REPO_PATH` |
| `spark_version` | `databricks.yml:9,789,811` | `14.3.x-scala2.12` | `14.3.x-scala2.12` | var do bundle |
| `gold_workers` / `sql_workers` | `databricks.yml:790-791,812-813` | 2 / 1 | 4 / 2 | var do bundle |

Secrets lidos do Key Vault via `dbutils.secrets.get(scope=key_vault, key=…)`
(`src/config/secrets.py:32-37`): `client-id`, `tenant-id`, `client-secret`,
`storage-account`, `kaggle-username`, `kaggle-key`, `salt` — **7 secrets**.

### 2.4 Parâmetros [NÃO RESOLVIDOS]

| Símbolo | Local de uso | Por que não resolve |
|---|---|---|
| `{df_score._sc.parallelize([]).toDF().name}` dentro de `spark.sql(f"… FROM {…}")` | `src/gold/fraude.py:35-38` | Nome de "tabela" montado em runtime a partir de um `DataFrame` vazio. `DataFrame` do PySpark **não tem atributo `.name`** → `AttributeError` a cada execução, engolido pelo `except Exception` de `:41-43`, que força `use_broadcast = True`. Não é uma tabela do lakehouse: é código morto por exceção. |
| `f"{SCHEMA_GOLD}"` como `output_schema_name` | `jobs/job_lakehouse_monitoring.py:47` | Resolve para o **schema** (`case_santander.hk_gold`), não uma tabela. Não conta no inventário de tabelas. |
| `${schema_bronze}` / `${schema_silver}` / `${schema_gold}` | `notebooks/case_presentation.py:157-1166` (células `%sql`) | Widgets Databricks (`:44-46`) alimentados por `SCHEMA_*`. Resolvem corretamente **em runtime no Databricks**, mas não são resolvíveis por leitura estática de arquivo isolado. Todas as tabelas referenciadas lá já estão cobertas pelo mapa §2.2. |

---

## 3. ORQUESTRAÇÃO — DEFINIÇÕES DE JOB

> **Não há Control-M.** A fonte da ordem de execução real é
> `databricks.yml` (Databricks Asset Bundles). O DAG Airflow
> `dags/dag_pipeline_santander.py` é **gerado** a partir dele
> (`scripts/sync_airflow_from_databricks.py`, cf. cabeçalho `:1-7` e rodapé `:208-212`).

### 3.0 Vocabulário Control-M → Databricks

| Control-M | Equivalente aqui |
|---|---|
| Job / folder | `resources.jobs.<id>` em `databricks.yml` |
| Script executado | `python_wheel_task.entry_point` → `setup.py:25-52` → `jobs/*.py` |
| Predecessor | `depends_on: - task_key: …` |
| Agendamento | `schedule.quartz_cron_expression` + `timezone_id` |
| Condição / IN-COND | não há: só dependência estrutural de task |
| Recurso / lock | não há pool nem `max_concurrent_runs`; cada task levanta **job cluster próprio** (`new_cluster`) |

### 3.1 Jobs definidos — **9**

| # | Job (`resources.jobs.*`) | Linha | Task(s) → entry_point | Agendamento | Timeout / retries |
|---|---|---|---|---|---|
| 1 | `t3_gold_anomalias` | `databricks.yml:39` | `anomalias` → `job_gold_anomalias` (`:49,:52`) | **sem schedule** | 3600 s / 2 (`:66-67`) |
| 2 | `t3_gold_performance` | `:70` | `performance` → `job_gold_performance` (`:80,:83`) | **sem schedule** | 3600 / 2 (`:97-98`) |
| 3 | `t3_gold_bcb` | `:100` | `bcb` → `job_gold_bcb` (`:110,:113`) | **sem schedule** | 3600 / 2 (`:127-128`) |
| 4 | `t3_gold_world_bank` | `:130` | `world_bank` → `job_gold_world_bank` (`:140,:143`) | **sem schedule** | 3600 / 2 (`:157-158`) |
| 5 | `t3_gold_acoes_cambio` | `:160` | `acoes_cambio` → `job_gold_acoes_vs_cambio` (`:170,:173`) | **sem schedule** | 3600 / 2 (`:187-188`) |
| 6 | `t3_gold_fraude` | `:190` | `fraude` → `job_gold_fraude` (`:200,:203`) | **sem schedule** | 3600 / 2 (`:217-218`) |
| 7 | `streaming_continuous` | `:221` | `streaming_continuous` → `job_streaming_continuous` (`:231,:234`) | **sem schedule** (serviço 24/7, `:249`) | **0 / 0** (`:246-247`) |
| 8 | `streaming_to_gold_continuous` | `:253` | `streaming_to_gold` → `job_streaming_to_gold_continuous` (`:263,:266`) | `0 */5 * * * ?` · `America/Sao_Paulo` · `UNPAUSED` (`:281-284`); **`PAUSED` no target `hk`** (`:801-806`) | 3600 / 2 (`:278-279`) |
| 9 | `pipeline_completo` | `:287` | **22 tasks** (§3.2) | `0 0 6 * * ?` · `America/Sao_Paulo` · `UNPAUSED` (`:779-782`) | por task (§3.3) |

**Jobs efetivamente agendados: 2** (`pipeline_completo`, `streaming_to_gold_continuous`).
Os 6 jobs `t3_gold_*` são réplicas avulsas das tasks homônimas dentro de
`pipeline_completo` — existem para execução manual (`databricks bundle run`),
comentário em `databricks.yml:36-37`.

### 3.2 `pipeline_completo` — grafo completo (ORDEM REAL)

22 tasks. Predecessores (`depends_on`) e sucessores derivados:

| # | `task_key` | Linha | entry_point | Predecessores | Sucessores |
|---|---|---|---|---|---|
| 1 | `t0_unity_catalog` | `:297` | `job_unity_catalog_schemas` | — (raiz) | `t1_extracao_acoes`, `t1_extracao_bcb`, `t1_extracao_world_bank`, `t6_bronze_clientes` |
| 2 | `t1_extracao_acoes` | `:317` | `job_extracao_acoes` | `t0_unity_catalog` (`:320`) | `t2_silver_acoes` |
| 3 | `t1_extracao_bcb` | `:338` | `job_extracao_bcb` | `t0_unity_catalog` (`:341`) | `t2_silver_bcb` |
| 4 | `t1_extracao_world_bank` | `:359` | `job_extracao_world_bank` | `t0_unity_catalog` (`:362`) | `t2_silver_world_bank` |
| 5 | `t6_bronze_clientes` | `:380` | `job_bronze_clientes` | `t0_unity_catalog` (`:383`) | `t6_bronze_ordens`, `t6_silver_clientes` |
| 6 | `t6_bronze_ordens` | `:402` | `job_bronze_ordens` | `t6_bronze_clientes` (`:405`) | `t6_silver_ordens` |
| 7 | `t6_silver_clientes` | `:424` | `job_silver_clientes` | `t6_bronze_clientes` (`:427`) | `t7_corretora_analises` |
| 8 | `t6_silver_ordens` | `:446` | `job_silver_ordens` | `t6_bronze_ordens` (`:449`) | `t7_corretora_analises` |
| 9 | `t2_silver_acoes` | `:468` | `job_silver_acoes` | `t1_extracao_acoes` (`:471`) | `t3_anomalias`, `t3_performance`, `t3_acoes_cambio` |
| 10 | `t2_silver_bcb` | `:489` | `job_silver_bcb` | `t1_extracao_bcb` (`:492`) | `t3_bcb`, `t3_acoes_cambio` |
| 11 | `t2_silver_world_bank` | `:510` | `job_silver_world_bank` | `t1_extracao_world_bank` (`:513`) | `t3_world_bank` |
| 12 | `t3_anomalias` | `:531` | `job_gold_anomalias` | `t2_silver_acoes` (`:534`) | `t8_lakehouse_monitoring` |
| 13 | `t3_performance` | `:552` | `job_gold_performance` | `t2_silver_acoes` (`:555`) | `t8_lakehouse_monitoring` |
| 14 | `t3_bcb` | `:573` | `job_gold_bcb` | `t2_silver_bcb` (`:576`) | `t8_lakehouse_monitoring` |
| 15 | `t3_world_bank` | `:594` | `job_gold_world_bank` | `t2_silver_world_bank` (`:597`) | `t8_lakehouse_monitoring` |
| 16 | `t3_acoes_cambio` | `:615` | `job_gold_acoes_vs_cambio` | `t2_silver_acoes`, `t2_silver_bcb` (`:618-619`) | `t8_lakehouse_monitoring` |
| 17 | `t7_corretora_analises` | `:638` | `job_corretora_analises` | `t6_silver_clientes`, `t6_silver_ordens` (`:641-642`) | `t9_scd`, `t3_fraude` |
| 18 | `t9_scd` | `:660` | `job_scd` | `t7_corretora_analises` (`:663`) | `t8_lakehouse_monitoring` |
| 19 | `t3_fraude` | `:681` | `job_gold_fraude` | `t7_corretora_analises` (`:684`) | `t8_lakehouse_monitoring` |
| 20 | `t8_lakehouse_monitoring` | `:703` | `job_lakehouse_monitoring` | `t3_anomalias`, `t3_performance`, `t3_bcb`, `t3_world_bank`, `t3_acoes_cambio`, `t3_fraude`, `t9_scd` (`:708-714`) | `t8b_uc_registro` |
| 21 | `t8b_uc_registro` | `:732` | `job_unity_catalog` | `t8_lakehouse_monitoring` (`:737`) | `t4_observabilidade` |
| 22 | `t4_observabilidade` | `:755` | `job_observabilidade` | `t8b_uc_registro` (`:758`) | — (folha) |

Grafo em texto:

```
t0_unity_catalog
├── t1_extracao_acoes ─── t2_silver_acoes ──┬── t3_anomalias ──────────┐
│                                            ├── t3_performance ───────┤
│                                            └──┐                      │
├── t1_extracao_bcb ───── t2_silver_bcb ─────┬─ t3_acoes_cambio ───────┤
│                                            └── t3_bcb ───────────────┤
├── t1_extracao_world_bank ─ t2_silver_world_bank ─ t3_world_bank ─────┤
└── t6_bronze_clientes ┬── t6_bronze_ordens ── t6_silver_ordens ──┐    │
                       └── t6_silver_clientes ────────────────────┤    │
                                                                  ↓    │
                                        t7_corretora_analises ────┬────┤
                                                                  ├─ t9_scd ──┤
                                                                  └─ t3_fraude┤
                                                                              ↓
                                                    t8_lakehouse_monitoring ──→ t8b_uc_registro ──→ t4_observabilidade
```

### 3.3 Recursos, timeouts, retries

Não há pools nem locks. **Cada task cria seu próprio job cluster** (`new_cluster`):

| Atributo | Valor | Linha (exemplo) |
|---|---|---|
| `spark_version` | `${var.spark_version}` = `14.3.x-scala2.12` | `databricks.yml:9` |
| `node_type_id` | `Standard_DS3_v2` (todas as 30 tasks) | `:57,:308,…` |
| `num_workers` | `${var.gold_workers}` (hk 2 / prod 4) na maioria; `${var.sql_workers}` (hk 1 / prod 2) em `t8_lakehouse_monitoring` (`:725`), `t8b_uc_registro` (`:748`), `t4_observabilidade` (`:769`) | — |
| `azure_attributes` | `SPOT_WITH_FALLBACK_AZURE`, `first_on_demand: 1` | `:60-62` |
| `spark_env_vars.ENVIRONMENT` | `${var.environment}` — presente em **todas** as tasks | `:64-65` etc. |
| `timeout_seconds` | 1800 em `t0_unity_catalog` (`:313`); **3600** nas demais; **0** em `streaming_continuous` (`:246`) | — |
| `max_retries` | **2** em todas; **0** em `streaming_continuous` (`:247`) | — |
| `libraries` | `- whl: ./dist/*.whl` em todas as tasks; wheel construído por `artifacts.case_santander_wheel` (`:29-33`) | — |

### 3.4 Targets (ambientes do bundle)

| Target | `mode` | Variáveis | Workspace | Override |
|---|---|---|---|---|
| `hk` (`:786`) | `development` | spark 14.3, gold 2, sql 1, `environment: hk`, `enable_streaming: false` (`:789-793`) | `${env.DATABRICKS_HOST_HK}`, root `/Workspace/Shared/bundles/case-santander-hk` (`:794-796`) | `streaming_to_gold_continuous.schedule.pause_status: PAUSED` (`:801-806`) |
| `prod` (`:808`) | `production` | spark 14.3, gold 4, sql 2, `environment: prod`, `enable_streaming: true` (`:811-815`) | `${env.DATABRICKS_HOST_PROD}`, root `/Workspace/Shared/bundles/case-santander-prod` (`:816-818`) | — |

### 3.5 DAG Airflow (espelho gerado)

`dags/dag_pipeline_santander.py` — `dag_id="pipeline_corretora_santander"`
(`:44`), `schedule_interval="0 6 * * *"` (`:47`), `start_date=datetime(2026,1,1)`
(`:48`), `catchup=False` (`:49`), `retries=2` / `retry_delay=5min` (`:23-24`).
Contém as **mesmas 22 tasks** e as **mesmas 22 arestas** (`:186-206`), via
`DatabricksSubmitRunOperator` com `existing_cluster_id` (`:35`) — note que aqui
usa **cluster existente**, não job cluster: divergência de execução em relação a
`databricks.yml` (§9).

### 3.6 Pipelines de CI (não são jobs de dado)

| Workflow | Gatilho | Papel |
|---|---|---|
| `.github/workflows/ci-cd.yml` | push `main`/`develop`, PR `main` | lint, testes, `databricks bundle deploy -t hk` e `-t prod` |
| `.github/workflows/deploy-databricks.yml` | push / `workflow_dispatch` | valida `databricks.yml`, sintaxe Python e DAG; deploy dev/prod |
| `.github/workflows/test.yml` | push `main`/`develop`, PR `main` | `pytest` de data quality |
| `.github/workflows/update-airflow-dag.yml` | push `main`/`develop` | regenera e valida o DAG Airflow a partir de `databricks.yml` |

---

## 4. INVENTÁRIO DE ARTEFATOS

### 4.1 Tabelas por camada — **30** (6 bronze + 7 silver + 17 gold)

Todos os nomes resolvidos pelo mapa da §2. Prefixo `hk_` mostrado; `prod_` análogo.

#### Bronze — 6

| # | Tabela | Formato / mecanismo de escrita | Escrita em |
|---|---|---|---|
| 1 | `case_santander.hk_bronze.acoes` | Parquet no ADLS → registrada Delta gerenciada | `src/ingestion/yahoo_finance.py:71-73`; registro `jobs/job_unity_catalog.py:44,71` |
| 2 | `case_santander.hk_bronze.bcb` | Parquet particionado `extracao=` → Delta | `src/ingestion/bcb.py:181-183`; registro `jobs/job_unity_catalog.py:45,63,71` |
| 3 | `case_santander.hk_bronze.world_bank` | Parquet particionado `extracao=` → Delta | `src/ingestion/world_bank.py:92-94`; registro `jobs/job_unity_catalog.py:46,63,71` |
| 4 | `case_santander.hk_bronze.kafka` | Avro (Event Hub Capture) → Delta | escrita externa: `terraform/modules/event_hub/main.tf:30-43`; registro `jobs/job_unity_catalog.py:47,65,71` |
| 5 | `case_santander.hk_bronze.clientes` | Delta gerenciada, MERGE/upsert | `src/ingestion/clientes_kaggle.py:104-105` → `src/utils/delta.py:27-44` |
| 6 | `case_santander.hk_bronze.ordens` | Delta gerenciada, MERGE/upsert | `src/ingestion/ordens_simuladas.py:87-88` → `src/utils/delta.py:27-44` |

#### Silver — 7

| # | Tabela | Formato / mecanismo | Escrita em |
|---|---|---|---|
| 7 | `case_santander.hk_silver.acoes` | Delta externa (path, particionada `ano`,`mes`) | dado `src/transformation/silver_acoes.py:66-71`; registro `:89` |
| 8 | `case_santander.hk_silver.bcb` | Delta externa (path) | dado `src/transformation/silver_bcb.py:37-41`; registro `:58` |
| 9 | `case_santander.hk_silver.world_bank` | Delta externa (path) | dado `src/transformation/silver_world_bank.py:34-38`; registro `:55` |
| 10 | `case_santander.hk_silver.clientes` | Delta gerenciada, MERGE | `src/transformation/silver_clientes.py:37` |
| 11 | `case_santander.hk_silver.ordens` | Delta gerenciada, MERGE | `src/transformation/silver_ordens.py:31` |
| 12 | `case_santander.hk_silver.streaming` | Delta via `writeStream.toTable` + `option("path")` | `jobs/job_streaming_continuous.py:105-113` |
| 13 | `case_santander.hk_silver.clientes_scd` | Delta gerenciada, SCD Type 2 | `src/clients/scd.py:82` → `:44-62` |

#### Gold — 17

| # | Tabela | Mecanismo | Escrita em |
|---|---|---|---|
| 14 | `case_santander.hk_gold.anomalias` | Delta externa (path) + registro | dado `src/gold/anomalias.py:41-45`; registro `jobs/job_unity_catalog.py:106,113` |
| 15 | `case_santander.hk_gold.performance_acoes` | Delta externa (path) + registro | dado `src/gold/performance.py:42-45`; registro `jobs/job_unity_catalog.py:105,113` |
| 16 | `case_santander.hk_gold.indicadores_bcb` | `saveAsTable` | `src/gold/bcb_analise.py:70-72` |
| 17 | `case_santander.hk_gold.contexto_macroeconomico` | `saveAsTable` | `src/gold/world_bank_analise.py:93-95` |
| 18 | `case_santander.hk_gold.acoes_vs_cambio` | `saveAsTable` | `src/gold/correlacao_acoes_cambio.py:112-114` |
| 19 | `case_santander.hk_gold.deteccao_fraude` | `saveAsTable` | `src/gold/fraude.py:81-85` |
| 20 | `case_santander.hk_gold.posicao_clientes` | `saveAsTable` | `jobs/job_corretora_analises.py:63-65` |
| 21 | `case_santander.hk_gold.score_risco_clientes` | `saveAsTable` | `jobs/job_corretora_analises.py:116-118` |
| 22 | `case_santander.hk_gold.perfil_clientes` | `saveAsTable` | `jobs/job_corretora_analises.py:135-137` |
| 23 | `case_santander.hk_gold.ordens_consolidadas` | `saveAsTable` | `jobs/job_corretora_analises.py:151-153` |
| 24 | `case_santander.hk_gold.ranking_acoes_perfil` | `saveAsTable` | `jobs/job_corretora_analises.py:167-169` |
| 25 | `case_santander.hk_gold.observabilidade` | `saveAsTable` | `jobs/job_observabilidade.py:33-37` |
| 26 | `case_santander.hk_gold.fraude_streaming` | `saveAsTable` | `src/gold/streaming_gold.py:89-93` |
| 27 | `case_santander.hk_gold.anomalias_intraday` | `saveAsTable` | `src/gold/streaming_gold.py:155-159` |
| 28 | `case_santander.hk_gold.volume_intraday` | `saveAsTable` | `src/gold/streaming_gold.py:227-231` |
| 29 | `case_santander.hk_gold.ranking_acoes_realtime` | `saveAsTable` | `src/gold/streaming_gold.py:295-299` |
| 30 | `case_santander.hk_gold.score_risco_scd` | SCD Type 2 | `src/clients/scd.py:108` → `:44-62` |

### 4.2 Arquivos / paths de entrada e saída (ADLS) — **12 reais**

Padrão: `abfss://<container>@<storage_account>.dfs.core.windows.net/<prefixo>`,
com `storage_account` = `stcasesantander{hk|prod}`.

| # | Path | E/S | Onde |
|---|---|---|---|
| 1 | `bronze/acoes/data=<data>/` | saída | `src/ingestion/yahoo_finance.py:71-73` |
| | `bronze/acoes/` | entrada | `src/transformation/silver_acoes.py:22,27`; `jobs/job_unity_catalog.py:44,65` |
| 2 | `bronze/bcb/extracao=<data>/` | saída | `src/ingestion/bcb.py:181-183` |
| | `bronze/bcb/` | entrada | `src/transformation/silver_bcb.py:11,16`; `jobs/job_unity_catalog.py:45,63` |
| 3 | `bronze/world_bank/extracao=<data>/` | saída | `src/ingestion/world_bank.py:92-94` |
| | `bronze/world_bank/` | entrada | `src/transformation/silver_world_bank.py:11,16`; `jobs/job_unity_catalog.py:46,63` |
| 4 | `bronze/kafka/` | saída (externa: Event Hub Capture) | `terraform/modules/event_hub/main.tf:37-42` |
| | `bronze/kafka/` | entrada (Auto Loader) | `jobs/job_streaming_continuous.py:51,78`; `jobs/job_streaming.py:41,73`; `jobs/job_unity_catalog.py:47,65` |
| 5 | `silver/acoes/` | saída | `src/transformation/silver_acoes.py:23,66-71` |
| | `silver/acoes/` | entrada | `src/gold/anomalias.py:10,16`; `src/gold/performance.py:19,24` |
| 6 | `silver/bcb/` | saída | `src/transformation/silver_bcb.py:12,37-41` |
| 7 | `silver/world_bank/` | saída | `src/transformation/silver_world_bank.py:12,34-38` |
| 8 | `silver/streaming/` | saída | `jobs/job_streaming_continuous.py:52,112`; `jobs/job_streaming.py:42` |
| 9 | `silver/checkpoints/streaming_continuous/` (+ `/schema`) | estado de streaming | `jobs/job_streaming_continuous.py:53,76,108` |
| 10 | `silver/checkpoints/streaming/` | estado de streaming (**job órfão**, §9) | `jobs/job_streaming.py:43` |
| 11 | `gold/anomalias/` | saída | `src/gold/anomalias.py:11,45` · entrada de registro `jobs/job_unity_catalog.py:106,113` |
| 12 | `gold/performance_acoes/` | saída | `src/gold/performance.py:20,45` · entrada de registro `jobs/job_unity_catalog.py:105,113` |
| — | `bronze/` (raiz) | *health check* | `src/health/health_check.py:47-48` (módulo sem call site, §9) |

### 4.3 Tópicos / filas — **1**

| # | Recurso | Nome real | Onde |
|---|---|---|---|
| 1 | Azure Event Hub (namespace `evhcasesantander-<env>`) | `transacoes-financeiras-<env>` | `src/config/environment.py:80-81`; `terraform/modules/event_hub/main.tf:3-11,13-19`; regra de acesso `pipeline-access` (listen/send/manage) `:46-54` |

Produtores: `scripts/eventhub_producer.py:86-98`,
`scripts/eventhub_producer_advanced.py:167-221`.
Consumo: **indireto** — o Event Hub Capture (`main.tf:30-43`, Avro, 60 s /
10 MiB) grava em `bronze/kafka/` e os jobs de streaming leem do ADLS com Auto
Loader (`jobs/job_streaming_continuous.py:73-78`). Nenhum job consome o Event
Hub diretamente.

### 4.4 Integrações externas — **8**

| # | Sistema | Protocolo / lib | Onde |
|---|---|---|---|
| 1 | Yahoo Finance | `yfinance` | `src/ingestion/yahoo_finance.py:5,49-50` (`period="2y"`) |
| 2 | API SGS do Banco Central | HTTPS REST | `src/ingestion/bcb.py:49-52` — séries `11` (selic), `1` (câmbio USD/BRL), `433` (ipca), janela `01/04/2021`–`01/04/2026` (`:29-30,:158-164`) |
| 3 | API World Bank | HTTPS REST | `src/ingestion/world_bank.py:46` — indicadores `NY.GDP.MKTP.KD.ZG` (pib_anual) e `SL.UEM.TOTL.ZS` (desemprego) (`:80-81`) |
| 4 | Kaggle Datasets API | HTTPS + Basic auth | `src/ingestion/clientes_kaggle.py:21,51-56` — dataset `mathchi/churn-for-bank-customers` |
| 5 | Azure Event Hub | AMQP (`azure-eventhub`) | `scripts/eventhub_producer.py:15,86`; `scripts/eventhub_producer_advanced.py:29,167` |
| 6 | Azure Data Lake Storage Gen2 | `abfss://` + OAuth client credentials | `src/config/settings.py:36-48` |
| 7 | Azure Key Vault (via secret scope Databricks) | `dbutils.secrets.get` | `src/config/secrets.py:32-37`; scope = `kv-case-santander-<env>` (`src/config/environment.py:79`) |
| 8 | Databricks Workspace API (Lakehouse Monitoring) | `databricks-sdk` `WorkspaceClient` | `jobs/job_lakehouse_monitoring.py:17,25,44-53` |

### 4.5 Módulos com lógica de negócio — **21**

| # | Módulo | Regra de domínio que carrega |
|---|---|---|
| 1 | `src/ingestion/yahoo_finance.py` | extração de 9 tickers B3, 2 anos, tag de ambiente |
| 2 | `src/ingestion/bcb.py` | 3 séries SGS, validação HTTP/JSON em 8 etapas, retry exponencial |
| 3 | `src/ingestion/world_bank.py` | 2 indicadores macro do Brasil |
| 4 | `src/ingestion/clientes_kaggle.py` | anonimização LGPD (hash), `classificar_perfil` (`:24-30`), `classificar_saldo` (`:33-41`), renomeação de 8 colunas (`:96-101`) |
| 5 | `src/ingestion/ordens_simuladas.py` | simulação determinística de ordens (seed 42, 1000 clientes, `:22-23`), id determinístico (`:69`) |
| 6 | `src/transformation/silver_acoes.py` | `variacao_diaria_pct`, `amplitude_diaria`, mapa ticker→`empresa` (`:43-53`) e ticker→`setor` (`:54-60`), filtros de qualidade |
| 7 | `src/transformation/silver_bcb.py` | tipagem de data `dd/MM/yyyy`, dedup por (`data`,`indicador`) |
| 8 | `src/transformation/silver_world_bank.py` | compatibilidade de schema `ano`/`data` (`:20-25`) |
| 9 | `src/transformation/silver_clientes.py` | `faixa_etaria` (`:25-28`), `score_categoria` (`:29-33`) |
| 10 | `src/transformation/silver_ordens.py` | tipagem `data_ordem`, derivação `ano`/`mes` |
| 11 | `src/gold/anomalias.py` | z-score por ticker, limiar \|z\|>2, `tipo_anomalia` |
| 12 | `src/gold/performance.py` | agregação por ticker/setor/ano, volatilidade |
| 13 | `src/gold/bcb_analise.py` | SMA 7d, volatilidade 30d, IPCA acumulado 12m, alertas de câmbio/inflação |
| 14 | `src/gold/world_bank_analise.py` | `cenario_macro`, `alerta_risco`, `impacto_bolsa` |
| 15 | `src/gold/correlacao_acoes_cambio.py` | correlação móvel 90d ação×câmbio, `sensibilidade_cambio`, `recomendacao` |
| 16 | `src/gold/fraude.py` | 4 regras de alerta + `score_fraude` + `requer_revisao` |
| 17 | `src/gold/streaming_gold.py` | 4 análises intraday (fraude, anomalia, volume, ranking) |
| 18 | `src/clients/scd.py` | SCD Type 2 genérico (`:12-65`) + duas aplicações de domínio |
| 19 | `src/observability/monitoring.py` | métricas de qualidade por tabela, limiar 95% (`:73`) |
| 20 | `src/security/hashing.py` | anonimização SHA256+salt de `CustomerId`/`Surname`/`Email` |
| 21 | `src/quality/data_quality.py` | gates de qualidade (completeness / row_count / nulls) |

### 4.6 Jobs agendados — **2** (de 9 definições)

| # | Job | Cron | Timezone | Estado |
|---|---|---|---|---|
| 1 | `pipeline_completo` | `0 0 6 * * ?` (06:00 diário) | `America/Sao_Paulo` | `UNPAUSED` (`databricks.yml:780-782`) |
| 2 | `streaming_to_gold_continuous` | `0 */5 * * * ?` (a cada 5 min) | `America/Sao_Paulo` | `UNPAUSED` em `prod` (`:282-284`); `PAUSED` em `hk` (`:804-806`) |

Adicionalmente, o **DAG Airflow** `pipeline_corretora_santander` replica o
mesmo agendamento (`0 6 * * *`, `dags/dag_pipeline_santander.py:47`).

### 4.7 Outros artefatos contáveis

| Artefato | Qtd | Evidência |
|---|---|---|
| Módulos `jobs/*.py` (excl. `__init__.py`) | 26 | `ls jobs/job_*.py` |
| Entry points `console_scripts` | 24 | `setup.py:27-50` |
| Tasks no `pipeline_completo` | 22 | `databricks.yml` |
| Tasks no DAG Airflow | 22 | `dags/dag_pipeline_santander.py` |
| Secrets do Key Vault | 7 | `src/config/secrets.py:42-72` |
| Tabelas com Liquid Clustering | 7 | `jobs/job_unity_catalog.py:122-130` |
| Tabelas com Change Data Feed | 3 | `jobs/job_unity_catalog.py:144-148` |
| Tabelas em Lakehouse Monitoring | 11 | `jobs/job_lakehouse_monitoring.py:27-39` |
| Tabelas em OPTIMIZE/VACUUM | 10 | `jobs/job_observabilidade.py:44-55` |
| Tabelas monitoradas por `executar_monitoramento` | 14 | `src/observability/monitoring.py:90-105` |
| Módulos Terraform | 7 | `terraform/modules/*` |
| Workflows GitHub Actions | 4 | `.github/workflows/*.yml` |
| Arquivos de teste | 6 | `tests/test_*.py` |
| Dashboards Grafana | 6 | `docker/grafana/provisioning/dashboards/*.json` + `*.yml` |

---

## 5. INVENTÁRIO DE RELAÇÕES

### 5.1 Joins — **7 lógicos / 8 sites**

| # | Arquivo:linha | Esquerda ⨝ Direita | Chave | Tipo |
|---|---|---|---|---|
| 1 | `jobs/job_corretora_analises.py:61` | `df_posicao` (de `hk_silver.ordens`, `:32`) ⨝ `df_clientes` (de `hk_silver.clientes`, `:34-37`) | `hash_cliente` | `left`, broadcast |
| 2 | `src/gold/fraude.py:46` | `hk_silver.ordens` (`:22`) ⨝ `hk_gold.score_risco_clientes` (`:23-27`) | `hash_cliente` | `left`, broadcast |
| 2b | `src/gold/fraude.py:50` | *mesma relação*, ramo de fallback sort-merge | `hash_cliente` | `left` |
| 3 | `src/gold/correlacao_acoes_cambio.py:56` | `hk_silver.acoes` agregada (`:29-32`) ⨝ `hk_silver.bcb` filtrada `indicador='cambio_usd_brl'` (`:34-38`) | `date` | `left` |
| 4 | `src/gold/streaming_gold.py:52` | `hk_silver.streaming` (`:37`) ⨝ `hk_gold.performance_acoes` (`:41-45`) | `ticker` | `left`, broadcast |
| 5 | `src/gold/streaming_gold.py:138` | `hk_silver.streaming` agregada por hora (`:113,116-123`) ⨝ `hk_gold.performance_acoes` (`:127-131`) | `ticker` | `left`, broadcast |
| 6 | `src/gold/streaming_gold.py:209` | `hk_silver.streaming` agregada (`:182,185-196`) ⨝ `hk_gold.performance_acoes` (`:200-204`) | `ticker` | `left`, broadcast |
| 7 | `src/gold/streaming_gold.py:280` | `hk_silver.streaming` agregada (`:253,256-267`) ⨝ `hk_gold.performance_acoes` (`:271-275`) | `ticker` | `left`, broadcast |

### 5.2 Chaves de MERGE / SCD (o equivalente a FK neste lakehouse)

Não há FK declarada (Delta não impõe). As chaves de casamento efetivas:

| # | Tabela | Chave | Onde |
|---|---|---|---|
| 1 | `hk_bronze.clientes` | `hash_cliente` | `src/ingestion/clientes_kaggle.py:105` |
| 2 | `hk_bronze.ordens` | `id_ordem` | `src/ingestion/ordens_simuladas.py:88` |
| 3 | `hk_silver.clientes` | `hash_cliente` | `src/transformation/silver_clientes.py:37` |
| 4 | `hk_silver.ordens` | `id_ordem` | `src/transformation/silver_ordens.py:31` |
| 5 | `hk_silver.clientes_scd` | `hash_cliente` | `src/clients/scd.py:82` |
| 6 | `hk_gold.score_risco_scd` | `hash_cliente` | `src/clients/scd.py:108` |

**Relacionamento referencial de fato:** `hash_cliente` liga
`bronze.clientes → bronze.ordens` (`src/ingestion/ordens_simuladas.py:46-47,74`)
→ `silver.clientes` / `silver.ordens` → `gold.posicao_clientes` →
`gold.score_risco_clientes` → `gold.deteccao_fraude` e `gold.score_risco_scd`.
`ticker` liga `silver.acoes` ↔ `silver.ordens` ↔ `silver.streaming` ↔
`gold.performance_acoes` e as 4 gold de streaming.

### 5.3 Linhagem tabela→tabela (arestas de dado)

| # | Origem | Destino | Onde |
|---|---|---|---|
| 1 | *(Yahoo Finance)* | `bronze.acoes` | `src/ingestion/yahoo_finance.py:71-73` |
| 2 | *(API BCB)* | `bronze.bcb` | `src/ingestion/bcb.py:181-183` |
| 3 | *(API World Bank)* | `bronze.world_bank` | `src/ingestion/world_bank.py:92-94` |
| 4 | *(Kaggle)* | `bronze.clientes` | `src/ingestion/clientes_kaggle.py:104-105` |
| 5 | *(Event Hub Capture)* | `bronze.kafka` | `terraform/modules/event_hub/main.tf:37-42` |
| 6 | `bronze.clientes` | `bronze.ordens` | `src/ingestion/ordens_simuladas.py:46,87-88` |
| 7 | `bronze.acoes` (path) | `silver.acoes` | `src/transformation/silver_acoes.py:27,66-71` |
| 8 | `bronze.bcb` (path) | `silver.bcb` | `src/transformation/silver_bcb.py:16,37-41` |
| 9 | `bronze.world_bank` (path) | `silver.world_bank` | `src/transformation/silver_world_bank.py:16,34-38` |
| 10 | `bronze.clientes` | `silver.clientes` | `src/transformation/silver_clientes.py:24,37` |
| 11 | `bronze.ordens` | `silver.ordens` | `src/transformation/silver_ordens.py:24,31` |
| 12 | `bronze.kafka` (path) | `silver.streaming` | `jobs/job_streaming_continuous.py:78,113` |
| 13 | `silver.acoes` (path) | `gold.anomalias` | `src/gold/anomalias.py:16,45` |
| 14 | `silver.acoes` (path) | `gold.performance_acoes` | `src/gold/performance.py:24,45` |
| 15 | `silver.bcb` | `gold.indicadores_bcb` | `src/gold/bcb_analise.py:24,70-72` |
| 16 | `silver.world_bank` | `gold.contexto_macroeconomico` | `src/gold/world_bank_analise.py:24,93-95` |
| 17 | `silver.acoes` + `silver.bcb` | `gold.acoes_vs_cambio` | `src/gold/correlacao_acoes_cambio.py:31,36,112-114` |
| 18 | `silver.ordens` + `silver.clientes` | `gold.posicao_clientes` | `jobs/job_corretora_analises.py:32,36,65` |
| 19 | `gold.posicao_clientes` (df em memória) | `gold.score_risco_clientes` | `jobs/job_corretora_analises.py:70,118` |
| 20 | `silver.clientes` | `gold.perfil_clientes` | `jobs/job_corretora_analises.py:131,137` |
| 21 | `silver.ordens` | `gold.ordens_consolidadas` | `jobs/job_corretora_analises.py:147,153` |
| 22 | `silver.ordens` | `gold.ranking_acoes_perfil` | `jobs/job_corretora_analises.py:162,169` |
| 23 | `silver.ordens` + `gold.score_risco_clientes` | `gold.deteccao_fraude` | `src/gold/fraude.py:22,26,85` |
| 24 | `silver.clientes` | `silver.clientes_scd` | `src/clients/scd.py:79,82` |
| 25 | `gold.score_risco_clientes` | `gold.score_risco_scd` | `src/clients/scd.py:105,108` |
| 26 | `silver.streaming` + `gold.performance_acoes` | `gold.fraude_streaming` | `src/gold/streaming_gold.py:37,41-45,93` |
| 27 | `silver.streaming` + `gold.performance_acoes` | `gold.anomalias_intraday` | `src/gold/streaming_gold.py:113,127-131,159` |
| 28 | `silver.streaming` + `gold.performance_acoes` | `gold.volume_intraday` | `src/gold/streaming_gold.py:182,200-204,231` |
| 29 | `silver.streaming` + `gold.performance_acoes` | `gold.ranking_acoes_realtime` | `src/gold/streaming_gold.py:253,271-275,299` |
| 30 | 14 tabelas silver+gold | `gold.observabilidade` | `src/observability/monitoring.py:90-105`; `jobs/job_observabilidade.py:37` |
| 31 | `gold.observabilidade` | *(marca d'água CDC dos jobs de streaming)* | `jobs/job_streaming_to_gold_continuous.py:56-60` |

### 5.4 Dependências entre módulos (chamadas job → src)

| Job | Importa / chama |
|---|---|
| `job_unity_catalog_schemas.py` | `src.config.tables:{SCHEMA_BRONZE,SCHEMA_SILVER,SCHEMA_GOLD}` (`:30`) |
| `job_extracao_acoes.py` | `src.ingestion.yahoo_finance.extrair_acoes` (`:18,34`) |
| `job_extracao_bcb.py` | `src.ingestion.bcb.extrair_bcb` (`:18,34`) |
| `job_extracao_world_bank.py` | `src.ingestion.world_bank.extrair_world_bank` (`:18,34`) |
| `job_bronze_clientes.py` | `src.ingestion.clientes_kaggle.extrair_clientes` (`:16,34`) |
| `job_bronze_ordens.py` | `src.ingestion.ordens_simuladas.gerar_ordens` (`:17,35`) |
| `job_silver_acoes.py` | `src.transformation.silver_acoes.transformar_acoes` (`:18,34`) |
| `job_silver_bcb.py` | `src.transformation.silver_bcb.transformar_bcb` (`:18,34`) |
| `job_silver_world_bank.py` | `src.transformation.silver_world_bank.transformar_world_bank` (`:18,34`) |
| `job_silver_clientes.py` | `src.transformation.silver_clientes.transformar_clientes` (`:16,34`) |
| `job_silver_ordens.py` | `src.transformation.silver_ordens.transformar_ordens` (`:16,34`) |
| `job_gold_anomalias.py` | `src.gold.anomalias.detectar_anomalias` (`:28,45`) |
| `job_gold_performance.py` | `src.gold.performance.calcular_performance` (`:28,45`) |
| `job_gold_bcb.py` | `src.gold.bcb_analise.analisar_indicadores_bcb` (`:28,45`) |
| `job_gold_world_bank.py` | `src.gold.world_bank_analise.analisar_contexto_macro` (`:28`) |
| `job_gold_acoes_vs_cambio.py` | `src.gold.correlacao_acoes_cambio.correlacionar_acoes_cambio` (`:29,46`) |
| `job_gold_fraude.py` | `src.gold.fraude.detectar_fraude` (`:26,36`) |
| `job_corretora_analises.py` | *(lógica inline, sem módulo `src/` de negócio)* |
| `job_scd.py` | `src.clients.scd.{aplicar_scd_clientes,aplicar_scd_score_risco}` (`:15,24-25`) |
| `job_observabilidade.py` | `src.observability.monitoring.executar_monitoramento` (`:15,25`) |
| `job_lakehouse_monitoring.py` | `databricks.sdk.WorkspaceClient` (`:17,25`) |
| `job_unity_catalog.py` | `src.config.tables.register_external_table` (`:18,113`) |
| `job_streaming_continuous.py` | `src.config.settings.configure_adls` (`:30,49`) |
| `job_streaming_to_gold_continuous.py` | `src.gold.streaming_gold.{detectar_fraude_streaming, detectar_anomalias_intraday, calcular_volume_intraday, calcular_ranking_realtime}` (`:28-33,85-97`) |

Dependências internas de `src/`:

| Consumidor | Provedor | Onde |
|---|---|---|
| `src/ingestion/clientes_kaggle.py` | `src/security/hashing.py`, `src/utils/delta.py`, `src/config/{tables,secrets,logging}` | `:14-18` |
| `src/ingestion/ordens_simuladas.py` | `src/utils/delta.py`, `src/config/{tables,logging}` | `:14-16` |
| `src/ingestion/{yahoo_finance,bcb,world_bank}.py` | `src/ingestion/api_wrapper.rate_limiter`, `src/config/environment` | `yahoo:8-9`, `bcb:7-8`, `wb:6-7` |
| `src/ingestion/{yahoo_finance,world_bank}.py` | `src/utils/retry.retry_on_connection_error` | `yahoo:10,13`; `wb:8,37` |
| `src/transformation/{silver_acoes,silver_bcb,silver_world_bank}.py` | `src/quality/data_quality.DataQualityValidator`, `src/config/tables.register_external_table` | `acoes:9-10,78,89`; `bcb:5-6,48,58`; `wb:5-6,45,55` |
| `src/transformation/{silver_clientes,silver_ordens}.py` | `src/utils/delta.merge_ou_cria` | `:14,37` / `:14,31` |
| `src/utils/delta.py` | `delta.tables.DeltaTable`, `src/config/logging` | `:5,7` |
| `src/security/hashing.py` | `src/config/secrets.get_secret` | `:9,15` |
| `src/config/tables.py` | `src/config/environment.EnvironmentConfig` | `:26,28` |
| `src/config/secrets.py` | `src/config/environment.get_config` | `:7,34` |
| `src/config/settings.py` | `src/config/secrets.get_secret` (lazy, `:60`) | `:58-69` |
| `config/config.py` (pacote raiz) | `src/config/environment.EnvironmentConfig` | `:15,17,19` |

### 5.5 Chamadas entre serviços

| Origem | Destino | Onde |
|---|---|---|
| Todos os jobs | Databricks Connect / Spark Cluster | `DatabricksSession.builder.getOrCreate()` em cada `jobs/job_*.py` |
| Todos os jobs de ingestão/silver | Azure Key Vault (via secret scope) | `get_secret(...)` — `src/config/secrets.py:32-37` |
| Todos os jobs de ingestão/silver | ADLS Gen2 (OAuth) | `configure_adls(...)` — `src/config/settings.py:36-48` |
| `job_lakehouse_monitoring` | Databricks REST (`lakehouse_monitors.create/get`) | `:44-53` |
| `scripts/eventhub_producer*.py` | Azure Event Hub | `:86-98` / `:167-221` |
| GitHub Actions | Databricks CLI (`bundle deploy`) | `.github/workflows/ci-cd.yml`, `deploy-databricks.yml` |
| Airflow | Databricks (`DatabricksSubmitRunOperator`) | `dags/dag_pipeline_santander.py:31-40` |
| Grafana | Postgres do Airflow | `docker/grafana/provisioning/datasources/airflow-postgres.yml` |

---

## 6. INVENTÁRIO DE CAMPOS FINAIS (insumo da Fase V)

> Critério: "tabelas finais" = **as 17 tabelas gold**, as saídas terminais do
> pipeline. As 7 silver vão em §6.2 como camada intermediária, e as 6 bronze em
> §6.3, para que a varredura tenha o denominador completo se precisar.
> Só nomes, sem regra — a regra é da Fase V.

### 6.1 GOLD — 17 tabelas, **227 campos**

**1. `gold.anomalias` — 12** *(`src/gold/anomalias.py:33-39`)*
`date`, `ticker`, `empresa`, `setor`, `open`, `close`, `volume`,
`variacao_diaria_pct`, `zscore`, `anomalia`, `tipo_anomalia`,
`data_processamento`

**2. `gold.performance_acoes` — 13** *(`src/gold/performance.py:28-39`)*
`ticker`, `empresa`, `setor`, `ano`, `preco_medio`, `preco_minimo`,
`preco_maximo`, `variacao_media_pct`, `volatilidade`, `volume_medio`,
`volume_total`, `dias_negociados`, `data_processamento`

**3. `gold.indicadores_bcb` — 13** *(`src/gold/bcb_analise.py:28-66`; colunas de pivot vêm dos `indicador` de `src/ingestion/bcb.py:158-164`)*
`data`, `selic`, `cambio_usd_brl`, `ipca`, `selic_media_7d`,
`selic_volatilidade_30d`, `cambio_media_7d`, `cambio_variacao_pct`,
`ipca_acumulado_12m`, `tendencia_selic`, `alerta_cambio`, `alerta_inflacao`,
`data_processamento`

**4. `gold.contexto_macroeconomico` — 11** *(`src/gold/world_bank_analise.py:28-89`; pivot de `src/ingestion/world_bank.py:80-81`)*
`ano`, `pib_anual`, `desemprego`, `pib_variacao_pct`,
`desemprego_variacao_pct`, `tendencia_pib`, `tendencia_desemprego`,
`cenario_macro`, `alerta_risco`, `impacto_bolsa`, `data_processamento`

**5. `gold.acoes_vs_cambio` — 13** *(`src/gold/correlacao_acoes_cambio.py:102-108`)*
`date`, `ticker`, `preco_medio`, `variacao_media_pct`, `cambio`,
`cambio_variacao_pct`, `correlacao_cambio`, `sensibilidade_cambio`,
`alerta_desacoplamento`, `recomendacao`, `ano`, `mes`, `data_processamento`

**6. `gold.deteccao_fraude` — 26** *(`src/gold/fraude.py:46-79`; herda `silver.ordens` §6.2-5 + 3 colunas de `gold.score_risco_clientes` `:24-25` + 7 derivadas)*
`id_ordem`, `hash_cliente`, `perfil_risco`, `faixa_saldo`, `ticker`, `preco`,
`quantidade`, `valor_total`, `tipo`, `corretora`, `status`, `data_ordem`,
`data_extracao`, `ano`, `mes`, `data_processamento`, `score_risco`,
`categoria_risco`, `limite_operacional`, `alerta_valor_alto`,
`alerta_volume_suspeito`, `alerta_preco_atipico`, `alerta_perfil_incompativel`,
`total_alertas`, `score_fraude`, `requer_revisao`

**7. `gold.posicao_clientes` — 15** *(`jobs/job_corretora_analises.py:41-61`)*
`hash_cliente`, `ticker`, `quantidade_liquida`, `total_comprado`,
`total_vendido`, `total_ordens`, `ordens_executadas`, `ordens_canceladas`,
`valor_investido`, `resultado_estimado`, `situacao`, `data_processamento`,
`perfil_risco`, `faixa_saldo`, `score_credito`

**8. `gold.score_risco_clientes` — 19** *(`jobs/job_corretora_analises.py:70-114`)*
`hash_cliente`, `perfil_risco`, `faixa_saldo`, `score_credito`, `num_ativos`,
`total_ordens`, `total_canceladas`, `valor_total_investido`,
`posicoes_descoberto`, `resultado_medio`, `taxa_cancelamento_pct`,
`score_credito_norm`, `score_perfil`, `score_saldo`, `score_comportamento`,
`score_risco`, `categoria_risco`, `limite_operacional`, `data_processamento`

**9. `gold.perfil_clientes` — 10** *(`jobs/job_corretora_analises.py:122-134`)*
`perfil_risco`, `faixa_etaria`, `score_categoria`, `pais`, `total_clientes`,
`saldo_medio`, `score_medio`, `salario_medio`, `total_churn`, `taxa_churn_pct`

**10. `gold.ordens_consolidadas` — 10** *(`jobs/job_corretora_analises.py:141-150`)*
`ticker`, `perfil_risco`, `faixa_saldo`, `tipo`, `status`, `ano`,
`total_ordens`, `volume_total`, `preco_medio`, `qtd_media`

**11. `gold.ranking_acoes_perfil` — 5** *(`jobs/job_corretora_analises.py:157-166`)*
`ticker`, `perfil_risco`, `total_ordens`, `volume_total`, `preco_medio`

**12. `gold.observabilidade` — 9** *(`src/observability/monitoring.py:54-64`)*
`camada`, `tabela`, `data_verificacao`, `total_registros`, `total_nulos`,
`total_duplicatas`, `versao_cdf`, `qualidade_pct`, `tempo_seg`

**13. `gold.fraude_streaming` — 17** *(`src/gold/streaming_gold.py:79-87`)*
`id_transacao`, `timestamp`, `ticker`, `tipo`, `preco`, `quantidade`,
`valor_total`, `preco_medio`, `volatilidade`, `alerta_volume_suspeito`,
`alerta_preco_atipico`, `alerta_valor_elevado`, `alerta_desvio_historico`,
`total_alertas`, `score_fraude`, `requer_revisao`, `data_processamento`

**14. `gold.anomalias_intraday` — 13** *(`src/gold/streaming_gold.py:116-153`)*
`ticker`, `hora`, `preco_medio_hora`, `valor_total_hora`, `volume_hora`,
`total_transacoes_hora`, `preco_medio`, `volatilidade`, `desvio_historico_rs`,
`zscore_intraday`, `anomalia`, `tipo_anomalia`, `data_processamento`

**15. `gold.volume_intraday` — 13** *(`src/gold/streaming_gold.py:185-225`)*
`ticker`, `hora`, `total_transacoes`, `volume_hora`, `valor_total_hora`,
`preco_medio_hora`, `volume_compras`, `volume_vendas`, `volume_medio`,
`pct_volume_diario`, `alerta_volume_intraday`, `pressao_compradora`,
`data_processamento`

**16. `gold.ranking_acoes_realtime` — 16** *(`src/gold/streaming_gold.py:256-293`)*
`ticker`, `total_transacoes`, `volume_total`, `valor_total`,
`preco_medio_atual`, `total_compras`, `total_vendas`, `preco_minimo`,
`preco_maximo`, `empresa`, `setor`, `preco_medio_historico`,
`variacao_vs_historico_pct`, `tendencia`, `data_processamento`, `rank_volume`

**17. `gold.score_risco_scd` — 12** *(`src/clients/scd.py:100-108` + `:21-24`)*
`hash_cliente`, `perfil_risco`, `faixa_saldo`, `score_credito`, `score_risco`,
`categoria_risco`, `limite_operacional`, `num_ativos`, `total_ordens`,
`data_inicio`, `data_fim`, `atual`

**Total GOLD: 12+13+13+11+13+26+15+19+10+10+5+9+17+13+13+16+12 = 227**

### 6.2 SILVER — 7 tabelas, **95 campos**

**1. `silver.acoes` — 17** *(`src/transformation/silver_acoes.py:30-64`, sobre o schema do yfinance em `src/ingestion/yahoo_finance.py:50-53,67-68`)*
`date`, `open`, `high`, `low`, `close`, `volume`, `ticker`, `data_extracao`,
`ambiente`, `ano`, `mes`, `trimestre`, `variacao_diaria_pct`,
`amplitude_diaria`, `empresa`, `setor`, `data_processamento`

**2. `silver.bcb` — 9** *(`src/transformation/silver_bcb.py:23-33`)*
`data`, `valor`, `indicador`, `data_extracao`, `ambiente`, `ano`, `mes`,
`trimestre`, `data_processamento`

**3. `silver.world_bank` — 8** *(`src/transformation/silver_world_bank.py:20-32`)*
`ano`, `valor`, `indicador`, `data_extracao`, `fonte`, `ambiente`, `extracao`,
`data_processamento`

**4. `silver.clientes` — 19** *(`src/transformation/silver_clientes.py:24-34` sobre `bronze.clientes`)*
`id_cliente`, `hash_cliente`, `sobrenome_masked`, `score_credito`, `pais`,
`genero`, `idade`, `anos_cliente`, `saldo`, `faixa_saldo`, `num_produtos`,
`perfil_risco`, `ativo`, `churn`, `salario_estimado`, `data_extracao`,
`faixa_etaria`, `score_categoria`, `data_processamento`

**5. `silver.ordens` — 16** *(`src/transformation/silver_ordens.py:24-28` sobre `bronze.ordens`)*
`id_ordem`, `hash_cliente`, `perfil_risco`, `faixa_saldo`, `ticker`, `preco`,
`quantidade`, `valor_total`, `tipo`, `corretora`, `status`, `data_ordem`,
`data_extracao`, `ano`, `mes`, `data_processamento`

**6. `silver.streaming` — 13** *(`jobs/job_streaming_continuous.py:62-70,88-101`)*
`timestamp`, `ticker`, `preco`, `quantidade`, `tipo`, `corretora`,
`id_transacao`, `hora`, `minuto`, `valor_total`, `alerta_volume`,
`alerta_preco`, `processado_em`

**7. `silver.clientes_scd` — 13** *(`src/clients/scd.py:74-82` + `:21-24`)*
`id_cliente`, `hash_cliente`, `sobrenome_masked`, `perfil_risco`,
`score_credito`, `faixa_saldo`, `faixa_etaria`, `score_categoria`, `ativo`,
`churn`, `data_inicio`, `data_fim`, `atual`

### 6.3 BRONZE — 6 tabelas, **57 campos**

**1. `bronze.acoes` — 11 + partição** *(`src/ingestion/yahoo_finance.py:50-53,67-68`)*
`date`, `open`, `high`, `low`, `close`, `volume`, `dividends`, `stock_splits`,
`ticker`, `data_extracao`, `ambiente` · partição de path `data=<YYYY-MM-DD>`

**2. `bronze.bcb` — 5 + partição** *(`src/ingestion/bcb.py:115-127`)*
`data`, `valor`, `indicador`, `data_extracao`, `ambiente` ·
partição `extracao=<YYYY-MM-DD>`

**3. `bronze.world_bank` — 6 + partição** *(`src/ingestion/world_bank.py:60,66-70`)*
`ano`, `valor`, `indicador`, `data_extracao`, `fonte`, `ambiente` ·
partição `extracao=<YYYY-MM-DD>`

**4. `bronze.kafka` — 6** *(envelope Avro do Event Hub Capture, `terraform/modules/event_hub/main.tf:26-29`; leitura do campo `Body` em `jobs/job_streaming_continuous.py:83-85`)*
`SequenceNumber`, `Offset`, `EnqueuedTimeUtc`, `SystemProperties`,
`Properties`, `Body`

**5. `bronze.clientes` — 16** *(`src/ingestion/clientes_kaggle.py:91-101`)*
`id_cliente`, `hash_cliente`, `sobrenome_masked`, `score_credito`, `pais`,
`genero`, `idade`, `anos_cliente`, `saldo`, `faixa_saldo`, `num_produtos`,
`perfil_risco`, `ativo`, `churn`, `salario_estimado`, `data_extracao`

**6. `bronze.ordens` — 13** *(`src/ingestion/ordens_simuladas.py:71-85`)*
`id_ordem`, `hash_cliente`, `perfil_risco`, `faixa_saldo`, `ticker`, `preco`,
`quantidade`, `valor_total`, `tipo`, `corretora`, `status`, `data_ordem`,
`data_extracao`

**Denominador total das 30 tabelas: 227 (gold) + 95 (silver) + 57 (bronze) = 379 campos.**

---

## 7. VALIDAÇÃO DO INVENTÁRIO

Para cada contagem: **bruto** (o que uma varredura ingênua traria) → **real**,
com cada falso positivo nomeado.

### 7.1 Tabelas — bruto 34 → **real 30** · 4 falsos positivos

| Falso positivo | Onde | Por que não conta |
|---|---|---|
| `{SCHEMA_SILVER}.<nome_da_tabela>` | `src/config/tables.py:15` | **Placeholder em docstring.** O próprio módulo alerta (`:19-23`) que um nome real ali seria contado como leitura por qualquer varredura. Não é tabela. |
| `case_santander.silver.X` | `src/config/tables.py:60` | Docstring narrando um bug corrigido. Além disso o nome está **sem prefixo de ambiente** — não existe. |
| `case_santander.bronze.{tabela}` (5 ocorrências) | `jobs/job_unity_catalog.py:79,86,88,89,91` | **Bloco inteiramente comentado** (`:77-92`), junto do dict `tabelas_bronze_delta` também comentado (`:51-54`). As tabelas que ele criaria (`bronze.clientes`, `bronze.ordens`) já são criadas por `src/ingestion/*` — não geram entrada nova. |
| `<catalog>.<env>_bronze.kafka` | `notebooks/case_presentation.py:391` | Dentro de uma **string `print("""…""")`** didática (`:370-395`), com placeholders `<catalog>`/`<env>` que não resolvem. A tabela real já está contada (#4). |
| `case_santander.{t}` com `t` em lista literal | `notebooks/case_presentation.py:1305-1312` | Bloco `# MAGIC` de markdown ilustrando um fluxo LGPD hipotético. Comentário, não código. As 7 tabelas citadas já estão contadas. |
| `f"{SCHEMA_GOLD}"` (sem `.tabela`) | `jobs/job_lakehouse_monitoring.py:47` | É `output_schema_name` — **schema**, não tabela. |
| `TB_BRONZE_CLIENTES` / `TB_SILVER_CLIENTES` / `TB_GOLD_FRAUDE` | `tests/test_data_quality.py:18-20` | Constantes de teste apontando para tabelas já contadas (#5, #10, #19). Não criam artefato. |

### 7.2 Os cinco padrões de cegueira — verificação item a item

**(a) Nome de tabela em DOCSTRING contado como uso real — 2 falsos positivos encontrados**

| Ocorrência | Local | Veredito |
|---|---|---|
| `{SCHEMA_SILVER}.<nome_da_tabela>` | `src/config/tables.py:15` | **FP confirmado.** Placeholder deliberado. |
| `FROM case_santander.silver.X` | `src/config/tables.py:60` | **FP confirmado.** Narrativa em docstring. |

Docstrings que citam tabelas **reais** mas com placeholder de ambiente
(`<catalog>.<env>_…`) — `jobs/job_bronze_clientes.py:4`,
`jobs/job_bronze_ordens.py:4-6`, `jobs/job_silver_clientes.py:4-5`,
`jobs/job_silver_ordens.py:4-5`, `jobs/job_gold_*.py:5-10`,
`jobs/job_unity_catalog_schemas.py:19-21`, `src/transformation/silver_*.py:4-5`,
`src/ingestion/{clientes_kaggle,ordens_simuladas}.py:4-6` — **não** foram
contados como uso; servem só como confirmação cruzada do mapa §2.

**(b) Nome montado por variável de loop `f"{SCHEMA_X}.{variavel}"` — 4 falsos NEGATIVOS**

Uma varredura por `\{SCHEMA_\w+\}\.[a-z_]+` **perde 4 tabelas bronze**, porque o
nome vem da chave de um dict:

| Site | Fonte dos nomes | Tabelas invisíveis à regex |
|---|---|---|
| `jobs/job_unity_catalog.py:71` — `.saveAsTable(f"{SCHEMA_BRONZE}.{tabela}")` | dict `tabelas_bronze_parquet`, chaves em `:44-47` | `bronze.acoes`, `bronze.bcb`, `bronze.world_bank`, `bronze.kafka` |
| `jobs/job_unity_catalog.py:113` — `register_external_table(spark,"gold",tabela,path)` | dict `tabelas_gold`, chaves em `:105-106` | `gold.performance_acoes`, `gold.anomalias` — **já visíveis** por outros sites (`jobs/job_observabilidade.py:50`, `:49`), então **não** são falso negativo líquido |

**Falsos negativos líquidos deste padrão: 4.**

**(c) FQN em variável intermediária antes do write — 2 sites, 0 falso negativo líquido**

| Site | Padrão | Veredito |
|---|---|---|
| `src/utils/delta.py:28,41` | `fqn` (parâmetro) → `DeltaTable.forName(spark, fqn)` / `.saveAsTable(fqn)` | O FQN **não é literal aqui**; é resolvido nos 4 call sites (§7.2d). Contado lá. |
| `src/clients/scd.py:28,48,61` | `tabela_uc` (parâmetro) → `.saveAsTable(tabela_uc)` | Idem: resolvido em `:82` e `:108`. |
| `src/config/tables.py:66-68` | `fqn = table_fqn(layer, name)` → `CREATE TABLE … {fqn}` | Resolvido nos 6 call sites de `register_external_table`. |
| `jobs/job_unity_catalog.py:57,110,133,150` | `for tabela, path in …` → uso de `tabela` | Coberto em (b). |
| `jobs/job_observabilidade.py:58-61` | `for tabela, cols_zorder in tabelas_manutencao` | Os 10 FQNs são literais em `:45-54`, capturados pela regex. |
| `jobs/job_lakehouse_monitoring.py:42-53` | `for tabela in tabelas` | Os 11 FQNs são literais em `:28-38`. |
| `src/observability/monitoring.py:110-113` | `for tabela in tabelas` | Os 14 FQNs são literais em `:91-104`. |

Uma varredura que só olhasse `.saveAsTable(f"…")` perderia estes 2 writes
genéricos, mas ambos são **funções**, não instâncias de tabela.
**Falsos negativos líquidos: 0** (todos resolvidos em (d)).

**(d) FQN passado como ARGUMENTO para helper genérico que escreve — 6 sites, 0 falso negativo líquido**

| Site | Helper | FQN resolvido | É a única evidência da tabela? |
|---|---|---|---|
| `src/transformation/silver_clientes.py:37` | `merge_ou_cria` (`src/utils/delta.py:10`) | `{SCHEMA_SILVER}.clientes` | Não (também lida em 8 lugares) |
| `src/transformation/silver_ordens.py:31` | `merge_ou_cria` | `{SCHEMA_SILVER}.ordens` | Não |
| `src/ingestion/clientes_kaggle.py:104-105` | `merge_ou_cria` | `{SCHEMA_BRONZE}.clientes` | Não (lida em `ordens_simuladas.py:46`, `silver_clientes.py:24`) |
| `src/ingestion/ordens_simuladas.py:87-88` | `merge_ou_cria` | `{SCHEMA_BRONZE}.ordens` | **Sim para escrita** — é o único write de `bronze.ordens` |
| `src/clients/scd.py:82` | `aplicar_scd_type2` (`src/clients/scd.py:12`) | `{SCHEMA_SILVER}.clientes_scd` | **Sim para escrita** |
| `src/clients/scd.py:108` | `aplicar_scd_type2` | `{SCHEMA_GOLD}.score_risco_scd` | **Sim para escrita** |

Todos os 6 têm o FQN **literal no argumento** → a regex de FQN os captura.
**Falsos negativos líquidos: 0.** *Porém*, uma varredura que procurasse só
`.saveAsTable(`/`.save(` como sinal de escrita perderia **6 escritas**
(`bronze.clientes`, `bronze.ordens`, `silver.clientes`, `silver.ordens`,
`silver.clientes_scd`, `gold.score_risco_scd`) — este é o falso negativo real do
padrão, e por isso a §8 traz comando dedicado.

**(e) Parênteses aninhados antes do FQN no argumento — 2 sites, 0 falso negativo líquido**

| Site | Chamada | Por que cega regex ingênua |
|---|---|---|
| `src/ingestion/clientes_kaggle.py:104-105` | `merge_ou_cria(spark, spark.createDataFrame(df_final), f"{SCHEMA_BRONZE}.clientes", "hash_cliente", CTX)` | O 2º argumento é uma chamada aninhada **e** a chamada quebra em 2 linhas — um regex de linha única com `\([^)]*\)` falha duas vezes |
| `src/ingestion/ordens_simuladas.py:87-88` | `merge_ou_cria(spark, spark.createDataFrame(pd.DataFrame(ordens)), f"{SCHEMA_BRONZE}.ordens", "id_ordem", CTX)` | **Dois níveis** de aninhamento (`createDataFrame(pd.DataFrame(...))`) + quebra de linha |

Testado: o comando `grep -rhoE '\{SCHEMA_(BRONZE|SILVER|GOLD)\}\.[a-z_0-9]+'` da
§8 **não** se importa com parênteses (não ancora na chamada), então captura os
dois. **Falsos negativos líquidos: 0** — mas qualquer comando que tentasse
casar `merge_ou_cria\([^)]*"…"` perderia ambos.

### 7.3 Paths ADLS — bruto 18 → **real 12** · 6 falsos positivos

| Falso positivo | Onde declarado | Por que não conta |
|---|---|---|
| `bronze/clientes/` | `src/config/settings.py:21`; `src/config/environment.py:133`; `jobs/job_unity_catalog.py:52` (comentado) | Declarado em dicionários de config, **nunca lido nem escrito** por código executável. A tabela é Delta gerenciada via `merge_ou_cria`. |
| `bronze/ordens/` | `src/config/settings.py:22`; `src/config/environment.py:134`; `jobs/job_unity_catalog.py:53` (comentado) | Idem. |
| `silver/clientes/` | `src/config/settings.py:27`; `src/config/environment.py:140` | Idem — `silver.clientes` é gerenciada. |
| `silver/ordens/` | `src/config/settings.py:28`; `src/config/environment.py:141` | Idem. |
| `gold/acoes_vs_cambio/` | `src/config/settings.py:31`; `src/config/environment.py:145` | `src/gold/correlacao_acoes_cambio.py:112` grava por `saveAsTable` (gerenciada). O próprio `jobs/job_unity_catalog.py:98-102` documenta que ler esse path falha. |
| `gold/observabilidade/` | `src/config/settings.py:32`; `src/config/environment.py:146` | `jobs/job_observabilidade.py:37` grava por `saveAsTable`. Path nunca usado. |

Também não contam como path de dado: `silver/checkpoints/*` são **estado de
streaming**, listados separadamente (#9 e #10 da §4.2), e
`src/health/health_check.py:47` é sonda de conectividade em módulo sem call site.

### 7.4 Joins — bruto 9 → **real 8 sites / 7 lógicos** · 1 falso positivo

| Falso positivo | Onde | Por quê |
|---|---|---|
| `", ".join(self.jobs.keys())` | `src/pipeline/dynamic_pipeline.py:279` | `str.join` do Python dentro de um template de geração de código. Nada a ver com Spark. |

`src/gold/fraude.py:46` e `:50` são **o mesmo join lógico** em dois ramos
(broadcast vs sort-merge), decididos por `use_broadcast` (`:45`).
Contagem lógica: **7**.

### 7.5 Módulos com lógica de negócio — bruto 32 → **real 21** · 11 falsos positivos

| Falso positivo | Por que não conta |
|---|---|
| `src/config/environment.py` | Configuração de ambiente, sem regra de domínio |
| `src/config/settings.py` | Paths + auth ADLS |
| `src/config/secrets.py` | Wrapper do Key Vault |
| `src/config/tables.py` | Resolução de nomes |
| `src/config/logging.py` | Logging |
| `src/ingestion/api_wrapper.py` | Rate limiter genérico, infra de rede |
| `src/utils/delta.py` | Helper Delta genérico (recebe FQN e chave por parâmetro) |
| `src/utils/retry.py` | Decorator de retry genérico |
| `src/pipeline/dynamic_pipeline.py` | Framework de orquestração — **sem call site no pipeline** (§9) |
| `src/health/health_check.py` | Framework de health check — **sem call site nenhum** (§9) |
| `src/streaming/__init__.py` | Arquivo vazio (0 bytes); o diretório `src/streaming/` não tem outro módulo |

### 7.6 Jobs — três contagens distintas, todas reais

| Contagem | Valor | O que é |
|---|---|---|
| Definições de job no bundle | **9** | `databricks.yml`, `resources.jobs.*` |
| Jobs **com** schedule | **2** | `pipeline_completo`, `streaming_to_gold_continuous` |
| Tasks do `pipeline_completo` | **22** | idênticas às 22 tasks do DAG Airflow |
| Módulos `jobs/job_*.py` | **26** | arquivos em disco |
| Entry points declarados | **24** | `setup.py:27-50` |

Falsos positivos / negativos aqui:

| Item | Veredito |
|---|---|
| `jobs/job_streaming.py` | **Órfão.** Não está em `setup.py`, nem em `databricks.yml`, nem no DAG. Não conta como job executável. |
| `jobs/job_streaming_to_gold.py` | **Órfão.** Idem. |
| `jobs/__init__.py` | Arquivo vazio, não é job. |
| Os 6 jobs `t3_gold_*` | **Reais** como definição de job, mas **duplicam** tasks do `pipeline_completo` — não representam execução adicional agendada. |
| `job_carga_sql_*.py`, `job_clientes_ordens.py`, `job_clientes_silver.py` | **Não existem.** Só sobrou `.pyc` em `jobs/__pycache__/`. Não contam (§9). |

### 7.7 Integrações externas — bruto 12 → **real 8** · 4 falsos positivos

| Falso positivo | Por quê |
|---|---|
| Databricks Connect (`DatabricksSession`) | É o runtime de execução, não sistema externo integrado |
| Grafana + Postgres (`docker/grafana/*`) | Observa o **Airflow**, não o lakehouse — fora do pipeline de dado |
| GitHub Actions / Databricks CLI | CI/CD, não integração de dado |
| Terraform providers (azurerm/azuread/databricks/random) | Provisionamento, não runtime |

### 7.8 Resumo dos números do CONTRATO

| Contagem | Bruto | **REAL** | FP |
|---|---|---|---|
| Tabelas | 34 | **30** (6 B + 7 S + 17 G) | 4 |
| Campos das tabelas finais (gold) | — | **227** | — |
| Campos de todas as 30 tabelas | — | **379** | — |
| Paths ADLS | 18 | **12** | 6 |
| Tópicos / filas | 1 | **1** | 0 |
| Integrações externas | 12 | **8** | 4 |
| Módulos com lógica de negócio | 32 | **21** | 11 |
| Definições de job | 9 | **9** | 0 |
| Jobs agendados | 9 | **2** | 7 (sem schedule) |
| Tasks do pipeline | 22 | **22** | 0 |
| Módulos `jobs/*.py` executáveis | 26 | **24** | 2 (órfãos) |
| Joins (sites) | 9 | **8** | 1 |
| Joins (lógicos) | — | **7** | — |
| Arestas de linhagem | — | **31** | — |
| Chaves MERGE/SCD | — | **6** | — |

---

## 8. COMANDOS DE REGENERAÇÃO

Todos executados a partir da raiz do repositório, em **Git Bash**. Cada um foi
testado e reproduz o número declarado.

### 8.1 Procedência

```bash
git rev-parse --abbrev-ref HEAD          # → release/segunda-chance-dm
git rev-parse --short HEAD               # → f7265c7
git status --short                       # → M README.md · M docs/README.md · ?? EXECUTIVE_SUMMARY.md
```
*Falsos negativos conhecidos:* nenhum.

### 8.2 Tabelas — FQN literal → **26** (dos 30)

```bash
grep -rhoE '\{SCHEMA_(BRONZE|SILVER|GOLD)\}\.[a-z_0-9]+' --include=*.py src jobs \
  | sort -u | wc -l          # → 26
```
**Falsos negativos conhecidos (4, padrão *b*):** `bronze.acoes`, `bronze.bcb`,
`bronze.world_bank`, `bronze.kafka` — nomes vêm de chaves de dict, não de
literal na linha do write. Recupere com:

```bash
sed -n '43,48p' jobs/job_unity_catalog.py    # dict tabelas_bronze_parquet
sed -n '104,107p' jobs/job_unity_catalog.py  # dict tabelas_gold
```
26 + 4 = **30**.

*Falso positivo conhecido:* o `[a-z_0-9]+` **exclui** o placeholder
`{SCHEMA_SILVER}.<nome_da_tabela>` de `src/config/tables.py:15` porque `<` não
está na classe. Para vê-lo (e confirmar que é FP):

```bash
grep -rnoE '\{SCHEMA_(BRONZE|SILVER|GOLD)\}\.[^"'"'"' ),]+' --include=*.py src jobs | grep '<'
# → src/config/tables.py:15:{SCHEMA_SILVER}.<nome_da_tabela>
```

### 8.3 Escritas de tabela — todos os mecanismos

```bash
# saveAsTable / toTable
grep -rn 'saveAsTable\|\.toTable(' --include=*.py src jobs
# → 20 linhas; 2 são comentário (job_unity_catalog.py:86,99) e 3 são
#   helpers genéricos (utils/delta.py:41, clients/scd.py:48,61)

# helper de MERGE (padrão d/e)
grep -rn 'merge_ou_cria(' --include=*.py src jobs | grep -v 'def merge_ou_cria'   # → 4 call sites

# helper de SCD (padrão d)
grep -rn 'aplicar_scd_type2(' --include=*.py src jobs | grep -v 'def aplicar'     # → 2 call sites

# registro de tabela externa
grep -rn 'register_external_table(' --include=*.py src jobs | grep -v 'def register'  # → 5 call sites

# escrita por path
grep -rnE '\.save\(|\.parquet\(' --include=*.py src jobs | grep -v 'read'         # → 8 (1 é docstring)
```
*Falsos negativos conhecidos:* `bronze.kafka` não aparece em nenhum destes — é
escrita **fora do repositório**, pelo Azure Event Hub Capture. Verificar em
`terraform/modules/event_hub/main.tf:30-43`.

### 8.4 Paths ADLS — **18 referências → 12 reais**

```bash
grep -rnoE 'abfss://[a-z]+@\{storage_account\}\.dfs\.core\.windows\.net/[a-z_/]*' \
  --include=*.py src jobs | sort -t: -k1,1 -k2,2n
```
*Falsos positivos conhecidos (6):* as 16 entradas de
`src/config/settings.py:17-32` e as de `src/config/environment.py:129-146` são
**declarações de dicionário**. Para separar as que realmente têm E/S:

```bash
grep -rnE '\.save\(|\.parquet\(|\.load\(|checkpointLocation|option\("path"' \
  --include=*.py src jobs | grep -v 'read.*format.*json'
```

### 8.5 Jobs e orquestração

```bash
# 9 definições de job
awk '/^  jobs:/{f=1;next} /^targets:/{f=0} f && /^    [a-z_0-9]+:/{gsub(/[ :]/,"");print}' \
  databricks.yml            # → 9 nomes

# 22 tasks do pipeline_completo
awk '/^    pipeline_completo:/{f=1} /^targets:/{f=0} f && /^      - task_key:/{print $3}' \
  databricks.yml | wc -l    # → 22

# 22 arestas de dependência (uma por linha `- task_key:` sob depends_on)
grep -n 'depends_on' -A3 databricks.yml | grep -c 'task_key'   # aproximado; conferir §3.2

# 2 schedules
grep -c 'quartz_cron_expression' databricks.yml   # → 2
grep -n 'quartz_cron_expression\|pause_status\|timezone_id' databricks.yml

# 24 entry points
grep -cE '^\s+"job_[a-z_]+ = jobs\.' setup.py     # → 24

# 22 tasks do DAG Airflow
grep -cE '^\s+task_id="' dags/dag_pipeline_santander.py   # → 22

# jobs órfãos (em disco mas sem entry point)
for f in jobs/job_*.py; do b=$(basename $f .py); \
  grep -q "$b = jobs" setup.py || echo "ORPHAN: $f"; done
# → jobs/job_streaming.py · jobs/job_streaming_to_gold.py
```
*Falso negativo conhecido do `grep -c 'quartz_cron_expression'`:* não distingue
schedule ativo de pausado. O override `PAUSED` de `hk` está em
`databricks.yml:801-806` e precisa ser lido à parte.

### 8.6 Joins — **9 hits → 8 reais**

```bash
grep -rn '\.join(' --include=*.py src jobs   # → 9
```
*Falso positivo conhecido (1):* `src/pipeline/dynamic_pipeline.py:279` é
`str.join` do Python. Filtro:

```bash
grep -rn '\.join(' --include=*.py src jobs | grep -v 'dynamic_pipeline'   # → 8
```

### 8.7 Campos de uma tabela final

Não há comando genérico — os schemas são construídos por encadeamento de
`withColumn`/`agg`/`select`, não declarados. O procedimento reprodutível é:

```bash
# 1. achar o write
grep -rn 'saveAsTable(f"{SCHEMA_GOLD}.<tabela>"' --include=*.py src jobs
# 2. ler o bloco que constrói o DataFrame até o write (o `.select(...)` final,
#    quando existe, é a lista definitiva)
sed -n '<inicio>,<fim>p' <arquivo>
```
Tabelas com `.select(...)` explícito (lista fechada, sem ambiguidade):
`gold.anomalias` (`src/gold/anomalias.py:33-39`), `gold.acoes_vs_cambio`
(`src/gold/correlacao_acoes_cambio.py:102-108`), `gold.fraude_streaming`
(`src/gold/streaming_gold.py:79-87`).
*Falso negativo conhecido:* as demais dependem do schema herdado da origem —
ver §9 para os dois casos em que isso não é 100% determinável estaticamente.

### 8.8 Referências obsoletas na documentação

```bash
for f in job_clientes_ordens job_clientes_silver job_carga_sql_acoes \
         job_carga_sql_clientes job_carga_sql_fraude job_carga_sql_macro \
         job_carga_sql_streaming; do
  printf "%s: fonte=%s pyc=%s\n" "$f" \
    "$(test -f jobs/$f.py && echo SIM || echo NAO)" \
    "$(test -f jobs/__pycache__/$f.cpython-311.pyc && echo SIM || echo NAO)"
done
# → todos: fonte=NAO pyc=SIM
```

---

## 9. ZONAS DE RISCO

### 9.1 Não foi possível determinar estaticamente

| # | Item | Por quê |
|---|---|---|
| 1 | Schema exato de `bronze.acoes` / `silver.acoes` | As colunas vêm de `yf.Ticker(...).history(period="2y")` (`src/ingestion/yahoo_finance.py:50`), definidas pela versão instalada do `yfinance` (`setup.py:8` só pina `>=0.2.37`). Documentei o schema padrão de ações (`Open/High/Low/Close/Volume/Dividends/Stock Splits`); versões recentes podem adicionar `Capital Gains` para fundos. O `.drop("dividends","stock_splits")` (`silver_acoes.py:64`) e o `mergeSchema:true` (`:69`) toleram variação — mas a contagem de campos de `silver.acoes` (17) pode variar. |
| 2 | Colunas de pivot de `gold.indicadores_bcb` e `gold.contexto_macroeconomico` | `.pivot("indicador")` (`bcb_analise.py:30`, `world_bank_analise.py:30`) gera **uma coluna por valor distinto de `indicador` presente no dado**. Derivei da lista de séries requisitadas (`bcb.py:158-164` → 3; `world_bank.py:80-81` → 2), mas se uma série falhar na extração (o código tolera: `bcb.py:167-171`, `world_bank.py:83-87`), a coluna correspondente **não existe** naquele dia — e as expressões que a referenciam (`selic_media_7d`, `pib_variacao_pct`, …) quebram. |
| 3 | Contagem de registros / volumetria | Nenhuma amostra de dado no repositório. Os números do README (`8.530 reg`, `5.341 linhas`, `~7.900 linhas`) não são verificáveis a partir do código. |
| 4 | Estado real dos schedules após deploy | `targets.hk.mode: development` (`databricks.yml:787`) pausa schedules como efeito colateral do Databricks CLI. O `databricks.yml:798-800` reconhece isso e adiciona um override explícito só para `streaming_to_gold_continuous`. O estado efetivo de `pipeline_completo` em `hk` **depende do comportamento da CLI**, não do arquivo. |

### 9.2 Código sem uso aparente

| # | Artefato | Evidência |
|---|---|---|
| 1 | `jobs/job_streaming.py` (127 linhas) | Ausente de `setup.py:27-50`, de `databricks.yml` e de `dags/dag_pipeline_santander.py`. Seu docstring cita a task `t5_streaming` (`:6`) que **não existe** em lugar nenhum. É o único produtor de `silver/checkpoints/streaming/`. |
| 2 | `jobs/job_streaming_to_gold.py` (112 linhas) | Idem; docstring cita `t10_streaming_gold` (`:16`), task inexistente. É duplicata quase literal de `job_streaming_to_gold_continuous.py`. |
| 3 | `src/health/health_check.py` (≈190 linhas) | `HealthChecker` e `health_check_decorator` **não têm nenhum call site** fora do próprio arquivo (`grep -rn 'HealthChecker\|health_check'` → só definições e docstring). |
| 4 | `src/pipeline/dynamic_pipeline.py` (≈380 linhas) | Único consumidor é `scripts/auto_generate_dag.py:14`, um script auxiliar que **não é chamado por nenhum job nem workflow**. `.github/workflows/update-airflow-dag.yml` usa `scripts/sync_airflow_from_databricks.py`, não este. |
| 5 | `src/streaming/__init__.py` | Diretório `src/streaming/` contém apenas um `__init__.py` de 0 byte. A lógica de streaming vive em `jobs/` e `src/gold/streaming_gold.py`. |
| 6 | `src/gold/fraude.py:33-43` | Bloco de estimativa de tamanho para decidir broadcast. `df_score._sc.parallelize([]).toDF().name` — `DataFrame` do PySpark não tem `.name`. Lança `AttributeError` sempre, capturado em `:41`, forçando `use_broadcast=True`. O ramo `:50` (sort-merge) é **inalcançável**. |
| 7 | `jobs/job_unity_catalog.py:51-54,77-92` | Dois blocos comentados (dict `tabelas_bronze_delta` e o loop que o consumiria). |
| 8 | 5 gold sem consumidor no código | `gold.indicadores_bcb`, `gold.contexto_macroeconomico`, `gold.acoes_vs_cambio`, `gold.perfil_clientes`, `gold.ordens_consolidadas`, `gold.ranking_acoes_perfil` — produzidas e nunca lidas por outro job. São terminais de consumo externo (SQL Warehouse / notebook), mas **nada no repositório as lê**. |
| 9 | Módulos `__pycache__` sem fonte | `jobs/__pycache__/` contém `.pyc` de 7 módulos removidos: `job_carga_sql_{acoes,clientes,fraude,macro,streaming}`, `job_clientes_ordens`, `job_clientes_silver`. |
| 10 | `src/config/settings.py:15-33` (`get_paths`) | Função duplicada de `src/config/environment.py:120-156` (`get_paths`), sem nenhum call site. |
| 11 | `src/security/hashing.py:64-68,71-90,93-104` | `hash_email`, `anonymize_customer_row`, `generate_random_salt` sem call site fora de `scripts/generate_salt.py`. |

### 9.3 Divergências entre documentação existente e código

| # | Documento | Afirma | Código mostra |
|---|---|---|---|
| 1 | `INVENTARIO.md` (versão anterior, sobrescrita por este) | Referencia `jobs/job_clientes_ordens.py` **23 vezes** e `jobs/job_clientes_silver.py` **6 vezes**, com linhas específicas (ex.: `:135,156`, `:204,225`, `:243,265`, `:277,299`) | **Nenhum dos dois arquivos existe** na working tree. Só restaram `.pyc` em `jobs/__pycache__/`. O commit `f7265c7` (`refactor: uma tabela por job, sem misturar camadas`) os quebrou em `job_bronze_clientes`, `job_bronze_ordens`, `job_silver_clientes`, `job_silver_ordens`. |
| 2 | `INVENTARIO.md` anterior | "os 22 `console_scripts` de `setup.py`" | São **24** (`setup.py:27-50`). |
| 3 | `README.md:99` | "Lakehouse Monitoring — **6 tabelas** monitoradas" | `jobs/job_lakehouse_monitoring.py:27-39` lista **11**; `src/observability/monitoring.py:90-105` lista **14**. |
| 4 | `README.md:544` | "Streaming (job separado…): `job_streaming.py` / `job_streaming_continuous.py` → `job_streaming_to_gold.py` / …" | `job_streaming.py` e `job_streaming_to_gold.py` **não estão implantados** (nem em `setup.py`, nem em `databricks.yml`). |
| 5 | `README.md:752` | "Power BI conectado ao SQL Warehouse sobre as tabelas Gold" | Não há nenhum artefato de Power BI, conexão ou SQL Warehouse no repositório. Os dashboards existentes (`docker/grafana/`) apontam para o **Postgres do Airflow**. |
| 6 | `dags/dag_pipeline_santander.py:35` | Executa com `existing_cluster_id` (`CLUSTER_ID` default `0401-150803-wefgy1hc`, `:17`) | `databricks.yml` usa **job clusters** (`new_cluster`) em todas as 30 tasks. Dois modelos de execução conflitantes para o mesmo grafo. |
| 7 | `dags/dag_pipeline_santander.py:36-38` | `spark_python_task` apontando para `{REPO_PATH}/jobs/*.py` | `databricks.yml` usa `python_wheel_task` com `entry_point`. Se o wheel for a única forma de instalar `src/`, o caminho Airflow depende do repo estar sincronizado no Workspace. |
| 8 | `scripts/eventhub_producer.py:23-24` | `EVENT_HUB_CONNECTION_STR = "YOUR_EVENT_HUB_CONNECTION_STRING"` e `EVENT_HUB_NAME = "transacoes-financeiras"` (**sem sufixo de ambiente**) | `src/config/environment.py:81` e `terraform/modules/event_hub/main.tf:14` definem `transacoes-financeiras-<env>`. O producer básico aponta para um Event Hub que não existe. O `_advanced` (`:42`) tem o mesmo default sem sufixo, mas aceita override por env var. |
| 9 | `src/config/tables.py:4-9` (docstring) | "219 referências hardcoded a `case_santander.bronze/silver/gold` espalhadas por 24 arquivos" | Hoje restam **11 ocorrências** de `case_santander.` literal, todas em comentário, docstring ou notebook (§7.1). A refatoração descrita está concluída — a docstring narra o passado. |
| 10 | `src/config/environment.py:32` | Workspace path fixo `/Workspace/Users/diego.silva0001@gmail.com/…` | E-mail pessoal hardcoded como fallback de path em produção. Só é usado quando `REPO_PATH` não está setado e `DATABRICKS_RUNTIME_VERSION` está. |

### 9.4 Riscos estruturais de orquestração

| # | Risco | Evidência |
|---|---|---|
| 1 | **`silver.streaming` nunca é alimentada em `hk`** | O único produtor é `streaming_continuous` (`databricks.yml:221`), que **não tem schedule** (`:249`) e é um serviço 24/7 que precisa ser iniciado à mão. Em `hk`, `enable_streaming: false` (`:793`) — mas essa variável **não é referenciada por nenhum `condition_task` nem `if`** no bundle: é declarativa e inerte. Consequência: as 4 gold de streaming (`fraude_streaming`, `anomalias_intraday`, `volume_intraday`, `ranking_acoes_realtime`) dependem de uma tabela que ninguém preenche automaticamente. |
| 2 | **Dependência cross-job não modelada** | `streaming_to_gold_continuous` (a cada 5 min) lê `gold.performance_acoes` (`src/gold/streaming_gold.py:41-45,127-131,200-204,271-275`), produzida por `t3_performance` **dentro de `pipeline_completo`** (06:00 diário). Nenhuma dependência declarada entre os dois jobs. Entre a meia-noite e as 06:00, ou na primeira execução, `performance_acoes` pode não existir → os 4 joins de `streaming_gold.py` retornam tudo `NULL` no lado direito, e `alerta_desvio_historico` / `zscore_intraday` / `pct_volume_diario` viram `NULL` silenciosamente (não há gate de qualidade nessas gold). |
| 3 | **Registro de tabela ocorre depois do consumo** | `t8b_uc_registro` (`job_unity_catalog`, posição 21 de 22) aplica Liquid Clustering em 7 tabelas (`:122-130`) e CDF em 3 (`:144-148`) **depois** que todas as gold já foram gravadas. Na primeira execução, `ALTER TABLE … CLUSTER BY` e `SET TBLPROPERTIES` rodam sobre tabelas recém-criadas — o CDF só passa a valer da versão seguinte. Cada `except` (`:137,:159`) apenas loga. |
| 4 | **Marca d'água de CDC pode estar defasada** | `jobs/job_streaming_to_gold_continuous.py:56-60` lê `MAX(versao_cdf)` de `gold.observabilidade`, escrita por `t4_observabilidade` — a **última** task do pipeline diário. O job de 5 em 5 minutos reprocessa a partir de uma versão de até 24 h atrás. `src/observability/monitoring.py:40-48` documenta que essa coluna nem existia antes. |
| 5 | **`t8_lakehouse_monitoring` bloqueia o registro** | `t8b_uc_registro` depende de `t8_lakehouse_monitoring` (`databricks.yml:737`), que chama a API `lakehouse_monitors.create` em 11 tabelas. Se a API falhar de forma não-"already exists", o `except` (`:51-56`) só loga — a task passa. Mas se o cluster falhar, o registro no Unity Catalog e a observabilidade não rodam. |
| 6 | **Sem `max_concurrent_runs` nem lock** | Nada impede duas execuções simultâneas de `pipeline_completo` (`databricks.yml`, ausência de `max_concurrent_runs`). Como quase todas as gold usam `mode("overwrite")`, execuções concorrentes se sobrescrevem. |
| 7 | **`hk` e `prod` compartilham o mesmo catálogo** | `catalog: "case_santander"` nos dois (`src/config/environment.py:87,102`); o isolamento é só por prefixo de schema. Um `ENVIRONMENT` não setado cai em `hk` por default (`:54`) — um job de produção sem a env var escreveria em `hk_*` sem erro. |
| 8 | **`validate_environment` nunca é chamado** | `src/config/environment.py:159-177` exige `CONFIRM_PRODUCTION=true` para prod. `grep -rn 'validate_environment'` → só a definição. A trava não é acionada por nenhum job. |
| 9 | **Gates de qualidade só existem em 3 das 30 tabelas** | `DataQualityValidator.run_all_validations` é chamado em `silver_acoes.py:78`, `silver_bcb.py:48`, `silver_world_bank.py:45`. `silver.clientes`, `silver.ordens`, `silver.streaming` e **todas as 17 gold** não têm gate — dado ruim propaga até o fim. |
| 10 | **Ordem de leitura Delta e reprodutibilidade** | `src/ingestion/ordens_simuladas.py:40-50` documenta que o `orderBy("hash_cliente")` é obrigatório antes do `sample`. Se alguém remover, a simulação deixa de ser determinística e os `id_ordem` (chave de MERGE, `:69`) mudam a cada execução, duplicando `bronze.ordens`. |

---

*Fim do inventário. Repositório `case-santander-data-master`,
branch `release/segunda-chance-dm`, commit `f7265c7`.*
