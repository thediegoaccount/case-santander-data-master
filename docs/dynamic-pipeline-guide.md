# DynamicPipeline - Guia Completo

## Visão Geral

DynamicPipeline é **100% automático** quando usado com auto-discovery e databricks.yml.

---

## O que é Automático ✅

### 1. Auto-Discovery de Jobs
**Descobre jobs automaticamente no diretório `jobs/`**

```python
from src.pipeline.dynamic_pipeline import DynamicPipeline

# Auto-discover jobs
pipeline = DynamicPipeline("pipeline_name", auto_discover=True)

# ❌ MANUAL: Registrar cada job
# ✅ AUTOMÁTICO: Jobs descobertos automaticamente
```

**O que faz:**
- Procura todos os arquivos `job_*.py` em `jobs/`
- Registra cada job automaticamente
- Não precisa registrar manualmente

---

### 2. Load from databricks.yml
**Carrega jobs e dependências do Asset Bundle**

```python
from src.pipeline.dynamic_pipeline import DynamicPipeline

pipeline = DynamicPipeline("pipeline_name", auto_discover=False)
pipeline.load_from_databricks_yml("databricks.yml")

# ❌ MANUAL: Definir dependências
# ✅ AUTOMÁTICO: Dependências carregadas do databricks.yml
```

**O que faz:**
- Lê `databricks.yml`
- Extrai jobs
- Extrai dependências (via `depends_on`)
- Registra tudo automaticamente

---

### 3. Geração Automática de DAG
**Gera código Airflow automaticamente**

```python
from src.pipeline.dynamic_pipeline import auto_generate_dag

# Gera DAG automaticamente
auto_generate_dag(
    databricks_yml_path="databricks.yml",
    output_path="dags/dag_pipeline_santander.py"
)

# ❌ MANUAL: Escrever DAG manualmente
# ✅ AUTOMÁTICO: DAG gerado automaticamente
```

**O que faz:**
- Lê `databricks.yml`
- Gera código Airflow
- Configura dependências
- Salva em arquivo

---

### 4. Ordem de Execução
**Calcula ordem automaticamente via topological sort**

```python
execution_order = pipeline.get_execution_order()

# ❌ MANUAL: Definir ordem manualmente
# ✅ AUTOMÁTICO: Ordem calculada automaticamente
```

**O que faz:**
- Analisa dependências
- Resolve ordem de execução
- Garante dependências são executadas primeiro

---

### 5. Validação Automática
**Valida pipeline automaticamente**

```python
pipeline.validate_pipeline()

# ❌ MANUAL: Validar manualmente
# ✅ AUTOMÁTICO: Validação automática
```

**O que faz:**
- Verifica dependências circular
- Verifica dependências inválidas
- Retorna True se válido

---

## Modos de Uso

### Modo 1: 100% Automático (Recomendado)

**Usar `auto_generate_dag`**

```python
from src.pipeline.dynamic_pipeline import auto_generate_dag

# Gera DAG automaticamente do databricks.yml
auto_generate_dag(
    databricks_yml_path="databricks.yml",
    output_path="dags/dag_pipeline_santander.py"
)
```

**Ou via script:**
```bash
python scripts/auto_generate_dag.py
```

**Benefícios:**
- ✅ 100% automático
- ✅ Baseado em databricks.yml (source of truth)
- ✅ Atualizado automaticamente ao modificar databricks.yml

---

### Modo 2: Auto-Discovery (Semi-Automático)

**Auto-descobre jobs, mas não dependências**

```python
from src.pipeline.dynamic_pipeline import DynamicPipeline

# Auto-discover jobs
pipeline = DynamicPipeline("pipeline_name", auto_discover=True)

# Executar pipeline
pipeline.execute_pipeline()
```

**Benefícios:**
- ✅ Jobs descobertos automaticamente
- ⚠️ Dependências precisam ser configuradas manualmente

---

### Modo 3: Manual (Controle Granular)

**Registro manual de jobs**

```python
from src.pipeline.dynamic_pipeline import DynamicPipeline

pipeline = DynamicPipeline("pipeline_name", auto_discover=False)

# Registrar jobs manualmente
pipeline.register_job("job_a", job_a_function)
pipeline.register_job("job_b", job_b_function, dependencies=["job_a"])

# Executar pipeline
pipeline.execute_pipeline()
```

**Benefícios:**
- ✅ Controle total
- ❌ Requer registro manual

---

## Fluxo Completo Automático

### 1. Definir Jobs no databricks.yml

```yaml
resources:
  jobs:
    job_clientes_ordens:
      name: job_clientes_ordens
      tasks:
        - task_key: t0
          depends_on:
            - task_key: t1  # Dependência
```

### 2. Gerar DAG Automaticamente

```bash
python scripts/auto_generate_dag.py
```

### 3. DAG Gerado Automaticamente

```python
# dags/dag_pipeline_santander_auto.py (gerado automaticamente)
from airflow import DAG
from airflow.operators.python import PythonOperator

# Jobs e dependências gerados automaticamente
```

### 4. Airflow Executa Automaticamente

- Airflow carrega DAG
- Executa jobs na ordem correta
- Monitora dependências

---

## Resumo

### 100% Automático ✅

**Quando usar:**
- Recomendado para produção
- Baseado em databricks.yml
- Fonte única de verdade

**O que é automático:**
- ✅ Descoberta de jobs
- ✅ Carregamento de dependências
- ✅ Geração de DAG
- ✅ Ordem de execução
- ✅ Validação

**O que você faz:**
- ❌ Nada (apenas editar databricks.yml)

---

### Semi-Automático ⚠️

**Quando usar:**
- Jobs não estão no databricks.yml
- Precisa de controle parcial

**O que é automático:**
- ✅ Descoberta de jobs
- ✅ Ordem de execução
- ✅ Validação

**O que você faz:**
- ⚠️ Configurar dependências manualmente

---

### Manual ❌

**Quando usar:**
- Controle total necessário
- Testes específicos

**O que é automático:**
- ✅ Ordem de execução
- ✅ Validação

**O que você faz:**
- ❌ Registrar jobs manualmente
- ❌ Configurar dependências manualmente

---

## Conclusão

**DynamicPipeline é 100% automático** quando usado com:
- `auto_generate_dag()` + `databricks.yml`

**Modo recomendado:**
```bash
python scripts/auto_generate_dag.py
```

**Resultado:**
- DAG gerado automaticamente
- Jobs e dependências carregados do databricks.yml
- Ordem de execução calculada automaticamente
- Zero trabalho manual
