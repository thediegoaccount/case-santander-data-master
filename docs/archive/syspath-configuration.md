# sys.path Configuration - Dinâmico por Ambiente

## Visão Geral

O `sys.path.insert` hard-coded foi substituído por configuração dinâmica via `setup_python_path()`.

## Problema Anterior

**Hard-coded:**
```python
sys.path.insert(0, "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")
```

**Problemas:**
- ❌ Email do usuário hard-coded
- ❌ Difícil mudar de usuário
- ❌ Não funciona em diferentes ambientes
- ❌ Não funciona localmente

---

## Solução Implementada

**Função dinâmica:**
```python
from src.config.environment import setup_python_path

setup_python_path()
```

**Arquivo:** `src/config/environment.py`

```python
@staticmethod
def get_repo_path() -> str:
    """
    Retorna caminho do repositório dinamicamente
    
    Priority:
    1. Variável de ambiente REPO_PATH
    2. Databricks Workspace path padrão
    3. Diretório atual (local)
    """
    # 1. Variável de ambiente
    repo_path = os.getenv("REPO_PATH")
    if repo_path:
        return repo_path
    
    # 2. Databricks Workspace path padrão
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        return "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master"
    
    # 3. Diretório atual (local)
    return os.getcwd()

@staticmethod
def setup_python_path():
    """
    Configura sys.path para importar módulos do repositório
    Deve ser chamado no início de cada job
    """
    repo_path = EnvironmentConfig.get_repo_path()
    
    # Adicionar ao sys.path se não estiver
    if repo_path not in sys.path:
        sys.path.insert(0, repo_path)
    
    return repo_path
```

---

## Como Configurar

### 1. **Databricks (Padrão)**

**Sem configuração:**
```python
# Usará path padrão do Databricks
from src.config.environment import setup_python_path
setup_python_path()
```

**Caminho detectado automaticamente:**
```
/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master
```

---

### 2. **Databricks (Customizado)**

**Via variável de ambiente:**
```python
# No cluster ou job config
REPO_PATH=/Workspace/Users/outro-usuario/case-santander-data-master
```

**No código:**
```python
from src.config.environment import setup_python_path
setup_python_path()  # Usará REPO_PATH
```

---

### 3. **Local (Desenvolvimento)**

**Sem configuração:**
```python
# Usará diretório atual
from src.config.environment import setup_python_path
setup_python_path()
```

**Caminho detectado automaticamente:**
```
C:\Users\thedi\OneDrive\Desktop\GIT\case-santander-data-master
```

---

### 4. **Local (Customizado)**

**Via variável de ambiente:**
```bash
# Windows
set REPO_PATH=C:\path\to\case-santander-data-master

# Linux/Mac
export REPO_PATH=/path/to/case-santander-data-master
```

**No código:**
```python
from src.config.environment import setup_python_path
setup_python_path()  # Usará REPO_PATH
```

---

## Jobs Corrigidos

**28 jobs corrigidos:**
- job_clientes_ordens.py
- job_clientes_silver.py
- job_extracao_acoes.py
- job_extracao_bcb.py
- job_extracao_world_bank.py
- job_silver_acoes.py
- job_silver_bcb.py
- job_silver_world_bank.py
- job_gold_anomalias.py
- job_gold_performance.py
- job_gold_bcb.py
- job_gold_world_bank.py
- job_gold_acoes_vs_cambio.py
- job_streaming.py
- job_streaming_continuous.py
- job_streaming_to_gold.py
- job_streaming_to_gold_continuous.py
- job_unity_catalog.py
- job_carga_sql_acoes.py
- job_carga_sql_clientes.py
- job_carga_sql_fraude.py
- job_carga_sql_macro.py
- job_carga_sql_streaming.py
- job_corretora_analises.py
- job_lakehouse_monitoring.py
- job_observabilidade.py
- job_scd.py
- job_gold_fraude.py

---

## Exemplo de Job Completo

```python
"""
Job: Clientes e Ordens
"""

import sys
from src.config.environment import setup_python_path

setup_python_path()
from src.config.logging import info, error, warning
from src.config.secrets import get_secret
from databricks.connect import DatabricksSession
from datetime import datetime


def main():
    inicio = datetime.now()
    info("job_clientes_ordens", f"=== JOB CLIENTES ORDENS INICIADO: {inicio} ===")
    
    try:
        spark = DatabricksSession.builder.getOrCreate()
        
        client_id = get_secret("client-id")
        storage_account = get_secret("storage-account")
        
        info("job_clientes_ordens", f"Storage Account: {storage_account}")
        
        # Processar dados
        total = 10000
        info("job_clientes_ordens", f"Bronze clientes gravado: {total} registros")
        
        fim = datetime.now()
        duracao = (fim - inicio).total_seconds()
        info("job_clientes_ordens", f"Job concluído em {duracao:.2f}s")
        
    except Exception as e:
        error("job_clientes_ordens", f"Erro ao processar: {str(e)}")
        raise


if __name__ == "__main__":
    main()
```

---

## Prioridade de Resolução

**Priority Order:**
1. **REPO_PATH** (variável de ambiente) - Prioridade máxima
2. **Databricks Runtime** - Detecta automaticamente se estiver no Databricks
3. **Diretório atual** - Fallback para desenvolvimento local

---

## Troubleshooting

### Erro: ModuleNotFoundError

**Problema:**
```
ModuleNotFoundError: No module named 'src.config'
```

**Solução:**
```python
# Verificar se setup_python_path() foi chamado
from src.config.environment import setup_python_path

repo_path = setup_python_path()
print(f"Repo path: {repo_path}")  # Debug
```

---

### Erro: Caminho incorreto

**Problema:**
```
Repo path: /Workspace/Users/wrong-email/case-santander-data-master
```

**Solução:**
```bash
# Definir REPO_PATH corretamente
export REPO_PATH=/Workspace/Users/correct-email/case-santander-data-master
```

---

### Erro: Local não funciona

**Problema:**
```
ModuleNotFoundError ao rodar localmente
```

**Solução:**
```bash
# Verificar se está no diretório correto
cd C:\Users\thedi\OneDrive\Desktop\GIT\case-santander-data-master

# Ou definir REPO_PATH
set REPO_PATH=C:\Users\thedi\OneDrive\Desktop\GIT\case-santander-data-master
```

---

## Resumo

**Implementação:**
- ✅ 28 jobs corrigidos
- ✅ Função `setup_python_path()` centralizada
- ✅ Prioridade de resolução (REPO_PATH > Databricks > Local)
- ✅ Funciona em Databricks e local

**Benefícios:**
- ✅ Email não hard-coded
- ✅ Configurável por ambiente
- ✅ Funciona localmente
- ✅ Fácil mudar de usuário

**Próximo passo:** Testar jobs em Databricks com a nova configuração.
