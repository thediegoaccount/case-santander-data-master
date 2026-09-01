# Logging Implementation - Substituição de print()

## Visão Geral

Todos os jobs principais foram migrados de `print()` para `logging` estruturado.

## Benefícios

### Antes (print())
```python
print("=== JOB INICIADO ===")
print("Erro ao processar dados")
print(f"Total: {total} registros")
```

**Problemas:**
- ❌ Sem estrutura de níveis (INFO, WARNING, ERROR)
- ❌ Sem timestamp automático
- ❌ Difícil filtrar logs por severidade
- ❌ Não integrado com sistemas de monitoramento
- ❌ Não roteável para diferentes destinos

### Depois (logging)
```python
from src.config.logging import info, error, warning

info("job_name", "=== JOB INICIADO ===")
error("job_name", "Erro ao processar dados")
info("job_name", f"Total: {total} registros")
```

**Benefícios:**
- ✅ Níveis de log (INFO, WARNING, ERROR, DEBUG, CRITICAL)
- ✅ Timestamp automático
- ✅ Nome do job em cada log
- ✅ Integrado com sistemas de monitoramento
- ✅ Roteável para diferentes destinos (Arquivo, Sistema, etc.)

---

## Módulo de Logging

**Arquivo:** `src/config/logging.py`

**Funções disponíveis:**
- `setup_logging(job_name)` - Configura logger para o job
- `get_logger(job_name)` - Retorna logger configurado
- `info(job_name, message)` - Log em nível INFO
- `warning(job_name, message)` - Log em nível WARNING
- `error(job_name, message)` - Log em nível ERROR
- `debug(job_name, message)` - Log em nível DEBUG
- `critical(job_name, message)` - Log em nível CRITICAL

---

## Formato de Log

**Formato padrão:**
```
[JOB_NAME] LEVEL [TIMESTAMP] Mensagem
```

**Exemplo:**
```
[job_clientes_ordens] INFO [2026-01-09 18:50:00] === JOB CLIENTES ORDENS INICIADO: 2026-01-09 18:50:00 ===
[job_clientes_ordens] INFO [2026-01-09 18:50:05] Bronze clientes gravado: 10000 registros
[job_clientes_ordens] WARNING [2026-01-09 18:50:10] Dados faltando em clientes
[job_clientes_ordens] ERROR [2026-01-09 18:50:15] Erro ao processar dados
```

---

## Jobs Migrados

**28 jobs migrados para logging:**
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

## Exemplo de Uso

### Job Completo com Logging

```python
"""
Job: Clientes e Ordens
"""

import sys
sys.path.insert(0, "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")

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
        
        if total == 0:
            warning("job_clientes_ordens", "Nenhum registro encontrado")
        
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

## Níveis de Log

### INFO
**Uso:** Informações gerais de execução
```python
info("job_name", "Job iniciado")
info("job_name", "Dados processados com sucesso")
```

### WARNING
**Uso:** Situações não críticas que merecem atenção
```python
warning("job_name", "Dados faltando")
warning("job_name", "Performance abaixo do esperado")
```

### ERROR
**Uso:** Erros que impedem execução normal
```python
error("job_name", "Erro ao conectar ao banco")
error("job_name", "Arquivo não encontrado")
```

### DEBUG
**Uso:** Informações detalhadas para debugging
```python
debug("job_name", "Processando registro 1000")
debug("job_name", "SQL executado: SELECT ...")
```

### CRITICAL
**Uso:** Erros críticos que requerem intervenção imediata
```python
critical("job_name", "Sistema indisponível")
critical("job_name", "Perda de dados detectada")
```

---

## Integração com Databricks

### Logs no Databricks UI

**Logs são visíveis em:**
- Cluster logs
- Job run logs
- Notebook execution logs

**Filtragem:**
```bash
# Filtrar por job
grep "job_clientes_ordens" logs/

# Filtrar por nível
grep "ERROR" logs/
grep "WARNING" logs/
```

---

## Scripts Auxiliares

### Scripts (mantidos com print)

**Razão:** Scripts de setup/geração são executados localmente pelo usuário, não em produção.

**Arquivos mantidos com print:**
- `scripts/generate_salt.py` - Geração de salt
- `scripts/fix_keyvault_hardcoding.py` - Correção de hardcoding
- `scripts/replace_print_with_logging.py` - Substituição de print
- `scripts/sync_airflow_from_databricks.py` - Sincronização Airflow
- `scripts/setup_airflow_env.py` - Setup Airflow
- `scripts/eventhub_producer_advanced.py` - Producer Event Hub

**Módulos auxiliares (mantidos com print):**
- `src/ingestion/` - Módulos de ingestão
- `src/transformation/` - Módulos de transformação
- `src/gold/` - Módulos de análise Gold
- `src/clients/` - Módulos de clientes
- `src/observability/` - Módulos de observabilidade

**Razão:** São funções auxiliares chamadas pelos jobs, não executados diretamente.

---

## Melhorias Futuras

### 1. Log Structured (JSON)
```python
# Formato JSON para integração com ELK, Splunk, etc.
{
  "job_name": "job_clientes_ordens",
  "level": "INFO",
  "timestamp": "2026-01-09T18:50:00",
  "message": "Job iniciado",
  "metadata": {
    "environment": "hk",
    "user": "diego.silva0001@gmail.com"
  }
}
```

### 2. Log Aggregation
- Centralizar logs em um sistema (ELK, Splunk, Datadog)
- Dashboard de logs em tempo real
- Alertas baseados em logs

### 3. Log Retention
- Configurar retenção de logs (30 dias, 90 dias)
- Archival em armazenamento de baixo custo
- Compliance com auditoria

---

## Resumo

**Implementação:**
- ✅ 28 jobs migrados para logging
- ✅ Módulo centralizado em `src/config/logging.py`
- ✅ Formato estruturado com nome do job e timestamp
- ✅ Níveis de log (INFO, WARNING, ERROR, DEBUG, CRITICAL)

**Benefícios:**
- ✅ Melhor rastreabilidade
- ✅ Filtragem por severidade
- ✅ Integração com sistemas de monitoramento
- ✅ Compliance com padrões enterprise

**Próximo passo:** Integrar com sistema de log aggregation (ELK, Splunk) para dashboard de logs em tempo real.
