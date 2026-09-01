# Isolamento de Ambiente - Boas Práticas

## Visão Geral

Este projeto implementa isolamento entre ambientes (HK - homologação reduzido e PROD - produção completa) para garantir que dados de homologação nunca afetem produção, com foco em redução de custos.

## Arquitetura de Ambientes

### 1. Configuração por Ambiente

O arquivo `src/config/environment.py` gerencia configurações isoladas:

```python
from src.config.environment import get_config, get_env, is_production

# Detecta ambiente automaticamente via variável ENVIRONMENT
env = get_env()  # "hk" ou "prod"
config = get_config()
```

### 2. Recursos Isolados por Ambiente

| Recurso | HK (Homologação Reduzido) | PROD (Produção Completo) |
|---------|---------------------------|--------------------------|
| **Storage Account** | `stcasesantander-hk` | `stcasesantander` |
| **Key Vault** | `kv-case-santander-hk` | `kv-case-santander` |
| **Unity Catalog** | `case_santander` (mesmo) | `case_santander` (mesmo) |
| **Schemas** | `hk_bronze`, `hk_silver`, `hk_gold` | `prod_bronze`, `prod_silver`, `prod_gold` |
| **Event Hub** | `evhcasesantander-hk` | `evhcasesantander` |
| **SQL Database** | `sqldb-case-santander-hk` | `sqldb-case-santander` |

**Nota:** Unity Catalog é compartilhado entre ambientes, mas com schemas separados (`hk_*` vs `prod_*`) para isolamento lógico sem custo adicional.

### 3. Rate Limiting por Ambiente

HK tem rate limits reduzidos para economia de custo:

| API | HK (Reduzido) | PROD (Completo) |
|-----|--------------|-----------------|
| Yahoo Finance | 15 req/min | 30 req/min |
| BCB API | 45 req/min | 120 req/min |
| World Bank | 90 req/min | 300 req/min |
| Kaggle | 5 req/min | 20 req/min |

### 4. Retenção de Dados

HK tem retenção reduzida para economia de custo:

| Ambiente | Retenção | Streaming |
|----------|----------|-----------|
| HK | 30 dias | **Desabilitado** (redução de custo) |
| PROD | 90 dias | Habilitado (completo) |

## Implementação

### Configuração de Variáveis de Ambiente

```bash
# Para homologação (padrão)
export ENVIRONMENT=hk

# Para produção (requer confirmação)
export ENVIRONMENT=prod
export CONFIRM_PRODUCTION=true
```

### Arquivo .env

Copie `.env.example` para `.env` e configure:

```bash
cp .env.example .env
# Edite .env com suas credenciais
```

### Uso nos Jobs

Os jobs de extração agora detectam automaticamente o ambiente:

```python
from src.ingestion.yahoo_finance import extrair_acoes

# Não precisa passar storage_account - usa config do ambiente
extrair_acoes(spark)  # Usa storage do ambiente atual
```

### Proteção de Produção

O ambiente de produção tem proteções adicionais:

```python
from src.config.environment import validate_environment

# Retorna False se não houver confirmação explícita
if not validate_environment():
    raise ValueError("Produção requer CONFIRM_PRODUCTION=true")
```

## Fluxo de CI/CD

O pipeline do GitHub Actions respeita ambientes:

- **Branch `develop`** → Deploy automático para **hk** (homologação)
- **Branch `main`** → Deploy automático para **hk** (homologação)
- **Branch `main`** + aprovação manual → Deploy para **prod** (produção)

## Boas Práticas

### 1. Nunca Hardcode Credenciais

```python
#  ERRADO
storage_account = "stcasesantander"

#  CORRETO
from src.config.environment import get_config
storage_account = get_config()["storage_account"]
```

### 2. Sempre Logar o Ambiente

```python
from src.config.environment import get_env
env = get_env()
print(f"[{env.upper()}] Iniciando processamento...")
```

### 3. Validar Ambiente Antes de Operações Críticas

```python
from src.config.environment import is_production

if is_production():
    # Validações adicionais
    print(" PRODUÇÃO - Operação crítica")
```

### 4. Usar Rate Limiting

```python
from src.ingestion.api_wrapper import rate_limiter

# Respeita limites do ambiente automaticamente
rate_limiter.wait_if_needed("yahoo_finance")
```

## Estrutura de Diretórios Isolados

### ADLS (Storage Accounts Separados)

```
stcasesantander-hk/
 bronze/
    acoes/
    bcb/
    world_bank/
 silver/
 gold/

stcasesantander/
 bronze/
 silver/
 gold/
```

### Unity Catalog (Mesmo Catalog, Schemas Separados)

```
case_santander (único catalog)
 hk_bronze/        # Homologação
    acoes
    bcb
    world_bank
 hk_silver/        # Homologação
    acoes
    bcb
    world_bank
 hk_gold/          # Homologação
    anomalias
    performance
 prod_bronze/      # Produção
    acoes
    bcb
    world_bank
 prod_silver/      # Produção
    acoes
    bcb
    world_bank
 prod_gold/        # Produção
     anomalias
     performance
```

## Tags de Rastreabilidade

Todos os dados são marcados com o ambiente de origem:

```python
df["ambiente"] = env  # "dev", "hk", ou "prod"
df["data_extracao"] = datetime.now()
```

Isso permite:
- Auditoria de dados
- Debugging de problemas
- Comprovação de isolamento

## Monitoramento

### Logs por Ambiente

```python
[DEV] Extraindo 9 acoes B3...
[DEV] Storage Account: stcasesantander-dev
[DEV] Catalog: case_santander_dev

[PROD] *** PRODUÇÃO *** - Dados reais serão gravados
[PROD] Extraindo 9 acoes B3...
[PROD] Storage Account: stcasesantander
[PROD] Catalog: case_santander
```

### Métricas de Rate Limiting

```python
[DEV] API yahoo_finance: 5/10 requests/min
[HK] API yahoo_finance: 15/20 requests/min
[PROD] API yahoo_finance: 25/30 requests/min
```

## Validação

### Teste de Isolamento

```python
from src.config.environment import get_config, get_paths

config = get_config()
paths = get_paths()

print(f"Ambiente: {get_env()}")
print(f"Storage: {config['storage_account']}")
print(f"Catalog: {config['catalog']}")
print(f"Schema Prefix: {config['schema_prefix']}")
print(f"Bronze Path: {paths['bronze_acoes']}")
print(f"Table Bronze: {paths['table_bronze_clientes']}")
```

### Verificação de Produção

```python
from src.config.environment import is_production

if is_production():
    print(" Você está em PRODUÇÃO")
    print(f"Storage: {get_config()['storage_account']}")
    print(f"Schema Prefix: {get_config()['schema_prefix']}")
else:
    print("ℹ Você está em HOMOLOGAÇÃO")
    print(f"Streaming: {get_config()['enable_streaming']}")
```

## Troubleshooting

### Erro: "Ambiente inválido"

```bash
# Verifique se ENVIRONMENT está definido corretamente
export ENVIRONMENT=hk  # ou prod
```

### Erro: "Produção requer confirmação"

```bash
# Para executar em produção
export ENVIRONMENT=prod
export CONFIRM_PRODUCTION=true
```

### Rate Limit Atingido

```python
# O rate limiter aguarda automaticamente
# Em HK, os limites são reduzidos por design
# Em PROD, os limites são maiores
```

## Benefícios

 **Isolamento Lógico**: Dados de HK nunca misturam com PROD (schemas separados)  
 **Rate Limiting Diferenciado**: HK com limites reduzidos para economia  
 **Streaming Desabilitado em HK**: Redução significativa de custo (Event Hub)  
 **Auditoria**: Rastreabilidade completa com tags de ambiente  
 **Segurança**: Proteções adicionais em produção  
 **Custo Controlado**: Retenção 30 dias (HK) vs 90 dias (PROD)  
 **CI/CD Integrado**: Deploy automático respeita ambientes  
 **Zero Custo de Catalog**: Mesmo Unity Catalog, schemas separados  

## Economia de Custo

### HK (Homologação Reduzido)
- **Event Hub**: Desabilitado (economia de ~R$ 500-1000/mês)
- **Rate Limits**: 50% dos limites de produção (reduz risco de bloqueio)
- **Retenção**: 30 dias vs 90 dias (economia de armazenamento)
- **Economia Estimada**: ~60-70% vs produção completa

### PROD (Produção Completo)
- **Event Hub**: Habilitado (streaming completo)
- **Rate Limits**: Limites completos (máxima disponibilidade)
- **Retenção**: 90 dias (compliance completo)
- **Custo Total**: Configurado para workload de produção

## Próximos Passos

1. Criar recursos Azure para HK e PROD
2. Configurar schemas separados no Unity Catalog
3. Configurar variáveis de ambiente no CI/CD
4. Testar isolamento entre HK e PROD
5. Implementar políticas de retenção no ADLS
6. Configurar monitoramento por ambiente
7. Revisar documentação de API Security & Governance
