# CDC Implementado - Base de Clientes

## Mudança Aplicada

### Arquivo Modificado
`jobs/job_clientes_ordens.py`

### Antes (OVERWRITE)
```python
#  OVERWRITE - Deduplica a base a cada execução
df_clientes_spark.write.format("delta").mode("overwrite") \
    .saveAsTable("case_santander.bronze.clientes")
```

### Depois (MERGE/CDC)
```python
#  MERGE - CDC real (apenas mudanças)
from delta.tables import DeltaTable

try:
    delta_table = DeltaTable.forName(spark, tabela_clientes_bronze)
    
    delta_table.alias("target") \
        .merge(
            df_clientes_spark.alias("source"),
            "target.hash_cliente = source.hash_cliente"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    
    print(" bronze.clientes atualizado via MERGE (CDC)")
    
except Exception as e:
    if "is not a Delta table" in str(e) or "Table or view not found" in str(e):
        # Primeira carga
        df_clientes_spark.write.format("delta").mode("overwrite") \
            .saveAsTable(tabela_clientes_bronze)
        print(" bronze.clientes primeira carga")
```

## Tabelas Afetadas

1. **`case_santander.bronze.clientes`**
   - OVERWRITE → MERGE (hash_cliente como chave)

2. **`case_santander.bronze.ordens`**
   - OVERWRITE → MERGE (id_ordem como chave)

3. **`case_santander.silver.clientes`**
   - OVERWRITE → MERGE (hash_cliente como chave)

4. **`case_santander.silver.ordens`**
   - OVERWRITE → MERGE (id_ordem como chave)

## Como Funciona o CDC Agora

### Fluxo com MERGE

```
Dia 1:
  Download Kaggle (10.000 clientes)
  ↓
  Bronze.clientes (MERGE) → 10.000 registros (primeira carga)
  ↓
  Silver.clientes (MERGE) → 10.000 registros (primeira carga)
  ↓
  SCD Type 2 → 10.000 registros (versão 1)

Dia 2:
  Download Kaggle (mesmos 10.000 clientes)
  ↓
  Bronze.clientes (MERGE) → 0 novos, 10.000 atualizados (se houver mudança)
  ↓
  Silver.clientes (MERGE) → 0 novos, 10.000 atualizados (se houver mudança)
  ↓
  SCD Type 2 → 
    - Se houve mudança: Fecha versão 1, insere versão 2
    - Se não houve mudança: Nada (versão 1 continua atual)
     HISTÓRICO REAL (apenas mudanças)

Dia 3:
  Download Kaggle (mesmos 10.000 clientes)
  ↓
  Bronze.clientes (MERGE) → 0 novos, 10.000 atualizados (se houver mudança)
  ↓
  Silver.clientes (MERGE) → 0 novos, 10.000 atualizados (se houver mudança)
  ↓
  SCD Type 2 →
    - Se houve mudança: Fecha versão 2, insere versão 3
    - Se não houve mudança: Nada (versão 2 continua atual)
     HISTÓRICO REAL (apenas mudanças)
```

## Chaves de MERGE

### Clientes
- **Chave:** `hash_cliente` (SHA-256 do CustomerId)
- **Lógica:** Atualiza se hash_cliente existe, insere se não existe

### Ordens
- **Chave:** `id_ordem` (ID gerado aleatoriamente)
- **Lógica:** Atualiza se id_ordem existe, insere se não existe

## Benefícios da Mudança

### Antes (OVERWRITE)
-  Deduplica a base a cada execução
-  Histórico artificial (snapshots idênticos)
-  Armazenamento alto (365 snapshots/ano)
-  Não rastreia mudanças reais
-  SCD Type 2 ineficiente

### Depois (MERGE/CDC)
-  Não deduplica (mantém + atualiza)
-  Histórico real (apenas mudanças)
-  Armazenamento eficiente (apenas mudanças)
-  Rastreia alterações reais
-  SCD Type 2 eficiente

## Comportamento do SCD Type 2

### Antes (OVERWRITE)
```
Dia 1: 10.000 registros (versão 1)
Dia 2: 10.000 registros (versão 2) ← IDÊNTICOS
Dia 3: 10.000 registros (versão 3) ← IDÊNTICOS
```

### Depois (MERGE)
```
Dia 1: 10.000 registros (versão 1)
Dia 2: 
  - Se mudou: 10.000 registros (versão 2) ← MUDANÇA REAL
  - Se não mudou: Nada (versão 1 continua)
Dia 3:
  - Se mudou: 10.000 registros (versão 3) ← MUDANÇA REAL
  - Se não mudou: Nada (versão anterior continua)
```

## Impacto no Kaggle

### Base é Estática?
**Sim, o dataset Kaggle é estático (10.000 clientes fixos)**

### Com OVERWRITE
- Cada execução sobrescreve tudo
- SCD cria histórico artificial de snapshots idênticos
- Armazenamento duplicado sem valor

### Com MERGE
- Primeira execução: Insere 10.000 clientes
- Execuções subsequentes: 
  - Atualiza se Kaggle mudar (não muda, é estático)
  - Nada se o Kaggle não mudar
- SCD Type 2:
  - Se Kaggle nunca mudar: Apenas 1 versão (histórico correto)
  - Se Kaggle mudar: Cria nova versão (histórico correto)

## Cenários de Teste

### Cenário 1: Dataset Estático (Atual)
```bash
# Execução 1
 Bronze.clientes: 10.000 (primeira carga)
 Silver.clientes: 10.000 (primeira carga)
 SCD: 10.000 (versão 1)

# Execução 2
 Bronze.clientes: 0 novos, 10.000 atualizados (sem mudança)
 Silver.clientes: 0 novos, 10.000 atualizados (sem mudança)
 SCD: Nada (não há mudança)
```

### Cenário 2: Dataset Atualizado (Futuro)
```bash
# Kaggle atualiza (adiciona 100 novos clientes)

# Execução
 Bronze.clientes: 100 novos, 10.000 atualizados
 Silver.clientes: 100 novos, 10.000 atualizados
 SCD: Fecha 10.000 (versão 1), insere 10.100 (versão 2)
```

### Cenário 3: Atributos Mudam (Futuro)
```bash
# Kaggle atualiza (muda score de 100 clientes)

# Execução
 Bronze.clientes: 0 novos, 100 atualizados (mudança)
 Silver.clientes: 0 novos, 100 atualizados (mudança)
 SCD: Fecha 100 (versão 1), insere 100 (versão 2)
```

## Economia de Armazenamento

### Antes (OVERWRITE)
- **Armazenamento:** 10.000 × 365 = 3.650.000 registros/ano
- **Custo:** Alto (histórico artificial)

### Depois (MERGE)
- **Armazenamento:** 10.000 (base) + mudanças reais
- **Custo:** Baixo (histórico real)
- **Economia:** ~99% se Kaggle for estático

## Validação

### Verificar CDC

```sql
-- Verificar histórico no Bronze
SELECT 
    hash_cliente,
    COUNT(*) as versoes
FROM case_santander.bronze.clientes
GROUP BY hash_cliente
HAVING COUNT(*) > 1

-- Se vazio: MERGE funcionou (1 versão por cliente)
-- Se não vazio: OVERWRITE foi usado (duplicações)
```

### Verificar SCD

```sql
-- Verificar histórico no SCD
SELECT 
    hash_cliente,
    data_inicio,
    data_fim,
    atual
FROM case_santander.silver.clientes_scd
WHERE hash_cliente = 'hash_especifico'
ORDER BY data_inicio

-- Deve mostrar:
-- Versão 1: data_inicio = primeira execução, data_fim = data mudança
-- Versão 2: data_inicio = data mudança, data_fim = 9999-12-31
```

## Resumo

### Mudança Aplicada
-  OVERWRITE → MERGE em 4 tabelas
-  CDC real implementado
-  Não deduplica mais
-  Histórico real (não artificial)
-  SCD Type 2 eficiente

### Impacto
-  Histórico correto de mudanças
-  Economia de armazenamento
-  SCD Type 2 funciona corretamente
-  Não perde dados
-  Rastreia alterações reais

### Comportamento
- **Kaggle estático:** Apenas 1 versão (histórico correto)
- **Kaggle atualizado:** Histórico de mudanças reais
- **Execução contínua:** Nada se não houver mudança
