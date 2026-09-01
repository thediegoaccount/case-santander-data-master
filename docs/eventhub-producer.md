# Producer Event Hub - Transações Financeiras

## Visão Geral

Producer que gera transações financeiras simuladas e envia para Azure Event Hub para testar o pipeline de streaming.

## Scripts Disponíveis

### 1. Producer Simples (`scripts/eventhub_producer.py)

**Uso:**
```bash
python scripts/eventhub_producer.py
```

**Características:**
-  Gera transações aleatórias
-  Envia 1 transação por segundo
-  Tickers realistas (PETR4, VALE3, etc.)
-  Precisa editar connection string no código

**Schema da Transação:**
```json
{
  "timestamp": "2024-01-15T10:30:00",
  "ticker": "PETR4.SA",
  "preco": 35.50,
  "quantidade": 1000,
  "tipo": "compra",
  "corretora": "Santander",
  "id_transacao": "PETR4-1705326600000-1234"
}
```

### 2. Producer Avançado (`scripts/eventhub_producer_advanced.py`)

**Uso:**
```bash
# Configurar variáveis de ambiente
export EVENTHUB_CONNECTION_STRING="Endpoint=sb://..."
export EVENTHUB_NAME="transacoes-financeiras"

# Modo contínuo (1 transação por segundo)
python scripts/eventhub_producer_advanced.py

# Modo volume (100 transações rapidamente)
python scripts/eventhub_producer_advanced.py --volume 100

# Modo burst (50 transações e para)
python scripts/eventhub_producer_advanced.py --burst 50

# Intervalo customizado (0.5 segundos)
python scripts/eventhub_producer_advanced.py --interval 0.5
```

**Características:**
-  Validação de schema
-  Variáveis de ambiente
-  Múltiplos modos (contínuo, volume, burst)
-  Intervalo configurável
-  Estatísticas de envio

## Configuração

### Variáveis de Ambiente

```bash
# Connection String do Event Hub
export EVENTHUB_CONNECTION_STRING="Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-key;EntityPath=your-event-hub"

# Nome do Event Hub
export EVENTHUB_NAME="transacoes-financeiras"
```

### Obter Connection String

1. Acesse Azure Portal
2. Navegue para Event Hub Namespace
3. Settings → Shared access policies
4. Crie ou use RootManageSharedAccessKey
5. Copie Connection String primária

## Exemplo de Uso

### Teste Rápido (Burst)

```bash
# Enviar 10 transações para testar
python scripts/eventhub_producer_advanced.py --burst 10
```

### Carga de Teste (Volume)

```bash
# Enviar 1000 transações para testar performance
python scripts/eventhub_producer_advanced.py --volume 1000
```

### Simulação Contínua

```bash
# Simular ambiente de produção (1 transação/seg)
python scripts/eventhub_producer_advanced.py --interval 1
```

### Alta Frequência

```bash
# Simular pico de transações (0.1 segundos entre transações)
python scripts/eventhub_producer_advanced.py --interval 0.1
```

## Dados Gerados

### Tickers Disponíveis

- PETR4.SA (Petrobras)
- VALE3.SA (Vale)
- ITUB4.SA (Itaú Unibanco)
- BBDC4.SA (Bradesco)
- BBAS3.SA (Banco do Brasil)
- WEGE3.SA (WEG)
- RENT3.SA (Localiza)
- MGLU3.SA (Magalu)
- B3SA3.SA (B3)
- HAPV3.SA (Hapvida)

### Tipos de Transação

- `compra`
- `venda`

### Corretoras

- Santander
- Itaú
- BTG
- XP
- Rico
- NuInvest

### Variação de Preço

- Preço base por ticker
- Variação de +/- 5% (simulado)
- Preço: R$ 2.50 - R$ 180.00

### Quantidade

- Mínimo: 100 ações
- Máximo: 10.000 ações
- Aleatório

## Validação

O producer avançado valida:

1. **Camadas obrigatórios:**
   - timestamp
   - ticker
   - preco
   - quantidade
   - tipo
   - corretora
   - id_transacao

2. **Tipos de dados:**
   - preco: numérico
   - quantidade: inteiro
   - tipo: compra/venda

3. **Saída de erro:**
   ```
   Erro de validação: Campo obrigatório faltando: timestamp
   ```

## Monitoramento

### Saída do Producer

```
=== Event Hub Producer Iniciado ===
Event Hub: transacoes-financeiras
Intervalo: 1.0s
Modo CONTÍNUO: Pressione Ctrl+C para parar
Gerando transações financeiras simuladas...
[1] PETR4.SA - compra - R$ 35000.00 - Santander
[2] VALE3.SA - venda - R$ 65000.00 - Itau
[3] ITUB4.SA - compra - R$ 32000.00 - BTG
...
```

### Verificar no Event Hub

```bash
# Via Azure CLI
az eventhubs event-hub show \
  --resource-group your-rg \
  --namespace your-namespace \
  --name transacoes-financeiras
```

### Verificar no Databricks

```python
# Ler dados do Bronze Kafka
df = spark.read.format("parquet").load("abfss://bronze@stcasesantander/kafka/")
df.show()
```

## Troubleshooting

### Erro: "EVENTHUB_CONNECTION_STRING não definido"

**Solução:**
```bash
export EVENTHUB_CONNECTION_STRING="Endpoint=sb://..."
```

### Erro: "Namespace does not exist"

**Causa:** Connection string incorreta

**Solução:**
- Verificar namespace no Azure Portal
- Copiar connection string correta

### Erro: "Unauthorized"

**Causa:** Chave de acesso inválida ou expirada

**Solução:**
- Regenerar chave no Azure Portal
- Atualizar connection string

### Nenhuma transação chegando no Databricks

**Verificar:**
1. Producer está rodando?
2. Connection string está correta?
3. Event Hub está criado?
4. Event Hub está habilitado?
5. Databricks tem acesso ao Event Hub?

## Cenários de Teste

### Cenário 1: Teste Básico

```bash
# Enviar 5 transações
python scripts/eventhub_producer_advanced.py --burst 5

# Verificar no Databricks
# jobs/job_streaming.py deve processar
```

### Cenário 2: Teste de Volume

```bash
# Enviar 100 transações
python scripts/eventhub_producer_advanced.py --volume 100

# Verificar performance do Auto Loader
# jobs/job_streaming.py deve processar em micro-batches
```

### Cenário 3: Teste Contínuo

```bash
# Iniciar producer contínuo
python scripts/eventhub_producer_advanced.py --interval 1

# Iniciar job streaming contínuo
# jobs/job_streaming_continuous.py

# Verificar latência (deve ser ~1 minuto)
```

### Cenário 4: Pico de Transações

```bash
# Alta frequência (0.1 segundos)
python scripts/eventhub_producer_advanced.py --interval 0.1

# Verificar se streaming processa corretamente
# jobs/job_streaming_continuous.py deve aguentar o volume
```

## Integração com Pipeline

### Fluxo Completo

```

 PRODUCER (scripts/eventhub_producer_advanced.py)            
  ↓                                                         
  Gera transações financeiras simuladas                      
  ↓                                                         
  Envia para Azure Event Hub                                 

                   
                   

 AZURE EVENT HUB                                            
  transacoes-financeiras                                    
  ↓                                                         
  Recebe transações em tempo real                           

                   
                   

 DATABRICKS AUTO LOADER                                     
  jobs/job_streaming_continuous.py                          
  ↓                                                         
  Lê parquet do Event Hub                                   
  ↓                                                         
  Processa e salva em Bronze/Silver                         

                   
                   

 DATABRICKS STREAMING TO GOLD                               
  jobs/job_streaming_to_gold_continuous.py                  
  ↓                                                         
  Processa Silver.streaming para Gold                       
  ↓                                                         
  Gera tabelas de fraude, anomalias, volume, ranking         

```

## Próximos Passos

1. **Configurar Event Hub:**
   - Criar namespace no Azure
   - Criar Event Hub "transacoes-financeiras"
   - Obter connection string

2. **Testar Producer:**
   - Rodar modo burst (10 transações)
   - Verificar se chegou no Event Hub

3. **Testar Streaming:**
   - Iniciar job streaming contínuo
   - Rodar producer contínuo
   - Verificar latência

4. **Monitorar:**
   - Dashboard de latência
   - Métricas de throughput
   - Alertas de falha

## Dependências

```bash
pip install azure-eventhub python-dotenv
```

## Conclusão

O producer permite:
-  Testar pipeline de streaming
-  Simular ambiente de produção
-  Validar schema de dados
-  Testar performance e latência
-  Debugar problemas de ingestão
