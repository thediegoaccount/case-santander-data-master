# Análise: Arquitetura Kappa vs Lambda

## Contexto Atual do Projeto

### Fontes de Dados

**Batch (Não Streaming):**
- Yahoo Finance (cotações históricas)
- BCB API (indicadores econômicos)
- World Bank API (PIB, desemprego)
- Kaggle (dataset de clientes)

**Streaming (Tempo Real):**
- Azure Event Hub (transações financeiras em tempo real)

### Arquitetura Atual (Híbrida)

```

 BATCH LAYER (Lambda)                                       
                                                             
  Fontes Externas → Bronze → Silver → Gold                 
  - Yahoo Finance, BCB, World Bank, Kaggle                  
  - Execução diária às 06:00                                
  - Análises históricas                                     



 SPEED LAYER (Kappa)                                        
                                                             
  Event Hub → Bronze → Silver → Gold (Streaming)           
  - Transações em tempo real                                
  - Detecção de fraudes                                     
  - Análises intraday                                       



 SERVING LAYER                                              
                                                             
  Gold Tables → Azure SQL → Dashboard                      
  - Consolidação de batch + streaming                       

```

## Arquitetura Lambda

### Conceito

**Definição:** Arquitetura com duas camadas distintas:
- **Batch Layer:** Processa todos os dados históricos (análises precisas)
- **Speed Layer:** Processa dados em tempo real (análises recentes)
- **Serving Layer:** Combina resultados de ambas

### Características

**Vantagens:**
-  **Precisão histórica:** Batch layer garante análise completa
-  **Flexibilidade:** Cada camada otimizada para seu caso de uso
-  **Resiliência:** Falha em streaming não afeta histórico
-  **Simplicidade:** Batch mais simples de implementar

**Desvantagens:**
-  **Complexidade:** Dois pipelines para manter
-  **Duplicação:** Lógica pode ser duplicada entre batch/streaming
-  **Latência:** Batch tem latência maior
-  **Consistência:** Reconciliação entre batch e streaming

### Aplicação ao Caso Santander

**Como ficaria:**

```

 BATCH LAYER (Existe hoje)                                  
                                                             
  Fontes Externas (Yahoo, BCB, WB, Kaggle)                 
  + Event Hub (snapshot diário)                             
  ↓                                                         
  Bronze → Silver → Gold (Batch)                           
  ↓                                                         
  Análises históricas completas                             



 SPEED LAYER (Existe hoje)                                  
                                                             
  Event Hub (tempo real)                                    
  ↓                                                         
  Bronze → Silver → Gold (Streaming)                       
  ↓                                                         
  Análises em tempo real (fraudes, anomalias)               



 SERVING LAYER (Existe hoje)                                
                                                             
  Gold (Batch) + Gold (Streaming) → SQL → Dashboard         
  ↓                                                         
  Consolidação (merge ou union)                             

```

**Complexidade:**
- Manter dois pipelines (batch e streaming)
- Reconciliar dados entre batch e streaming
- Lógica duplicada para algumas análises

**Desafios específicos:**
- Como unir batch de clientes com streaming de transações?
- Como garantir consistência entre fraude (batch) e fraude (streaming)?
- Como lidar com atualizações tardias no batch?

## Arquitetura Kappa

### Conceito

**Definição:** Arquitetura onde tudo é processado como streaming
- **Log Imutável:** Todos os dados são armazenados como log de eventos
- **Replay:** Estado pode ser recriado processando o log novamente
- **Single Pipeline:** Um único sistema de processamento

### Características

**Vantagens:**
-  **Simplicidade:** Um único pipeline para tudo
-  **Consistência:** Sem reconciliação entre batch/streaming
-  **Flexibilidade:** Replay do log para recriar estado
-  **Evolução:** Lógica pode ser alterada e reprocessada

**Desvantagens:**
-  **Dependência de Streaming:** Tudo depende de infraestrutura de streaming
-  **Complexidade de Estado:** Gerenciamento de estado em streaming
-  **Mudança de Paradigma:** APIs batch precisam ser convertidas para streaming
-  **Retenção:** Log imutável requer retenção de longo prazo

### Aplicação ao Caso Santander

**Como ficaria:**

```

 EVENT LOG (Imutável)                                       
                                                             
  Event Hub (todos os eventos)                              
  ↓                                                         
  Bronze (log imutável - Delta Lake)                       
  ↓                                                         
  Retenção: 90 dias (ou mais)                              

                   
                   

 STREAMING PROCESSING (Único Pipeline)                      
                                                             
  Bronze → Silver → Gold (Streaming)                       
  ↓                                                         
  State Management (agregações, janelas)                    
  ↓                                                         
  Replay: Recriar estado processando log                    

                   
        
                               
  
 Análises Atuais     Análises        
 (Tempo Real)        Históricas      
  ↓ Replay            ↓ Replay       
 Últimas 24h         Últimos 90 dias  
  
```

**Mudanças necessárias:**

1. **Converter APIs Batch para Streaming:**
   - Yahoo Finance: Não é nativamente streaming
     - Solução: Simular streaming via schedule frequente (ex: a cada 15 min)
   - BCB API: Não é nativamente streaming
     - Solução: Simular streaming via schedule frequente
   - World Bank API: Não é nativamente streaming
     - Solução: Simular streaming via schedule frequente
   - Kaggle: Dataset estático
     - Solução: Tratar como snapshot inicial + updates se houver

2. **Gerenciamento de Estado:**
   - Agregações em streaming (som, count, avg)
   - Janelas de tempo (tumbling, sliding, session)
   - Watermarks para lidar com dados tardios

3. **Log Imutável:**
   - Armazenar todos os eventos em Delta Lake
   - Retenção de longo prazo (90+ dias)
   - Capacidade de replay completo

**Complexidade:**
- Migrar APIs batch para streaming (simulado)
- Implementar gerenciamento de estado complexo
- Mudança de paradigma na lógica de negócio

**Desafios específicos:**
- Como fazer Yahoo Finance ser streaming?
- Como lidar com APIs que não suportam streaming nativo?
- Como gerenciar estado de agregações históricas?
- Como fazer replay de 90 dias de dados?

## Comparação Detalhada

### Tabela Comparativa

| Aspecto | Lambda (Atual) | Kappa (Proposto) |
|---------|----------------|------------------|
| **Complexidade de Implementação** | Média | Alta |
| **Número de Pipelines** | 2 (batch + streaming) | 1 (streaming) |
| **Fontes Externas** | Batch nativo | Precisa simular streaming |
| **Gerenciamento de Estado** | Simples (batch) | Complexo (streaming) |
| **Precisão Histórica** | Alta (batch completo) | Alta (replay) |
| **Latência** | Alta (batch) | Baixa (streaming) |
| **Consistência** | Requer reconciliação | Automática |
| **Replay** | Difícil (reexecutar batch) | Fácil (reprocessar log) |
| **Resiliência** | Alta (batch independente) | Média (depende de streaming) |
| **Custo** | Duplo (batch + streaming) | Único (streaming) |
| **Manutenção** | Duplicação de lógica | Lógica única |
| **Curva de Aprendizado** | Baixa | Alta |

### Complexidade por Área

#### 1. Ingestão de Dados

**Lambda (Atual):**
-  APIs batch: Simples (chamada HTTP diária)
-  Event Hub: Simples (Auto Loader)
- **Complexidade:** Baixa

**Kappa:**
-  APIs batch: Complexo (simular streaming)
-  Event Hub: Simples (mesmo)
- **Complexidade:** Alta

#### 2. Transformação

**Lambda (Atual):**
-  Batch: Simples (Spark SQL)
-  Streaming: Médio (Structured Streaming)
- **Complexidade:** Média

**Kappa:**
-  Tudo como streaming: Alto (Structured Streaming avançado)
- **Complexidade:** Alta

#### 3. Gerenciamento de Estado

**Lambda (Atual):**
-  Batch: Sem estado (reprocessa tudo)
-  Streaming: Simples (checkpoint)
- **Complexidade:** Baixa

**Kappa:**
-  Streaming: Complexo (stateful aggregations, watermarks)
- **Complexidade:** Alta

#### 4. Manutenção

**Lambda (Atual):**
-  Duplicação de lógica em alguns casos
-  Pipelines independentes
- **Complexidade:** Média

**Kappa:**
-  Lógica única
-  Single point of failure
- **Complexidade:** Média

## Análise para o Caso Santander

### Pontos Chave

1. **Fontes de Dados:**
   - 4 APIs que não são nativamente streaming
   - 1 fonte nativamente streaming (Event Hub)
   - **Conclusão:** Lambda é mais natural

2. **Análises Necessárias:**
   - Históricas (performance ao longo do tempo)
   - Em tempo real (fraudes, anomalias)
   - **Conclusão:** Lambda atende bem ambos

3. **Equipe:**
   - Equipe familiarizada com Spark batch
   - Streaming é mais complexo
   - **Conclusão:** Lambda é mais fácil de aprender

4. **Custo:**
   - Lambda: Duplo (batch + streaming)
   - Kappa: Único (streaming)
   - **Conclusão:** Kappa pode ser mais barato

5. **Tempo de Implementação:**
   - Lambda: J existe, precisa apenas refinar
   - Kappa: Precisa re-arquitetar tudo
   - **Conclusão:** Lambda é muito mais rápido

### Recomendação

##  Lambda (Arquitetura Atual) - Mais Fácil

### Por que Lambda é mais fácil para este caso:

1. **Fontes de Dados Naturais:**
   - APIs batch (Yahoo, BCB, World Bank, Kaggle) não são streaming
   - Converter para streaming seria artificial e complexo
   - Event Hub já é usado nativamente para streaming

2. **Análises Híbridas:**
   - Você precisa de análises históricas (Lambda é ideal)
   - Você precisa de análises em tempo real (Speed layer funciona)
   - Lambda permite otimizar cada caso

3. **Curva de Aprendizado:**
   - Batch Spark é mais simples que Streaming Spark
   - Equipe provavelmente já conhece batch
   - Streaming stateful é complexo

4. **Resiliência:**
   - Falha no streaming não quebra análises históricas
   - Batch é mais estável e previsível

5. **Implementação:**
   - Arquitetura já existe
   - Apenas precisa refinar e documentar
   - Tempo de implementação: dias (não semanas/meses)

### Otimizações Sugeridas para Lambda Atual:

1. **Reduzir Duplicação:**
   - Mover lógica comum para módulos reutilizáveis
   - Usar Delta Lake como fonte única de verdade

2. **Melhorar Reconciliação:**
   - Implementar merge automático batch → streaming
   - Adicionar timestamps de versão

3. **Padronizar Schema:**
   - Usar schema evolution do Delta Lake
   - Garantir consistência entre batch e streaming

4. **Monitoramento:**
   - Adicionar alerts para desvios entre batch e streaming
   - Dashboard de latência e consistência

## Quando Considerar Kappa?

### Ficaria Kappa se:

1. **Todas as fontes fossem streaming nativo:**
   - APIs emitissem eventos em tempo real
   - Não houvesse fontes batch

2. **Precisasse de replay frequente:**
   - Lógica de negócio mudasse constantemente
   - Precisasse reprocessar histórico com nova lógica

3. **Tivesse equipe especializada:**
   - Engenheiros com experiência em streaming
   - Conhecimento profundo de stateful processing

4. **Custo fosse crítico:**
   - Duplicação de infraestrutura fosse proibitiva
   - Economia justificasse complexidade

## Conclusão

### Para o Caso Santander: **Lambda é mais fácil**

**Motivos:**
1.  Arquitetura já existe (economia de tempo)
2.  Fontes de dados são naturalmente batch + streaming
3.  Curva de aprendizado menor
4.  Menor risco de implementação
5.  Mais resiliente e previsível

**Esforço Estimado:**
- **Lambda (refinar):** 2-3 dias
- **Kappa (re-arquitetar):** 4-6 semanas

**Recomendação:**
- Manter arquitetura Lambda atual
- Otimizar pontos de dor (duplicação, reconciliação)
- Considerar Kappa apenas se requisitos mudarem drasticamente
