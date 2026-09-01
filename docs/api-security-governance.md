# API Security & Governance - Boas Práticas Enterprise

## Contexto Atual

### Implementação Atual
Neste projeto, as APIs externas são executadas diretamente no mesmo processo dos jobs de ingestão:

```python
# Exemplo atual: job_extracao_acoes.py
from src.ingestion.yahoo_finance import extrair_acoes

# API roda no mesmo processo do job
extrair_acoes(spark, storage_account)
```

**Apis utilizadas:**
- Yahoo Finance (cotações de ações B3)
- BCB API (indicadores econômicos)
- World Bank API (indicadores macroeconômicos)
- Kaggle API (dataset de clientes bancários)

## Boas Práticas Enterprise

### 1. Ambiente Isolado para APIs

#### Arquitetura Recomendada em Enterprise

```

                  AMBIENTE DE PRODUÇÃO                        

                                                             
    
    LAYER DE ORQUESTRAÇÃO (Airflow/Databricks)            
    - Jobs de ingestão                                   
    - Transformações                                     
    - Carga de dados                                     
    
                                                                
    
    LAYER DE API GATEWAY (AMBIENTE ISOLADO)              
    - Proxy/Rate Limiter Centralizado                     
    - Cache de Respostas                                  
    - Monitoramento e Alertas                             
    - Autenticação e Autorização                          
    
                                                                
    
    APIs EXTERNAS (Fornecedores)                          
    - Yahoo Finance                                       
    - BCB API                                             
    - World Bank                                          
    - Kaggle                                              
    
                                                             

```

#### Por que Ambiente Isolado?

**Segurança:**
-  Isolamento de credenciais (API keys separadas)
-  Controle de acesso granular (RBAC)
-  Auditoria separada de chamadas de API
-  Prevenção de exfiltração de dados

**Operacional:**
-  Rate limiting centralizado
-  Cache de respostas (reduzir chamadas)
-  Retry automático com backoff
-  Circuit breaker (evitar cascata de falhas)
-  Monitoramento unificado

**Governança:**
-  Política de uso aprovada por Data Governance
-  Compliance com SLAs dos fornecedores
-  Gestão de custos centralizada
-  Documentação de contratos e termos de uso

### 2. Área Responsável por APIs

#### Estrutura Organizacional Recomendada

```
Comitê de Data Governance
 Representante de Segurança
 Representante de Engenharia de Dados
 Representante de Negócio
 Representante Legal/Compliance

 Área de API Management
     Estudo e Análise de APIs
     Aprovação de Utilização
     Monitoramento de SLAs
     Gestão de Credenciais
```

#### Responsabilidades da Área de API Management

**1. Estudo e Análise de APIs**
- Avaliar reputação do fornecedor
- Analisar termos de uso e licenças
- Verificar limites de rate e custos
- Avaliar disponibilidade e SLAs
- Revisar políticas de privacidade

**2. Aprovação de Utilização**
- Validar necessidade de negócio
- Aprovar credenciais e permissões
- Definir quotas de uso
- Estabelecer políticas de retenção
- Documentar justificativa de uso

**3. Monitoramento de SLAs**
- Monitorar disponibilidade das APIs
- Rastrear chamadas e erros
- Alertar sobre violações de rate limit
- Gerenciar custos de APIs pagas
- Reportar métricas de uso

**4. Gestão de Credenciais**
- Gerar e rotacionar API keys
- Armazenar em vault seguro (Key Vault)
- Implementar least privilege
- Auditoria de acesso a credenciais
- Revogação em caso de comprometimento

### 3. Processo de Aprovação de APIs

#### Fluxo de Solicitação

```

 Solicitante     
 (Eng. Dados)   

         
         

 Formulário de   
 Solicitação API 
 - Justificativa 
 - Caso de Uso   
 - Volume Est.   
 - Custos        

         
         

 Análise Técnica 
 API Management  
 - Viabilidade  
 - Alternativas  
 - Impacto       

         
         

 Revisão Segurança
 - Riscos        
 - Compliance    
 - Mitigações    

         
         

 Aprovação       
 Data Governance 
 - Autorização   
 - Quotas        
 - SLAs          

         
         

 Provisionamento 
 - Credenciais   
 - Configuração  
 - Documentação  

```

#### Template de Solicitação de API

```markdown
# Solicitação de Uso de API Externa

## Informações Gerais
- **Solicitante:** [Nome]
- **Departamento:** [Área]
- **Data:** [DD/MM/AAAA]
- **Projeto:** [Nome do projeto]

## API a ser Utilizada
- **Nome:** [Nome da API]
- **Fornecedor:** [Empresa]
- **Documentação:** [URL]
- **Tipo:** [Gratuita/Paga/Freemium]

## Justificativa de Negócio
- **Caso de Uso:** [Descrição detalhada]
- **Benefício Esperado:** [Valor para o negócio]
- **Alternativas Consideradas:** [Por que esta API?]

## Requisitos Técnicos
- **Volume Estimado:** [requests/mês]
- **Frequência:** [diária/semanal/mensal]
- **Dados Sensíveis:** [sim/não - quais?]
- **SLA Necessário:** [% disponibilidade]

## Análise de Riscos
- **Riscos de Segurança:** [Identificar]
- **Riscos de Compliance:** [Identificar]
- **Riscos Operacionais:** [Identificar]
- **Plano de Mitigação:** [Como mitigar]

## Custos
- **Custo Direto:** [R$/mês]
- **Custo Indireto:** [infrastructure, etc.]
- **ROI Estimado:** [benefício vs custo]

## Aprovações
- [ ] Engenharia de Dados
- [ ] Segurança
- [ ] Data Governance
- [ ] Negócio
```

### 4. Implementação Técnica Recomendada

#### API Gateway / Proxy

**Opção 1: Azure API Management**
```python
# Configuração via Azure API Management
from azure.mgmt.apimanagement import ApiManagementClient

# Benefits:
# - Rate limiting centralizado
# - Cache integrado
# - Monitoramento nativo
# - Segurança avançada
```

**Opção 2: API Gateway Customizado (Python/FastAPI)**
```python
# Exemplo de API Gateway simplificado
from fastapi import FastAPI, HTTPException
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
import requests

app = FastAPI()

@app.get("/api/yahoo-finance/{ticker}")
async def get_yahoo_finance(ticker: str):
    # Rate limiting
    if not rate_limiter.check("yahoo_finance"):
        raise HTTPException(429, "Rate limit exceeded")
    
    # Cache check
    cached = await cache.get(f"yahoo:{ticker}")
    if cached:
        return cached
    
    # Proxy call
    response = requests.get(f"https://query1.finance.yahoo.com/...")
    
    # Cache result
    await cache.set(f"yahoo:{ticker}", response.json(), ttl=3600)
    
    return response.json()
```

#### Centralização de Credenciais

```python
# Key Vault Integration
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

class APICredentialManager:
    def __init__(self, key_vault_url):
        credential = DefaultAzureCredential()
        self.client = SecretClient(
            vault_url=key_vault_url,
            credential=credential
        )
    
    def get_credential(self, api_name: str):
        """Busca credencial de forma segura"""
        secret_name = f"api-{api_name}-key"
        return self.client.get_secret(secret_name).value
```

### 5. Monitoramento e Observabilidade

#### Métricas a Monitorar

**Disponibilidade:**
- Uptime das APIs externas
- Latência média das chamadas
- Taxa de erro por API
- Tempo de resposta p95/p99

**Volume:**
- Requests por minuto/hora/dia
- Volume de dados transferido
- Picos de utilização
- Tendências de uso

**Custo:**
- Custo por API (se paga)
- Custo total por mês
- Projeção de custos
- Alertas de limite de orçamento

**Segurança:**
- Tentativas de acesso não autorizado
- Anomalias de uso
- Violations de rate limit
- Exposição de credenciais

#### Dashboard Recomendado

```

 API MONITORING DASHBOARD                                 

                                                         
  Disponibilidade                                        
          
   Yahoo: 99.9%  BCB: 100%    WB: 99.5%            
          
                                                         
  Volume de Chamadas (24h)                               
      
        
   Yahoo Finance                                   
   BCB API                                      
   World Bank                                 
      
                                                         
  Custo do Mês                                            
      
   Total: R$ 450,00 (45% do orçamento)                 
   Projeção: R$ 500,00 (50% do orçamento)              
      
                                                         
  Alertas Ativos                                         
   BCB API: Rate limit 85% (threshold: 90%)           
   Kaggle: Custo próximo ao limite mensal             
                                                         

```

### 6. Documentação e Compliance

#### Registro de APIs Aprovadas

```yaml
# api-registry.yaml
apis:
  yahoo_finance:
    name: "Yahoo Finance API"
    provider: "Yahoo Inc."
    approved_by: "Data Governance Committee"
    approved_date: "2024-01-15"
    expiry_date: "2025-01-15"
    rate_limit: "2000 requests/hour"
    cost: "Free"
    data_classification: "Public"
    sla: "99.5% uptime"
    contract: "https://policies.yahoo.com/us/en/yahoo/terms/apiterms/"
    
  bcb_api:
    name: "Banco Central SGS API"
    provider: "Banco Central do Brasil"
    approved_by: "Data Governance Committee"
    approved_date: "2024-01-15"
    expiry_date: "2025-01-15"
    rate_limit: "120 requests/minute"
    cost: "Free"
    data_classification: "Public"
    sla: "99.9% uptime"
    contract: "https://www.bcb.gov.br/estabilidadefinanceira/sgs"
```

### 7. Implementação Gradual

#### Roadmap de Migração

**Fase 1: Documentação e Governança (1-2 semanas)**
- [ ] Criar comitê de Data Governance
- [ ] Definir processo de aprovação
- [ ] Documentar APIs atuais
- [ ] Registrar em API Registry

**Fase 2: Centralização de Credenciais (1 semana)**
- [ ] Mover todas as API keys para Key Vault
- [ ] Implementar rotação automática
- [ ] Auditoria de acesso

**Fase 3: Rate Limiting Centralizado (2 semanas)**
- [ ] Implementar rate limiter global
- [ ] Configurar quotas por ambiente
- [ ] Monitoramento de violações

**Fase 4: API Gateway (4-6 semanas)**
- [ ] Escolher solução (Azure APIM ou custom)
- [ ] Implementar proxy
- [ ] Migrar chamadas existentes
- [ ] Testes e validação

**Fase 5: Monitoramento Avançado (2 semanas)**
- [ ] Dashboard de observabilidade
- [ ] Alertas automatizados
- [ ] Relatórios de custo
- [ ] Análise de tendências

### 8. Referências e Padrões

**Padrões de Indústria:**
- OWASP API Security Top 10
- NIST SP 800-53 (Security Controls)
- ISO 27001 (Information Security)
- GDPR (Data Protection)

**Ferramentas Recomendadas:**
- Azure API Management
- Kong API Gateway
- Apigee (Google Cloud)
- AWS API Gateway
- FastAPI (custom gateway)

**Documentação Adicional:**
- [Azure API Management Best Practices](https://docs.microsoft.com/azure/api-management/)
- [OWASP API Security](https://owasp.org/www-project-api-security/)
- [API Management Pattern](https://microservices.io/patterns/apigateway.html)

## Conclusão

A implementação atual (APIs no mesmo processo) é adequada para:
-  Projetos de desenvolvimento
-  Prova de conceito
-  Ambientes de baixo risco

Para ambientes enterprise de produção, recomenda-se:
-  Ambiente isolado para APIs
-  Área responsável por governança
-  Processo formal de aprovação
-  Monitoramento centralizado
-  Gestão de credenciais segregada

Esta transição deve ser planejada e implementada gradualmente, com foco em segurança, governança e custo-benefício.
