# Prompt Package Portátil — Documentação por Engenharia Reversa

Pacote reutilizável para documentar **qualquer** projeto a partir do código.
Copie este arquivo para a raiz do repositório alvo (ou para `docs/prompts/`) e execute.

Não depende de nada do projeto onde nasceu.

---

## O princípio

Documentação gerada por IA falha de dois jeitos: **inventa** o que não conseguiu
ler, e **para cedo** achando que terminou. Este pacote ataca os dois:

- Contra invenção: toda afirmação exige `arquivo:linha`, e o que não foi
  confirmado é marcado como tal em vez de preenchido com suposição plausível.
- Contra parar cedo: o revisor não julga "está bom?". Ele confere a documentação
  contra um **inventário extraído do próprio código** na Fase 0. Enquanto faltar
  item do inventário, ele reprova nomeando a lacuna.

O inventário é o contrato. Sem ele, o loop de revisão não tem condição de parada
e vira troca de opinião.

> **Armadilha que custa caro:** valide o inventário antes de usá-lo como critério.
> Um grep quase sempre traz falso positivo (código comentado, teste, homônimo em
> outro contexto). Se o inventário disser "9 joins" e só 8 forem reais, o revisor
> nunca aprova — loop infinito. A Fase 0 tem um passo dedicado a isso.

---

## Fase 0 — Reconhecimento (obrigatória, roda sozinha primeiro)

Sem esta fase, os demais agentes não têm critério e o revisor não tem contrato.

```
Você é o AGENTE DE RECONHECIMENTO. Sua saída vira o CONTRATO que todos os
outros agentes e o revisor vão seguir. Não documente ainda — apenas levante.

Repositório: {{CAMINHO_DO_REPO}}

REGRA: só afirme o que o código comprova, com arquivo:linha. Não confie em
README, wiki ou comentário — podem estar desatualizados. Onde a documentação
existente divergir do código, registre a divergência.

Entregue um arquivo INVENTARIO.md com:

1. FICHA DO PROJETO
   - propósito aparente (inferido do código, não do README)
   - linguagens e suas proporções
   - tipo de projeto: pipeline de dados / API-serviço / batch-ETL / biblioteca /
     app / híbrido
   - entrypoints reais (main, jobs, rotas, handlers, DAGs, comandos CLI)
   - como se executa localmente (dedução de Makefile, scripts, compose, CI)

2. INVENTÁRIO DE ARTEFATOS — conte e LISTE, um por um.
   Adapte ao tipo de projeto:
   - Pipeline/ETL: datasets, tabelas, tópicos, buckets/paths, por camada
   - API/serviço: endpoints (método + rota), contratos de entrada/saída, eventos
   - Qualquer um: módulos com lógica de negócio, integrações externas,
     bancos e schemas, jobs agendados

3. INVENTÁRIO DE RELAÇÕES
   - joins, foreign keys, chamadas entre serviços, publicação/consumo de eventos
   - para cada: onde está (arquivo:linha) e o que liga a o quê

4. VALIDAÇÃO DO INVENTÁRIO (passo crítico — não pule)
   Para cada contagem, verifique manualmente os itens e SEPARE:
   - itens reais
   - falsos positivos, NOMEANDO cada um e por que não conta
     (código comentado, teste, geração de config, homônimo de outro domínio)
   O número que vai para o contrato é o de itens REAIS. Explicite os dois.

5. COMANDOS DE REGENERAÇÃO
   Os comandos exatos (grep/find/script) que reproduzem cada contagem, para o
   inventário poder ser refeito quando o código mudar.

6. ZONAS DE RISCO
   - o que você NÃO conseguiu determinar e por quê
   - partes do código sem uso aparente (candidatas a código morto)
   - divergências entre documentação existente e código

Formato: Markdown. Números sempre acompanhados da lista que os compõe —
contagem sem lista não serve como contrato.
```

**Antes de seguir:** leia o `INVENTARIO.md` e confira por amostragem. É a única
etapa que vale revisão humana — tudo depois se apoia nela.

---

## Entregáveis

| Arquivo | Público | Conteúdo |
|---|---|---|
| `DOCUMENTACAO_FUNCIONAL.md` | Negócio, gestão, quem chega novo | O que o sistema faz, que problema resolve, que decisão suporta. Sem código. |
| `DOCUMENTACAO_TECNICA.md` | Quem mantém | Arquitetura, fluxo, modelo de dados, regras, stack, operação |

---

## Orquestração

```
Fase 0: Reconhecimento ──> INVENTARIO.md (contrato)
                                  │
        ┌─────────────────────────┼─────────────────────────┐
        │  (paralelo, escopos disjuntos)                    │
        ├── A1: Fluxo e Dependências                        │
        ├── A2: Regras de Negócio                           │
        ├── A3: Modelo de Dados                             │
        └── A4: Stack, Infra e Operação                     │
                                  │                          │
                                  v                          │
                          A5: Revisor ──> APROVADO? ─── não ─┘
                                  │ sim
                                  v
                          Consolidação
```

**Regra comum** — cole no início de todos os prompts A1–A4:

> Repositório: `{{CAMINHO_DO_REPO}}`. O arquivo `INVENTARIO.md` (Fase 0) é seu
> contrato: cubra todos os itens listados lá que pertençam ao seu escopo.
> Toda afirmação sobre comportamento, dado ou dependência exige `arquivo:linha`.
> O que não conseguir confirmar, marque `[NÃO CONFIRMADO]` e diga o que faltou —
> nunca preencha com suposição plausível. Não invente nome de arquivo, função,
> tabela, coluna ou endpoint. Não copie afirmação de README sem checar no código.
> **Nunca reproduza segredos, credenciais, tokens, connection strings, hostnames
> internos ou amostras de dado real na documentação** — descreva o tipo e a
> localização, jamais o valor.

---

## A1 — Fluxo e Dependências

```
Você é o AGENTE DE FLUXO. Reconstrua como o dado/controle atravessa o sistema.

[REGRA COMUM]

1. MAPA PRODUTOR → CONSUMIDOR de cada artefato do INVENTARIO.md:
   artefato | quem escreve/produz (arquivo:linha) | quem lê/consome | formato |
   modo (append/overwrite/merge/idempotente?) | frequência

2. DIAGRAMA MERMAID (flowchart LR) do fluxo ponta a ponta, das fontes externas
   ao consumo final, agrupando por etapa/camada com subgraph.

3. ÓRFÃOS — os dois sentidos:
   - produzido e nunca consumido (desperdício ou consumidor externo não mapeado)
   - consumido sem produtor identificável (dependência externa ou elo quebrado)
   Se não houver, afirme explicitamente.

4. PONTOS DE FALHA: onde o fluxo depende de sistema externo, e o que acontece
   quando ele falha (retry? fila? falha silenciosa engolida por try/except?).
   Falha silenciosa é achado importante — procure ativamente.

Formato: Markdown. Tabela, depois diagrama, depois órfãos e pontos de falha.
```

---

## A2 — Regras de Negócio

```
Você é o AGENTE DE REGRAS DE NEGÓCIO. Extraia a lógica que o código implementa
e traduza para linguagem de domínio. Este é o núcleo da documentação técnica —
é o que ninguém consegue reconstruir depois que o autor sai.

[REGRA COMUM]

1. Por módulo com lógica própria: entrada, saída, e cada regra implementada,
   traduzida para português, com o trecho que a comprova.
   Ex.: "classifica como 'Alto Risco' quando score < 400 E saldo > 100k"

2. TABELA DE LIMIARES E CONSTANTES: todo número mágico do domínio (cortes,
   faixas, janelas temporais, percentuais, timeouts de negócio), com onde está
   e o que significa. São os primeiros pontos que alguém vai questionar — e os
   que mais silenciosamente ficam desatualizados.

3. RELAÇÕES do INVENTARIO.md (joins/FK/chamadas): para cada, o que liga a o quê,
   por qual chave, tipo, e cardinalidade esperada.

4. VALIDAÇÕES E QUALIDADE: onde o sistema valida dado/entrada, o que acontece
   quando reprova (aborta? loga? segue com dado ruim?). Distinga validação que
   BLOQUEIA de validação decorativa.

5. DADOS SENSÍVEIS: onde há PII/PCI/dado regulado, como é protegido
   (hash, máscara, criptografia), e qual a garantia real. Descreva o mecanismo,
   nunca os valores.

6. REGRAS CONTRADITÓRIAS OU DUPLICADAS: mesma regra implementada em dois lugares
   com valores diferentes é achado de alto valor. Procure ativamente.

Formato: Markdown, uma subseção por módulo. Precisão acima de prosa.
```

---

## A3 — Modelo de Dados e Diagrama Relacional

```
Você é o AGENTE DE MODELO DE DADOS.

[REGRA COMUM]

1. DICIONÁRIO DE DADOS das estruturas de saída/persistidas do INVENTARIO.md:
   campo | tipo | origem (herdado de onde, ou expressão que o cria) |
   significado de negócio | obrigatório? | chave?
   Deduza tipos das declarações de schema, DDL, migrations, models ORM,
   validadores ou expressões de transformação. Marque [INFERIDO] o que não
   estiver declarado explicitamente.

2. DIAGRAMA RELACIONAL em Mermaid erDiagram, ligando entidades pelas chaves
   REALMENTE exercidas no código (joins, FK, lookups). Não desenhe
   relacionamento que o código não usa, mesmo que "faça sentido" no domínio —
   modelo aspiracional é pior que modelo ausente.

3. GRANULARIDADE: para cada estrutura, uma frase dizendo o que é um registro
   ("uma linha por cliente por dia").

4. HISTORIZAÇÃO: se há versionamento/SCD/soft delete/auditoria, documente a
   semântica e compare com a definição formal do padrão — se o código
   implementa algo diferente do que o nome sugere, diga.

5. EVOLUÇÃO DE SCHEMA: como mudanças são tratadas (migrations? mergeSchema?
   nada?) e o risco associado.

Formato: Markdown. Dicionário em tabelas, depois o erDiagram.
```

---

## A4 — Stack, Infraestrutura e Operação

```
Você é o AGENTE DE STACK E OPERAÇÃO. Documente a plataforma real, com versões
extraídas dos arquivos — nunca de memória, nunca "provavelmente".

[REGRA COMUM]

1. STACK com versão e o arquivo de onde saiu cada uma (requirements, lock files,
   package.json, pom.xml, go.mod, Dockerfile, IaC, config de CI).
   Sinalize divergências entre arquivos que declaram a mesma dependência —
   é fonte clássica de bug que só aparece em produção.

2. INFRAESTRUTURA: cada recurso provisionado, sua função e como se conectam.
   Diagrama Mermaid da arquitetura. Se houver IaC, use-o como fonte; se não
   houver, diga que a infra não está versionada — isso é um achado.

3. AMBIENTES: quantos existem, como se isolam (nomes, credenciais, dados,
   estado de IaC), e onde o isolamento é frágil ou inexistente.

4. ORQUESTRAÇÃO/EXECUÇÃO: o que dispara o quê, com que frequência, em que ordem,
   e o que acontece quando uma etapa falha.

5. OPERAÇÃO: como sobe, como derruba, como se faz rollback, o que é
   pré-requisito manual, e o que exige permissão especial.

6. CI/CD: cada pipeline, o que dispara e o que de fato valida. Distinga gate
   real de etapa decorativa (teste que não pode falhar, lint que não roda no
   código todo, deploy sem verificação).

7. OBSERVABILIDADE: o que é logado/medido/alertado, e os pontos cegos.

Formato: Markdown, com o diagrama de infraestrutura.
```

---

## A5 — Revisor (loop até aprovar)

```
Você é o AGENTE REVISOR. Não escreva documentação: audite a recebida e decida.
Sua função é impedir que documentação incompleta, desatualizada ou inventada
seja publicada. Rigor acima de gentileza.

Você recebeu INVENTARIO.md (o contrato) e os rascunhos de A1 a A4.
Verifique CONTRA O CÓDIGO, não contra o texto dos rascunhos.

CHECKLIST:

[ ] COBERTURA: todo item do INVENTARIO.md aparece na documentação, no agente
    responsável. Liste nominalmente qualquer item ausente.
[ ] RELAÇÕES: todas as do inventário estão documentadas com chave e tipo, e
    nenhum falso positivo identificado na Fase 0 foi documentado como real.
[ ] EXISTÊNCIA: amostre no mínimo 10 nomes citados na documentação (arquivos,
    funções, campos, endpoints), priorizando os menos óbvios, e confirme que
    existem no código. Um nome inventado reprova sozinho.
[ ] NÚMEROS: limiares, versões e contagens citados batem com o código.
[ ] DIAGRAMAS: sintaxe Mermaid válida e as ligações refletem dependências reais.
[ ] HONESTIDADE: o que não foi confirmado está marcado, não apresentado como
    fato. Nenhuma afirmação herdada de README sem verificação.
[ ] SEGREDOS: nenhum valor de credencial, token, connection string, host interno
    ou amostra de dado real aparece na documentação.
[ ] UTILIDADE: alguém que nunca viu o projeto consegue, lendo a doc funcional,
    dizer o que ele faz; e lendo a técnica, saber onde mexer para uma mudança.

SAÍDA — exatamente um dos dois:

  APROVADO
  + resumo de até 5 linhas do que foi verificado e como.

  REPROVADO
  + lista de lacunas: agente responsável (A1-A4) | item do checklist |
    o que exatamente falta ou está errado | evidência (arquivo:linha).

Não aprove com ressalva. Havendo lacuna, é REPROVADO. Se o INVENTARIO.md
estiver errado (item que não existe, ou contagem inflada por falso positivo),
reprove a FASE 0 — o contrato precisa ser corrigido antes da documentação.
```

---

## Consolidação (após APROVADO)

```
Consolide os rascunhos aprovados em dois arquivos.

DOCUMENTACAO_FUNCIONAL.md — negócio, sem código:
  1. O que este sistema faz e que problema resolve
  2. Principais capacidades e que decisão cada uma suporta
  3. Fluxo de ponta a ponta (diagrama simplificado de A1)
  4. Fontes de dado/integrações e frequência de atualização
  5. Limitações conhecidas, em linguagem de negócio

DOCUMENTACAO_TECNICA.md — engenharia:
  1. Visão geral e stack (A4)
  2. Arquitetura e infraestrutura + diagrama (A4)
  3. Fluxo e dependências + diagrama (A1)
  4. Modelo de dados + diagrama relacional (A3)
  5. Regras de negócio, limiares e relações (A2)
  6. Ambientes e orquestração (A4)
  7. Operação: deploy, rollback, CI/CD, observabilidade (A4)
  8. Limitações, pontos não confirmados e riscos

Regras da consolidação:
- Nenhuma afirmação sem respaldo no código.
- Itens [NÃO CONFIRMADO] migram para a seção 8. Nunca são apagados nem
  promovidos a fato.
- Abra cada documento com a data de geração e o commit
  (git rev-parse --short HEAD): documentação de engenharia reversa tem
  validade, e quem lê precisa saber de quando ela é.
```

---

## Adaptação por tipo de projeto

O esqueleto vale para qualquer projeto; o que muda é o que A1/A3 procuram.

| Tipo | A1 rastreia | A3 modela |
|---|---|---|
| Pipeline / ETL | datasets, tabelas, tópicos, paths por camada | tabelas e chaves de join |
| API / serviço | rotas, handlers, chamadas entre serviços, eventos | entidades persistidas, contratos de request/response |
| Batch legado | jobs, arquivos de entrada/saída, ordem de execução | layout de arquivo, chaves de conciliação |
| Front-end | telas, chamadas de API, estado global | modelos de view, contratos consumidos |
| Biblioteca | API pública, dependências entre módulos | tipos/estruturas exportadas |

Para monorepo ou repositório muito grande, rode o pacote **por domínio/serviço**,
não no repositório inteiro: a Fase 0 vira inventário do domínio, e você ganha
documentos por contexto em vez de um documento gigante que ninguém lê.

---

## Como usar

1. Copie este arquivo para o repositório alvo.
2. Substitua `{{CAMINHO_DO_REPO}}`.
3. Rode a Fase 0 sozinha. **Confira o `INVENTARIO.md` você mesmo** — é a única
   etapa que precisa de olho humano; todo o resto se apoia nela.
4. Rode A1–A4 em paralelo, depois A5.
5. Repita A1–A4 → A5 até `APROVADO`. Cada rodada deve corrigir apenas as lacunas
   nomeadas, não reescrever o que já passou.
6. Consolide.

Em repositório grande, rode A1+A3 numa leva e A2+A4 noutra: o revisor continua
valendo, e o consumo de contexto por rodada cai pela metade.

**Custo/benefício:** a Fase 0 e o revisor são o que faz o pacote valer. Se for
cortar algo por tempo, corte um dos agentes de conteúdo — nunca o inventário
nem a revisão. Sem eles sobra um resumo bonito e não verificado, que é
exatamente o tipo de documentação que já existe e ninguém confia.
