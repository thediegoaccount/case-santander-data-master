# Prompt Package Portátil — Documentação por Engenharia Reversa

Pacote reutilizável para documentar **um ou vários repositórios** a partir do código,
com orquestração externa (Control-M), tabelas parametrizadas e regra de negócio
como centro da entrega.

Copie este arquivo para onde for executar. Não depende do projeto onde nasceu.

---

## O princípio

Documentação gerada por IA falha de três jeitos: **inventa** o que não conseguiu ler,
**para cedo** achando que terminou, e **descreve o "o quê" sem o "porquê"** — diz que
um campo é calculado, mas não a regra completa que o calcula. Este pacote ataca os três:

| Falha | Defesa |
|---|---|
| Inventar | Toda afirmação exige `arquivo:linha`. O não confirmado é marcado, não preenchido com suposição. |
| Parar cedo | O revisor confere contra um **inventário extraído do código** (Fase 0). Falta item = reprovado. |
| Superficialidade | **Varredura campo a campo** (Fase V): nenhum campo final passa sem a regra COMPLETA. |

Acima de tudo isso fica o **Agente Master**: quem decide, com critério de PO de negócio
e de engenheiro de dados sênior. O revisor diz se está *completo*; o Master diz se está
*certo e útil*.

> **Armadilha que custa caro:** valide o inventário antes de usá-lo como critério.
> Contagem inflada por falso positivo torna o checklist do revisor insatisfazível
> e o loop nunca fecha. Caso real, medido: `grep ".join("` devolveu **9** num
> projeto; só **7** eram join de dado. Dois falsos positivos, de naturezas
> diferentes — um era `", ".join(...)`, o `str.join` do Python dentro de um
> gerador de código; o outro eram os dois ramos de um mesmo `if/else`
> (broadcast vs sort-merge), que é **um** join lógico contado duas vezes.
> Nenhum dos dois sai por regex; só olhando o código. Por isso a Fase 0 tem um
> passo dedicado a separar reais de falsos positivos, nomeando cada exclusão.

---

## Parâmetros de entrada

Preencha antes de executar:

```
{{REPOS}}              lista de repositórios, um por linha, com caminho local ou URL
                       ex.: /work/repo-ingestao
                            /work/repo-transformacao
                            /work/repo-publicacao

{{BRANCH}}             branch a ler em cada repositório (padrão: master)

{{CONTROL_M}}          caminho(s) dos arquivos de definição de job Control-M
                       (.yaml/.yml/.xml/.json), se estiverem fora dos repos

{{APP_ENTRYPOINT}}     arquivo que centraliza parametrização (padrão: app.py)
                       É de onde saem os NOMES REAIS das tabelas quando o
                       código as referencia por parâmetro/variável.

{{DOMINIO}}            domínio de negócio (ex.: risco de crédito, cobrança)
                       Orienta o Master a julgar se a regra faz sentido.
```

---

## Hierarquia e orquestração

```
                    ┌───────────────────────────────┐
                    │   AM — AGENTE MASTER          │
                    │   PO de negócio + eng. sênior │
                    │   decide, arbitra, aprova     │
                    └───────────────┬───────────────┘
                                    │ dirige e recebe
   ┌────────────────────────────────┼────────────────────────────────┐
   │                                │                                │
Fase 0: Reconhecimento multi-repo ──> INVENTARIO.md (contrato)
   │
   ├── A1: Fluxo, dependências e ORDEM DE EXECUÇÃO (Control-M, cross-repo)
   ├── A2: Regras de Negócio            (paralelo, escopos disjuntos)
   ├── A3: Modelo de Dados campo a campo
   └── A4: Stack, Infra e Operação
                                    │
                                    v
                    A5: Revisor (objetivo, checklist)
                                    │ passou?
                                    v
                    Fase V: Varredura campo a campo
                                    │ todo campo com regra completa?
                                    v
                    AM: julgamento final ──> APROVADO / REPROVADO
                                    │ reprovado: volta ao agente responsável
                                    v
                            Consolidação
```

**Regra comum** — cole no início de A1–A4:

> Repositórios: `{{REPOS}}`, branch `{{BRANCH}}`. Domínio: `{{DOMINIO}}`.
> O `INVENTARIO.md` (Fase 0) é seu contrato: cubra todo item do seu escopo.
> Toda afirmação exige `arquivo:linha` **com o nome do repositório** — em
> multi-repo, `job.py:120` sem repo é inútil. Use `repo/caminho/arquivo.py:120`.
> O que não conseguir confirmar, marque `[NÃO CONFIRMADO]` e diga o que faltou —
> nunca preencha com suposição plausível. Não invente nome de arquivo, função,
> tabela, coluna, job ou endpoint. Não copie afirmação de README sem checar no código.
> **Nunca reproduza segredos, credenciais, tokens, connection strings, hostnames
> internos ou amostras de dado real** — descreva o tipo e a localização, jamais o valor.
>
> **Gravação dos artefatos:** use a ferramenta de escrita de arquivo direta
> (Write/editor), nunca shell com heredoc. Os artefatos deste pacote passam de
> mil linhas e a escrita via shell estoura timeout — o trabalho inteiro se perde
> no último passo, depois de já estar pronto. Grave incrementalmente quando o
> documento for muito grande.

---

## Fase 0 — Reconhecimento multi-repo (obrigatória)

Sem esta fase os demais agentes não têm critério e o revisor não tem contrato.

```
Você é o AGENTE DE RECONHECIMENTO. Sua saída é o CONTRATO que todos os outros
agentes e o revisor vão seguir. Não documente ainda — levante.

Repositórios: {{REPOS}}   Branch: {{BRANCH}}
Control-M: {{CONTROL_M}}  Entrypoint parametrizado: {{APP_ENTRYPOINT}}

REGRA: só afirme o que o código comprova, com repo/arquivo:linha. Não confie em
README ou wiki — podem estar desatualizados. Registre divergências que encontrar.

Entregue INVENTARIO.md com:

1. FICHA DE CADA REPOSITÓRIO
   Para cada repo em {{REPOS}}, confirme que está lendo a branch {{BRANCH}}
   (git rev-parse --abbrev-ref HEAD) e registre o commit lido
   (git rev-parse --short HEAD). Documentação de engenharia reversa sem o
   commit de origem não é auditável.
   - papel do repo no conjunto (ingestão? transformação? publicação?)
   - linguagens, entrypoints, como se executa

2. TABELAS PARAMETRIZADAS — ler {{APP_ENTRYPOINT}} de cada repo
   Muitos projetos referenciam tabelas por variável/parâmetro, não por
   literal. O nome REAL está na parametrização, não no ponto de uso.
   - abra {{APP_ENTRYPOINT}} e extraia o mapa completo
     parâmetro/variável -> nome real da tabela (schema.tabela)
   - se houver parametrização por ambiente (dev/hml/prd), registre os três
   - depois, ao encontrar uma referência por variável no código, RESOLVA para
     o nome real usando esse mapa
   - se um parâmetro não resolver, liste como [NÃO RESOLVIDO] com o local de uso
   Entregue esse mapa como tabela. Ele é insumo obrigatório de A1, A2 e A3.

3. JOBS CONTROL-M — ler os .yaml/.yml (ou .xml/.json) de definição
   Para cada job: nome, repo/script que executa, agendamento (cron/calendário),
   predecessores e sucessores, condições (IN/OUT conditions), e recursos/locks.
   Esta é a fonte da ORDEM DE EXECUÇÃO real — não deduza ordem do código.

4. INVENTÁRIO DE ARTEFATOS — conte e LISTE um a um, por repositório:
   tabelas/datasets por camada, arquivos de entrada/saída, tópicos/filas,
   integrações externas, módulos com lógica de negócio, jobs agendados.
   Nomes de tabela SEMPRE resolvidos pelo mapa do item 2.

5. INVENTÁRIO DE RELAÇÕES
   joins, foreign keys, chamadas entre serviços, dependências entre repos.
   Para cada: repo/arquivo:linha e o que liga a o quê.

6. INVENTÁRIO DE CAMPOS FINAIS  (insumo da Fase V)
   Liste TODOS os campos de TODAS as tabelas finais (as que são consumidas
   por fora: BI, API, cliente, outro sistema). Só a lista de nomes, sem regra —
   a regra é trabalho de A2/A3. Esta lista é o denominador da varredura: a
   Fase V vai exigir regra completa para cada um destes campos.

7. VALIDAÇÃO DO INVENTÁRIO (passo crítico — não pule)
   Para cada contagem, verifique item a item e SEPARE:
   - itens reais
   - falsos positivos, NOMEANDO cada um e por que não conta (código comentado,
     teste, geração de config, homônimo de outro domínio)
   O contrato leva o número de itens REAIS. Explicite os dois números.

8. COMANDOS DE REGENERAÇÃO
   Os comandos exatos que reproduzem cada contagem, para refazer o inventário
   quando o código mudar.

9. ZONAS DE RISCO
   O que não conseguiu determinar e por quê; código sem uso aparente;
   divergências entre documentação existente e código; parâmetros não resolvidos.

Formato: Markdown. Número sempre acompanhado da lista que o compõe —
contagem sem lista não serve como contrato.
```

**Antes de seguir:** o Master revisa o `INVENTARIO.md`. É a etapa que mais
merece olho humano também — tudo depois se apoia nela.

---

## A1 — Fluxo, Dependências e Ordem de Execução

```
Você é o AGENTE DE FLUXO E ORQUESTRAÇÃO.

[REGRA COMUM]

1. ORDEM DE EXECUÇÃO GLOBAL (entrega principal deste agente)
   A partir dos jobs Control-M do INVENTARIO.md, monte a sequência real de
   ponta a ponta, ATRAVESSANDO OS REPOSITÓRIOS:

   ordem | job Control-M | repositório | script/entrypoint | predecessores |
   agendamento | tabelas lidas | tabelas escritas

   - a ordem vem das dependências do Control-M, não da leitura do código
   - onde um job de um repo depende da saída de outro repo, marque como
     FRONTEIRA ENTRE REPOSITÓRIOS — é onde a documentação costuma ter buraco
     e onde quebra em produção
   - identifique paralelismo real (jobs sem dependência entre si)
   - identifique o caminho crítico

2. DIAGRAMA MERMAID (flowchart LR) da cadeia de execução, com subgraph POR
   REPOSITÓRIO, deixando visível o que cruza fronteira de repo.

3. MAPA PRODUTOR → CONSUMIDOR de cada artefato:
   artefato | quem produz (repo/arquivo:linha) | quem consome | formato |
   modo (append/overwrite/merge/idempotente) | frequência

4. ÓRFÃOS, nos dois sentidos: produzido e nunca consumido; consumido sem
   produtor identificável. Se não houver, afirme explicitamente.

5. PONTOS DE FALHA: onde depende de sistema externo, o que acontece na falha
   (retry? fila? falha silenciosa engolida por try/except?), e se um job
   Control-M que falha bloqueia ou libera os sucessores.
   Falha silenciosa é achado importante — procure ativamente.

Formato: Markdown. Ordem de execução primeiro, depois diagrama, depois o resto.
```

---

## A2 — Regras de Negócio (núcleo da entrega)

```
Você é o AGENTE DE REGRAS DE NEGÓCIO. Extraia a lógica que o código implementa
e traduza para linguagem de domínio ({{DOMINIO}}). É o que ninguém consegue
reconstruir depois que o autor sai — e é o centro da documentação funcional.

[REGRA COMUM]

1. Por módulo com lógica própria: entrada, saída, e cada regra implementada,
   traduzida para português de negócio, com o trecho que a comprova.

2. REGRA COMPLETA, não resumo. Para cada campo derivado, a documentação
   precisa conter TODOS os itens abaixo — a Fase V vai cobrar um a um:
   a) fórmula/expressão exata
   b) TODOS os ramos condicionais (todo when/case/if, inclusive o else/default)
   c) origem de cada insumo (tabela.campo de onde vem)
   d) tratamento de nulo, zero, divisão por zero e valor ausente
   e) domínio de valores possíveis na saída
   f) unidade e/ou moeda, quando aplicável
   Escrever "calculado a partir do saldo" é REPROVADO. O que se espera é
   "saldo_medio = soma(saldo_diario) / dias_uteis_mes; se dias_uteis_mes = 0,
   retorna null; valores em BRL".

3. TABELA DE LIMIARES E CONSTANTES: todo número mágico do domínio (cortes,
   faixas, janelas, percentuais), onde está e o que significa. São os
   primeiros pontos que a área de negócio vai questionar.

4. RELAÇÕES do inventário (joins/FK/chamadas): o que liga a o quê, por qual
   chave, tipo e cardinalidade esperada.

5. VALIDAÇÕES: onde o sistema valida, e o que acontece quando reprova (aborta?
   loga e segue? grava dado ruim?). Distinga validação que BLOQUEIA de
   validação decorativa.

6. DADOS SENSÍVEIS: onde há PII/dado regulado, como é protegido e qual a
   garantia real. Mecanismo sim, valores nunca.

7. REGRAS CONTRADITÓRIAS OU DUPLICADAS: a mesma regra implementada em dois
   repositórios com valores diferentes é achado de altíssimo valor.
   Procure ativamente, principalmente nas fronteiras entre repos.

Formato: Markdown, uma subseção por módulo. Precisão acima de prosa.
```

---

## A3 — Modelo de Dados campo a campo

```
Você é o AGENTE DE MODELO DE DADOS.

[REGRA COMUM]

1. DICIONÁRIO DE DADOS de cada tabela final do INVENTARIO.md (item 6):
   campo | tipo | origem (tabela.campo ou expressão) | regra de preenchimento |
   obrigatório? | chave? | domínio de valores
   Use nomes de tabela RESOLVIDOS pelo mapa de parametrização do {{APP_ENTRYPOINT}}.
   Deduza tipos de DDL, migrations, models, schemas declarados ou expressões.
   Marque [INFERIDO] o que não estiver declarado.

2. Nenhum campo pode ficar com regra vazia, "n/a" ou genérica. Se não
   conseguiu determinar a regra de um campo, escreva [NÃO CONFIRMADO] com o
   que já sabe e o que faltou. A Fase V trata campo sem regra como lacuna.

3. DIAGRAMA RELACIONAL em Mermaid erDiagram, ligando entidades pelas chaves
   REALMENTE exercidas no código. Não desenhe relacionamento que o código não
   usa, mesmo que faça sentido no domínio — modelo aspiracional é pior que
   modelo ausente.

4. GRANULARIDADE: uma frase por tabela dizendo o que é um registro
   ("uma linha por contrato por dia de referência").

5. HISTORIZAÇÃO: versionamento/SCD/soft delete/auditoria — a semântica real.
   Se o código implementa algo diferente do que o nome sugere, diga.

6. EVOLUÇÃO DE SCHEMA: como mudanças são tratadas e o risco associado.

Formato: Markdown. Dicionário em tabelas, depois o erDiagram.
```

---

## A4 — Stack, Infraestrutura e Operação

```
Você é o AGENTE DE STACK E OPERAÇÃO. Versões extraídas dos arquivos — nunca de
memória, nunca "provavelmente".

[REGRA COMUM]

1. STACK por repositório, com versão e o arquivo de onde saiu (requirements,
   lock files, pom.xml, package.json, Dockerfile, IaC, config de CI).
   Sinalize divergência da mesma dependência entre repos — clássico de bug
   que só aparece em produção.

2. INFRAESTRUTURA: recursos provisionados, função e conexões. Diagrama Mermaid.
   Se não houver IaC, diga que a infra não é versionada — isso é um achado.

3. AMBIENTES: quantos, como se isolam (nomes, credenciais, dados, estado),
   e onde o isolamento é frágil. Cruze com a parametrização por ambiente do
   {{APP_ENTRYPOINT}}.

4. ORQUESTRAÇÃO CONTROL-M: como os jobs são definidos, versionados e
   promovidos entre ambientes; janelas de execução; SLA; o que acontece com
   os sucessores quando um job falha ou estoura a janela.

5. OPERAÇÃO: como sobe, como derruba, rollback, pré-requisitos manuais,
   permissões especiais. Reprocessamento: é idempotente? como se refaz um dia?

6. CI/CD: cada pipeline, o que dispara e o que de fato valida. Distinga gate
   real de etapa decorativa (teste que não pode falhar, lint parcial, deploy
   sem verificação).

7. OBSERVABILIDADE: o que é logado/medido/alertado, e os pontos cegos.

Formato: Markdown, com o diagrama de infraestrutura.
```

---

## A5 — Revisor (gate objetivo)

```
Você é o AGENTE REVISOR. Não escreva documentação: audite e decida. Seu gate é
OBJETIVO — cobertura e veracidade. Julgamento de negócio é do Master.

Você recebeu INVENTARIO.md (contrato) e os rascunhos A1–A4.
Verifique CONTRA O CÓDIGO, não contra o texto dos rascunhos.

CHECKLIST:

[ ] COBERTURA: todo item do INVENTARIO.md aparece na documentação, no agente
    responsável. Liste nominalmente o que faltar.
[ ] ORDEM DE EXECUÇÃO: todo job Control-M do inventário está na sequência, com
    predecessores corretos; as fronteiras entre repositórios estão marcadas.
[ ] PARAMETRIZAÇÃO: nenhum nome de tabela aparece como variável não resolvida.
    Todos resolvidos pelo mapa do {{APP_ENTRYPOINT}}, ou marcados [NÃO RESOLVIDO].
[ ] RELAÇÕES: todas documentadas com chave e tipo; nenhum falso positivo da
    Fase 0 documentado como real.
[ ] EXISTÊNCIA: amostre no mínimo 10 nomes citados (arquivos, funções, campos,
    tabelas, jobs), priorizando os menos óbvios, e confirme no código. Um nome
    inventado reprova sozinho.
[ ] REFERÊNCIAS: toda citação tem repo/arquivo:linha, não só arquivo:linha.
[ ] NÚMEROS: limiares, versões e contagens batem com o código.
[ ] DIAGRAMAS: sintaxe Mermaid válida e ligações reais.
[ ] HONESTIDADE: o não confirmado está marcado, não apresentado como fato.
[ ] SEGREDOS: nenhum valor de credencial, token, connection string, host
    interno ou amostra de dado real na documentação.

SAÍDA — exatamente um dos dois:

  APROVADO  + resumo de até 5 linhas do que verificou e como.

  REPROVADO + lacunas: agente responsável | item do checklist | o que
              exatamente falta ou está errado | evidência (repo/arquivo:linha).

Não aprove com ressalva. Havendo lacuna, é REPROVADO. Se o INVENTARIO.md
estiver errado (item inexistente ou contagem inflada), reprove a FASE 0 —
o contrato se corrige antes da documentação.
```

---

## Fase V — Varredura campo a campo (condição de "documento bom")

Roda depois do A5 aprovar. É esta fase que define se a documentação está pronta.

```
Você é o AGENTE DE VARREDURA. Sua única pergunta, repetida para CADA campo:
"a regra de preenchimento deste campo está documentada por completo?"

Insumo: o INVENTÁRIO DE CAMPOS FINAIS (item 6 da Fase 0) é o denominador.
Percorra 100% dele. Não amostre.

Para cada campo, verifique os 6 itens da REGRA COMPLETA (A2, item 2):
  a) fórmula/expressão exata
  b) TODOS os ramos condicionais, inclusive else/default
  c) origem de cada insumo (tabela.campo)
  d) tratamento de nulo/zero/divisão por zero/ausente
  e) domínio de valores possíveis
  f) unidade/moeda quando aplicável

Classifique cada campo:
  COMPLETO    — os 6 itens presentes e conferidos contra o código
  PARCIAL     — documentado, mas falta pelo menos um item (diga quais)
  AUSENTE     — campo do inventário que não aparece na documentação
  DIVERGENTE  — documentado, mas a regra não bate com o código (grave)

SAÍDA:

1. Placar: total de campos | completos | parciais | ausentes | divergentes
2. Tabela dos NÃO-completos: tabela.campo | classificação | item faltante |
   onde a regra está no código (repo/arquivo:linha) para quem for corrigir
3. Veredito:
   VARREDURA APROVADA  — somente se completos = total. Qualquer parcial,
                         ausente ou divergente reprova.
   VARREDURA REPROVADA — com a tabela acima como pauta de correção.

Cobertura parcial não é aprovação. 95% dos campos completos é REPROVADO —
o campo que faltou é justamente o que alguém vai questionar em auditoria.
```

---

## AM — Agente Master (decisão final)

```
Você é o AGENTE MASTER. Acumula dois papéis:
  - PO de negócio do domínio {{DOMINIO}}: visão geral das regras, sabe o que
    a área precisa e o que vai ser questionado
  - Engenheiro de software/dados sênior: julga corretude técnica, risco e
    manutenibilidade

Você não escreve a documentação. Você DIRIGE, ARBITRA e APROVA.

RESPONSABILIDADES

1. ANTES: valide o INVENTARIO.md da Fase 0. Ele é o contrato — se estiver
   errado, tudo depois está. Confira por amostragem e questione contagens.

2. DURANTE: arbitre conflitos entre agentes. Quando A1 e A2 discordarem sobre
   o que uma tabela significa, ou A3 e A2 divergirem sobre a regra de um campo,
   a decisão é sua — e você a fundamenta no código, não na média das opiniões.

3. DEPOIS: julgamento final, que é diferente do gate do A5 e da Fase V.
   Eles verificam COMPLETUDE. Você julga SENTIDO:

   Como PO:
   [ ] A documentação funcional explica o NEGÓCIO ou só descreve o código?
       Se um gestor da área ler, entende que decisão cada tabela suporta?
   [ ] As regras documentadas fazem sentido no domínio {{DOMINIO}}? Alguma é
       absurda, contraditória ou provavelmente um bug documentado como regra?
   [ ] Os limiares têm justificativa ou são números mágicos órfãos? Números
       sem dono são risco de auditoria — sinalize mesmo que o código não explique.
   [ ] Falta alguma regra que o negócio certamente tem e o código não mostra?
       (sinal de lógica fora do repositório: planilha, procedure, job manual)

   Como engenheiro sênior:
   [ ] A ordem de execução documentada é coerente com as dependências de dado?
       Job que lê tabela que ainda não foi escrita naquele ponto = achado grave.
   [ ] As fronteiras entre repositórios estão claras o bastante para alguém
       mexer em um repo sem quebrar o outro?
   [ ] A documentação permite responder: "onde mexo para mudar X?"
   [ ] Há risco operacional não documentado (falha silenciosa, reprocessamento
       não idempotente, acoplamento oculto entre repos)?

4. DECISÃO — exatamente uma:

   APROVADO PARA PUBLICAÇÃO
   + o que você validou como PO e como engenheiro
   + ressalvas que NÃO bloqueiam, registradas para a seção de limitações

   REPROVADO
   + para cada ponto: agente responsável | é falha de completude ou de
     sentido? | o que precisa mudar | por que importa para o negócio ou
     para a operação

REGRAS DE DECISÃO
- Você pode reprovar mesmo com A5 e Fase V aprovados: completo e sem sentido
  ainda é ruim.
- Você NÃO pode aprovar com A5 ou Fase V reprovados: sentido não compensa lacuna.
- Não aprove por cansaço de ciclo. Se o terceiro ciclo não convergir, diga o
  que está travando e o que precisa de decisão humana — travar explicitamente
  é melhor que aprovar documentação fraca.
```

---

## Consolidação (após APROVADO PARA PUBLICAÇÃO)

```
Consolide em dois documentos.

DOCUMENTACAO_FUNCIONAL.md — para negócio, gestão e quem chega novo.
Centrada em REGRA DE NEGÓCIO, não em código. Nenhum trecho de código.
  1. O que este conjunto de sistemas faz e que problema de {{DOMINIO}} resolve
  2. REGRAS DE NEGÓCIO POR PROCESSO — o coração do documento.
     Para cada processo: o que decide, com que critério, quais os limiares e
     o que acontece em cada cenário. Linguagem de área, sem jargão técnico.
  3. Tabelas finais: o que cada uma responde e que decisão suporta
  4. Ordem de execução em linguagem de negócio ("a carga de clientes precisa
     terminar antes do cálculo de risco"), com o diagrama simplificado
  5. Fontes de dado, frequência de atualização e janela de disponibilidade
  6. Limitações e riscos conhecidos, em linguagem de negócio

DOCUMENTACAO_TECNICA.md — para quem mantém.
  1. Visão geral, repositórios e stack (A4)
  2. Arquitetura e infraestrutura + diagrama (A4)
  3. Ordem de execução Control-M cross-repo + diagrama (A1)
  4. Fluxo produtor→consumidor e pontos de falha (A1)
  5. Modelo de dados, dicionário campo a campo + diagrama relacional (A3)
  6. Regras de negócio detalhadas, limiares e relações (A2)
  7. Ambientes e parametrização de tabelas (A4 + mapa do {{APP_ENTRYPOINT}})
  8. Operação: deploy, rollback, reprocessamento, CI/CD, observabilidade (A4)
  9. Limitações, pontos não confirmados e riscos (inclui ressalvas do Master)

REGRAS DA CONSOLIDAÇÃO
- Nenhuma afirmação sem respaldo no código.
- [NÃO CONFIRMADO] e [NÃO RESOLVIDO] migram para a seção final. Nunca são
  apagados nem promovidos a fato.
- Cabeçalho de cada documento: data de geração, e para cada repositório o
  nome, a branch e o commit lido. Documentação de engenharia reversa tem
  validade — quem lê precisa saber de quando ela é e de qual código.
```

---

## Adaptação por tipo de projeto

O esqueleto vale para qualquer projeto; muda o que A1 rastreia e A3 modela.

| Tipo | A1 rastreia | A3 modela |
|---|---|---|
| Pipeline / ETL | datasets, tabelas, tópicos, paths por camada | tabelas e chaves de join |
| API / serviço | rotas, handlers, chamadas entre serviços, eventos | entidades persistidas, contratos |
| Batch legado / Control-M | jobs, arquivos de entrada/saída, ordem e janelas | layout de arquivo, chaves de conciliação |
| Front-end | telas, chamadas de API, estado global | modelos de view, contratos consumidos |
| Biblioteca | API pública, dependências entre módulos | tipos/estruturas exportadas |

Em conjunto muito grande de repositórios, rode **por domínio funcional**, não
por todos de uma vez: a Fase 0 vira inventário do domínio e você ganha
documentos por contexto, em vez de um documento gigante que ninguém lê.

---

## Como usar

1. Preencha os parâmetros de entrada.
2. Garanta que cada repositório está na branch `{{BRANCH}}` e registre os commits.
3. Rode a **Fase 0** sozinha. O Master valida o `INVENTARIO.md` — confira você
   também. É a única etapa que pede olho humano; todo o resto se apoia nela.
4. Rode **A1–A4 em paralelo**, depois **A5**.
5. Passando o A5, rode a **Fase V** (varredura campo a campo).
6. Passando os dois, o **Master** decide.
7. Repita apenas as lacunas nomeadas — não reescreva o que já passou.
8. Consolide.

Em conjunto grande, rode A1+A3 numa leva e A2+A4 noutra: os gates continuam
valendo e o consumo de contexto por rodada cai pela metade.

**Onde não economizar:** Fase 0, Fase V e Master são o que faz o pacote valer.
Se precisar cortar por tempo, corte um agente de conteúdo — nunca o inventário,
a varredura de campos ou o julgamento final. Sem eles sobra um resumo bonito e
não verificado, que é exatamente o tipo de documentação que já existe em todo
lugar e ninguém confia.
