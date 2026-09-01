# Autorização de Deploy - Configuração Completa

## Visão Geral

Autorização para deploy pode ser configurada em múltiplos níveis:
1. GitHub (branch protection, required reviewers)
2. Databricks (RBAC, access control)
3. Azure (RBAC)
4. Airflow (RBAC)

---

## 1. GitHub - Autorização de Deploy

### Branch Protection

**Proteger branch `main`:**

1. Vá ao repositório no GitHub
2. Settings → Branches
3. Clique em `main`
4. Configure:
   - ✅ Require pull request before merging
   - ✅ Require approvals (1 ou mais)
   - ✅ Require status checks to pass before merging
   - ✅ Require branches to be up to date before merging
   - ❌ Do not allow bypassing the above settings

---

### Required Reviewers

**Definir revisores obrigatórios:**

1. Settings → Branches → `main`
2. Require pull request reviews
3. Require approval from:
   - Code owners
   - Specific users (ex: senior engineers)
   - Specific teams (GitHub Teams)

---

### GitHub Teams

**Criar equipe de aprovação:**

1. Vá ao GitHub Organization
2. Teams → New team
3. Nome: `data-engineering-approvals`
4. Adicionar membros
5. Em branch protection, adicionar equipe como required reviewer

---

### Branch Protection Rules

**Exemplo de configuração:**

```yaml
# .github/branch-protection.yml (usando app)
branch_protection:
  main:
    required_status_checks:
      - validate
      - build-hk
      - build-prod
    required_pull_request_reviews:
      required_approving_review_count: 1
      dismiss_stale_reviews: true
      require_code_owner_review: false
    restrictions:
      users: []
      teams: ["data-engineering-approvals"]
    enforce_admins: true
```

---

### Exemplo Prático

**Fluxo:**
1. Desenvolvedor cria PR para `main`
2. Membro da equipe `data-engineering-approvals` aprova
3. Status checks passam
4. PR pode ser mergeado
5. Deploy automático para PROD

---

## 2. Databricks - RBAC

### Workspace Access Control

**Configurar permissões no Databricks:**

1. Vá ao Databricks Workspace
2. Settings → Compute → Permissions
3. Adicionar usuários/grupos

**Níveis de permissão:**
- **Admin** - Acesso total
- **Can Manage** - Gerenciar workspace
- **Can Attach To** - Acessar clusters
- **Can Restart** - Reiniciar clusters
- **Can View** - Visualizar apenas

---

### Secret Scopes

**Configurar acesso ao Key Vault:**

1. Vá ao Databricks Workspace
2. Compute → Create Secret Scope
3. ACL:
   - READ: Quem pode ler secrets
   - WRITE: Quem pode escrever secrets
   - MANAGE: Quem pode gerenciar scope

**Exemplo:**
```python
# ACL do scope kv-case-santander
READ:  data-engineering-team
WRITE: data-engineering-lead
MANAGE: data-engineering-manager
```

---

### Job Access Control

**Configurar permissões por job:**

1. Vá ao Databricks Workspace
2. Workflows → Select Job → Permissions
3. Adicionar usuários/grupos

**Permissões:**
- **Can Run** - Pode executar job
- **Can Edit** - Pode editar job
- **Can Manage** - Pode gerenciar job
- **Can View** - Pode visualizar apenas

---

### Unity Catalog

**Configurar permissões no Unity Catalog:**

1. Vá ao Databricks Workspace
2. Data → Unity Catalog
3. Selecione catalog/schemas/tables
4. Grant:

**Exemplo:**
```sql
-- GRANT para grupo
GRANT SELECT ON TABLE case_santander.gold.fraude TO data_analysts;
GRANT ALL PRIVILEGES ON SCHEMA case_santander.gold TO data_engineers;
```

---

### Databricks Groups

**Criar grupos no Databricks:**

1. Vá ao Databricks Workspace
2. Admin Console → Identity and Access
3. Groups → Add Group

**Grupos recomendados:**
- `data-engineering-leads` - Líderes (pode deploy)
- `data-engineering-developers` - Desenvolvedores (pode editar)
- `data-analysts` - Analistas (apenas read)
- `data-science` - Data scientists (read + write)

---

## 3. Azure - RBAC

### Resource Groups

**Configurar RBAC no Azure:**

1. Vá ao Azure Portal
2. Resource Groups → `rg-case-santander-prod`
3. Access control (IAM)
4. Add role assignment

**Roles:**
- **Owner** - Acesso total
- **Contributor** - Pode modificar recursos
- **Reader** - Apenas leitura
- **Data Contributor** - Pode modificar dados
- **Storage Blob Data Contributor** - Pode modificar ADLS

---

### Key Vault Access

**Configurar acesso ao Key Vault:**

1. Vá ao Azure Portal
2. Key Vaults → `kv-case-santander`
3. Access policies
4. Add access policy

**Permissions:**
- **Get** - Ler secrets
- **List** - Listar secrets
- **Set** - Escrever secrets
- **Delete** - Deletar secrets
- **Manage** - Gerenciar Key Vault

**Exemplo:**
```yaml
# data-engineering-leads
Permissions: Get, List, Set, Delete, Manage

# data-engineering-developers
Permissions: Get, List

# data-engineering-managers
Permissions: Get, List, Set, Delete, Manage
```

---

### Databricks Workspace Access

**Configurar acesso ao Databricks Workspace:**

1. Vá ao Azure Portal
2. Databricks Workspace → Access control (IAM)
3. Add role assignment

**Roles:**
- **Contributor** - Pode gerenciar workspace
- **Databricks Contributor** - Pode usar workspace

---

### Azure AD Groups

**Criar grupos no Azure AD:**

1. Vá ao Azure Portal
2. Azure Active Directory → Groups
3. New group

**Grupos recomendados:**
- `Case-Santander-Data-Engineering-Leads` - Líderes (deploy)
- `Case-Santander-Data-Engineering-Developers` - Desenvolvedores
- `Case-Santander-Data-Analysts` - Analistas

---

## 4. Airflow - RBAC

### Configurar RBAC no Airflow

**Local (Docker):**

1. Vá ao Airflow UI: http://localhost:8080
2. Admin → Users
3. Criar usuários

**Permissões:**
- **Admin** - Acesso total
- **User** - Acesso limitado
- **Viewer** - Apenas visualização

---

### DAG-specific Permissions

**Configurar permissões por DAG:**

1. Vá ao Airflow UI
2. Admin → Permissions
3. Selecionar DAG
4. Grant permissions

**Permissões:**
- **Can Edit** - Pode editar DAG
- **Can Read** - Pode visualizar DAG
- **Can Trigger** - Pode executar DAG manualmente

---

### Airflow Production (CeleryExecutor)

**Configurar RBAC em produção:**

1. Vá ao Airflow UI de produção
2. Admin → Users
3. Criar usuários/grupos

**Integração com LDAP/SSO:**
- Configure Airflow para usar LDAP
- Configure Airflow para usar SSO (Azure AD, Okta, etc.)

---

## Estrutura de Autorização Recomendada

### Nível 1: GitHub (Entry Point)

**Quem pode mergear para `main`:**
- ✅ Membros da equipe `data-engineering-approvals`
- ✅ Requer 1 aprovação
- ✅ Status checks obrigatórios

**Quem pode mergear para `develop`:**
- ✅ Todos os desenvolvedores
- ✅ Sem aprovação necessária

---

### Nível 2: Databricks (Execution)

**Quem pode deploy jobs:**
- ✅ `data-engineering-leads` (MANAGE)
- ✅ `data-engineering-managers` (MANAGE)

**Quem pode editar jobs:**
- ✅ `data-engineering-leads` (CAN EDIT)
- ✅ `data-engineering-developers` (CAN EDIT)

**Quem pode executar jobs:**
- ✅ `data-engineering-leads` (CAN RUN)
- ✅ `data-engineering-developers` (CAN RUN)
- ✅ `data-analysts` (CAN RUN - apenas leitura)

---

### Nível 3: Azure (Infrastructure)

**Quem pode gerenciar infraestrutura:**
- ✅ `Case-Santander-Data-Engineering-Leads` (Contributor)
- ✅ `Case-Santander-Data-Engineering-Managers` (Owner)

**Quem pode acessar Key Vault:**
- ✅ `Case-Santander-Data-Engineering-Leads` (Get, List, Set, Delete, Manage)
- ✅ `Case-Santander-Data-Engineering-Developers` (Get, List)

---

### Nível 4: Airflow (Orchestration)

**Quem pode editar DAGs:**
- ✅ `data-engineering-leads` (CAN EDIT)
- ✅ `data-engineering-managers` (CAN EDIT)

**Quem pode executar DAGs manualmente:**
- ✅ `data-engineering-leads` (CAN TRIGGER)
- ✅ `data-engineering-developers` (CAN TRIGGER)

---

## Exemplo Prático

### Cenário: Deploy para PROD

**Usuário:** Desenvolvedor John

**Passo 1: Criar PR**
```bash
git checkout -b feature/nova-funcionalidade
git add .
git commit -m "feat: nova funcionalidade"
git push origin feature/nova-funcionalidade
```

**Passo 2: Criar PR no GitHub**
- John cria PR para `main`
- GitHub notifica equipe `data-engineering-approvals`

**Passo 3: Aprovação**
- Líder Mary (membro da equipe) aprova PR
- Status checks passam

**Passo 4: Merge**
- John mergea PR
- GitHub Actions executa CI/CD
- Deploy automático para PROD

**Passo 5: Execução**
- Databricks executa jobs (John tem permissão CAN RUN)
- Airflow monitora execução (John tem permissão CAN TRIGGER)

---

## Como Criar Grupos

### GitHub Teams

**Criar equipe:**
1. GitHub Organization → Teams
2. New team
3. Nome: `data-engineering-approvals`
4. Adicionar membros

**Usar em branch protection:**
- Settings → Branches → `main`
- Required reviewers → `data-engineering-approvals`

---

### Azure AD Groups

**Criar grupo:**
1. Azure Portal → Azure Active Directory → Groups
2. New group
3. Nome: `Case-Santander-Data-Engineering-Leads`
4. Adicionar membros

**Usar em RBAC:**
- Resource Group → Access control (IAM)
- Add role assignment → Selecione grupo

---

### Databricks Groups

**Criar grupo:**
1. Databricks Workspace → Admin Console
2. Identity and Access → Groups
3. Add Group
4. Nome: `data-engineering-leads`
5. Adicionar membros

**Usar em Workspace/Job permissions:**
- Workspace → Permissions → Adicionar grupo
- Job → Permissions → Adicionar grupo

---

## Resumo

### Quem pode Deploy?

**Nível 1 (GitHub):**
- ✅ Membros da equipe `data-engineering-approvals`

**Nível 2 (Databricks):**
- ✅ `data-engineering-leads` (MANAGE)
- ✅ `data-engineering-managers` (MANAGE)

**Nível 3 (Azure):**
- ✅ `Case-Santander-Data-Engineering-Leads` (Contributor)
- ✅ `Case-Santander-Data-Engineering-Managers` (Owner)

**Nível 4 (Airflow):**
- ✅ `data-engineering-leads` (CAN EDIT, CAN TRIGGER)
- ✅ `data-engineering-managers` (CAN EDIT, CAN TRIGGER)

---

### Como Criar Grupos?

**GitHub:**
- Organization → Teams → New team

**Azure:**
- Azure AD → Groups → New group

**Databricks:**
- Admin Console → Identity and Access → Groups → Add group

---

### Próximo Passo

1. **Criar equipes/grupos**
   - GitHub: `data-engineering-approvals`
   - Azure: `Case-Santander-Data-Engineering-Leads`
   - Databricks: `data-engineering-leads`

2. **Configurar branch protection**
   - Proteger branch `main`
   - Adicionar required reviewers

3. **Configurar RBAC**
   - Azure: Grant roles para grupos
   - Databricks: Grant permissions para grupos
   - Airflow: Grant permissions para usuários

**Conclusão:** Autorização em múltiplos níveis garante segurança e controle granular de deploy.
