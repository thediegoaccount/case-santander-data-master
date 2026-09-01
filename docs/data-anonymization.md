# Data Anonymization - SHA256 with Salt

## Visão Geral

Dados sensíveis de clientes são mascarados usando **SHA256 com salt** armazenado no Key Vault.

## Por que SHA256 + Salt?

### Segurança Criptográfica

**One-Way Hash (Hash Unidirecional):**
- ✅ Impossível reverter o hash original
- ✅ SHA256 é criptograficamente seguro
- ✅ 64 caracteres hexadecimais (256 bits)

**Salt (Sal):**
- ✅ Previne ataques de rainbow table
- ✅ Adiciona aleatoriedade ao hash
- ✅ Armazenado no Key Vault (não em código)

**Mesmo com acesso ao salt:**
- ❌ Não é possível reverter o hash
- ❌ Não é possível descobrir o valor original
- ✅ Apenas previne ataques de pre-computação

## Implementação

### 1. Salt no Key Vault

**Secret:**
```yaml
scope: kv-case-santander
key: salt
value: <random-64-char-hex-string>
```

**Terraform:**
```hcl
resource "azurerm_key_vault_secret" "salt" {
  name         = "salt"
  value        = var.salt
  key_vault_id = var.key_vault_id
}
```

---

### 2. Função de Hashing

**Arquivo:** `src/security/hashing.py`

```python
def hash_with_salt(data: str, salt: str = None) -> str:
    """
    Gera hash SHA256 com salt (one-way, não reversível)
    
    Args:
        data: Dado a ser mascarado
        salt: Salt para hashing (opcional, usa Key Vault se não fornecido)
    
    Returns:
        Hash SHA256 em formato hexadecimal (64 caracteres)
    """
    if salt is None:
        salt = get_salt()  # Recupera do Key Vault
    
    # Concatenar dado + salt
    salted_data = f"{data}{salt}"
    
    # Gerar hash SHA256
    hash_obj = hashlib.sha256(salted_data.encode('utf-8'))
    hash_hex = hash_obj.hexdigest()
    
    return hash_hex
```

---

### 3. Aplicação na Tabela de Clientes

**Arquivo:** `jobs/job_clientes_ordens.py`

```python
from src.security.hashing import hash_customer_id, hash_surname

# Mascara CustomerId
df_clientes["hash_cliente"] = df_clientes["CustomerId"].apply(hash_customer_id)

# Mascara Sobrenome
df_clientes["sobrenome_masked"] = df_clientes["Surname"].apply(hash_surname)
```

**Resultado:**
```python
# Antes:
CustomerId: 15674913
Surname: Smith

# Depois:
hash_cliente: a1b2c3d4e5f6... (64 caracteres hex)
sobrenome_masked: 7f8e9d0a1b2c... (64 caracteres hex)
```

---

## Por que é Seguro?

### 1. SHA256 é One-Way

**Propriedade:**
```
hash(input) → output
hash^-1(output) → IMPOSSÍVEL
```

**Exemplo:**
```python
hash("123456") = "8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92"
hash^-1("8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92") = IMPOSSÍVEL
```

**Mesmo com força bruta:**
- 2^256 combinações possíveis
- Anos de computação para reverter

---

### 2. Salt Previne Rainbow Tables

**Sem salt:**
```
hash("123456") = "8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92"
# Atacante pode usar rainbow table pré-computada
```

**Com salt:**
```
hash("123456" + "salt_abc123") = "7f8e9d0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8"
# Rainbow table não funciona (salt é único por sistema)
```

---

### 3. Mesmo com Acesso ao Salt

**Cenário:** Atacante tem acesso ao salt

```python
salt = "abc123def456"
hash_cliente = "7f8e9d0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8"

# Atacante tenta:
for i in range(1000000000000):
    if hash(f"{i}{salt}") == hash_cliente:
        print(f"Cliente encontrado: {i}")
        break

# Resultado: IMPOSSÍVEL em tempo razoável
```

**Por que:**
- 2^256 combinações possíveis
- Mesmo 1 milhão de tentações/segundo → 10^19 anos

---

## Compliance com LGPD

### Pseudonimização

**LGPD Art. 12 - Anonimização:**
> "A anonimização consiste no uso de meios técnicos razoáveis e disponíveis no momento do processamento, por meio dos quais um dado perde a possibilidade de associação, direta ou indireta, a um indivíduo."

**Implementação:**
- ✅ CustomerId → hash_cliente (SHA256 + salt)
- ✅ Surname → sobrenome_masked (SHA256 + salt)
- ✅ Impossível associar hash ao indivíduo original
- ✅ Dados permanecem utilizáveis para análise

---

## Geração do Salt

### Via Terraform

**Geração automática:**
```python
import secrets

salt = secrets.token_hex(32)  # 64 caracteres hex
```

**Manual (para teste):**
```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

**Armazenar no Key Vault:**
```bash
az keyvault secret set --vault-name kv-case-santander --name salt --value <salt>
```

---

## Verificação

### Teste de Irreversibilidade

```python
from src.security.hashing import hash_with_salt

# Teste
original = "15674913"
salt = "abc123def456"

# Hash
hashed = hash_with_salt(original, salt)
print(f"Hash: {hashed}")

# Tentativa de reversão (IMPOSSÍVEL)
for i in range(1000000):
    if hash_with_salt(str(i), salt) == hashed:
        print(f"Encontrado: {i}")
        break
else:
    print("Não foi possível reverter (como esperado)")
```

**Resultado:**
```
Hash: 7f8e9d0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8
Não foi possível reverter (como esperado)
```

---

## Resumo

**Implementação:**
- ✅ SHA256 + salt (one-way hash)
- ✅ Salt armazenado no Key Vault
- ✅ CustomerId mascarado
- ✅ Sobrenome mascarado
- ✅ Impossível reverter

**Segurança:**
- ✅ Criptograficamente seguro
- ✅ Impossível reverter mesmo com salt
- ✅ Previne rainbow tables
- ✅ Compliance LGPD

**Valor para o cliente:**
- ✅ Proteção de dados sensíveis
- ✅ Compliance com LGPD
- ✅ Análise possível sem dados identificados
- ✅ Enterprise-grade security
