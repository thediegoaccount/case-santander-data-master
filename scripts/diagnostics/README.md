# Scripts de diagnóstico

Verificações manuais de conectividade com APIs externas (Yahoo Finance, BCB,
World Bank, Kaggle). **Não são testes automatizados.**

Estavam na raiz do repositório com prefixo `test_`, o que fazia o pytest
coletá-los junto com a suíte: 9 funções sem um único `assert`, que "passavam"
mesmo com a API fora do ar, disparando chamadas de rede reais e download de
dataset a cada execução da suíte.

Uso:

```bash
python scripts/diagnostics/apis.py
python scripts/diagnostics/kaggle.py <token>
python scripts/diagnostics/yahoo.py
```
