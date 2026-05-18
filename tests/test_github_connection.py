"""
Diagnóstico de conexão Databricks Repos ↔ GitHub.

Execute em um notebook Databricks com:
    %run ./tests/test_github_connection
ou cole o conteúdo diretamente em uma célula Python.
"""

import json
import urllib.request
import urllib.error


def _get_host_and_token():
    """Tenta obter host/token via dbutils (ambiente Databricks)."""
    try:
        # dbutils está disponível apenas dentro do Databricks
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
        host = ctx.apiUrl().get()
        token = ctx.apiToken().get()
        return host, token
    except Exception:
        return None, None


def _api_get(host, token, path):
    req = urllib.request.Request(
        f"{host}{path}",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def check_git_credentials(host, token):
    """Verifica se há credenciais Git configuradas no workspace."""
    print("\n=== [1] Credenciais Git no Workspace ===")
    try:
        data = _api_get(host, token, "/api/2.0/git-credentials")
        creds = data.get("credentials", [])
        if not creds:
            print("  AVISO: Nenhuma credencial Git configurada.")
            print("  -> User Settings > Git Integration > Add credential")
        else:
            for c in creds:
                print(f"  OK  Provider : {c.get('git_provider')}")
                print(f"      Username : {c.get('git_username')}")
    except urllib.error.HTTPError as e:
        print(f"  ERRO HTTP {e.code}: {e.reason}")


def check_repos(host, token, github_repo_url=None):
    """Lista repositórios clonados no workspace e verifica o repo do projeto."""
    print("\n=== [2] Repos no Workspace ===")
    try:
        data = _api_get(host, token, "/api/2.0/repos")
        repos = data.get("repos", [])
        if not repos:
            print("  AVISO: Nenhum repo encontrado no workspace.")
            print("  -> Repos > Add Repo > colar URL do GitHub")
            return

        for r in repos:
            url = r.get("url", "")
            branch = r.get("branch", "?")
            path = r.get("path", "?")
            match = " <-- ESTE PROJETO" if github_repo_url and github_repo_url in url else ""
            print(f"  - {url}  [branch: {branch}]  path: {path}{match}")

        if github_repo_url:
            found = any(github_repo_url in r.get("url", "") for r in repos)
            if not found:
                print(f"\n  AVISO: Repo '{github_repo_url}' NÃO encontrado.")
                print("  -> Repos > Add Repo > colar a URL acima")
    except urllib.error.HTTPError as e:
        print(f"  ERRO HTTP {e.code}: {e.reason}")


def check_repo_sync(host, token, repo_id):
    """Verifica o status de sincronização de um repo específico."""
    print(f"\n=== [3] Status do Repo ID {repo_id} ===")
    try:
        data = _api_get(host, token, f"/api/2.0/repos/{repo_id}")
        print(f"  Branch : {data.get('branch')}")
        print(f"  URL    : {data.get('url')}")
        print(f"  Path   : {data.get('path')}")
        head = data.get("head_commit_id", "N/A")
        print(f"  HEAD   : {head}")
    except urllib.error.HTTPError as e:
        print(f"  ERRO HTTP {e.code}: {e.reason}")


def run_diagnostics():
    GITHUB_REPO = "https://github.com/thediegoaccount/case-santander-data-master"

    host, token = _get_host_and_token()

    if not host or not token:
        print("Execute este script dentro de um notebook Databricks.")
        print("O script obtém host e token automaticamente via dbutils.")
        return

    print(f"Workspace : {host}")
    print(f"Token     : {token[:8]}...")

    check_git_credentials(host, token)
    check_repos(host, token, github_repo_url=GITHUB_REPO)


if __name__ == "__main__":
    run_diagnostics()
