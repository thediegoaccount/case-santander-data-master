"""
Script de teste para validar conexão com a API do Kaggle.
Testa download do dataset: churn-for-bank-customers
"""

import os
import zipfile
import requests

def test_kaggle_api(token):
    """Testa conexão com API do Kaggle usando novo formato de token"""
    print("\n" + "="*60)
    print("TESTE: Kaggle API")
    print("="*60)
    
    if not token:
        print("[ERRO] Token do Kaggle e obrigatorio")
        print("Obtenha seu token em: https://www.kaggle.com/settings/api")
        return False
    
    # Criar diretorio temporario
    pid = os.getpid()
    work_dir = f"kaggle_test_{pid}"
    zip_path = f"{work_dir}/churn.zip"
    
    try:
        os.makedirs(work_dir, exist_ok=True)
        print(f"Diretorio de trabalho: {work_dir}")
        
        # URL do dataset
        url = "https://www.kaggle.com/api/v1/datasets/download/mathchi/churn-for-bank-customers"
        print(f"Conectando a: {url}")
        
        # Download com autenticacao usando novo formato de token
        print("Autenticando e baixando dataset...")
        headers = {
            "Authorization": f"Bearer {token}"
        }
        response = requests.get(url, headers=headers, stream=True, timeout=30)
        
        print(f"Status HTTP: {response.status_code}")
        
        if response.status_code == 200:
            # Salvar arquivo ZIP
            with open(zip_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            print(f"Arquivo baixado: {zip_path}")
            print(f"Tamanho: {os.path.getsize(zip_path)} bytes")
            
            # Extrair ZIP
            print("Extraindo arquivo ZIP...")
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(work_dir)
                print(f"Arquivos extraidos: {z.namelist()}")
            
            # Verificar CSV
            csv_path = f"{work_dir}/churn.csv"
            if os.path.exists(csv_path):
                import pandas as pd
                df = pd.read_csv(csv_path)
                print(f"[OK] Dataset carregado com sucesso!")
                print(f"Registros: {len(df)}")
                print(f"Colunas: {len(df.columns)}")
                print(f"Primeiras colunas: {df.columns[:5].tolist()}")
                return True
            else:
                print(f"[ERRO] CSV nao encontrado: {csv_path}")
                return False
                
        elif response.status_code == 401:
            print("[ERRO] Autenticacao falhou - Token invalido")
            return False
        elif response.status_code == 403:
            print("[ERRO] Acesso negado - Verifique se aceitou os termos do dataset")
            return False
        elif response.status_code == 404:
            print("[ERRO] Dataset nao encontrado")
            return False
        else:
            print(f"[ERRO] HTTP {response.status_code}")
            return False
            
    except requests.exceptions.Timeout:
        print("[ERRO] Timeout na conexao")
        return False
    except Exception as e:
        print(f"[ERRO] {str(e)}")
        return False
    finally:
        # Limpar arquivos temporarios
        if os.path.exists(work_dir):
            import shutil
            try:
                shutil.rmtree(work_dir)
                print(f"\nArquivos temporarios removidos: {work_dir}")
            except:
                pass

def main():
    import sys
    
    print("\n" + "="*60)
    print("TESTE DE CONEXAO COM KAGGLE API")
    print("="*60)
    
    # Aceitar token via argumento ou variavel de ambiente
    token = None
    
    if len(sys.argv) >= 2:
        token = sys.argv[1]
    else:
        # Tentar variavel de ambiente
        token = os.getenv("KAGGLE_API_TOKEN")
    
    if not token:
        print("\n[ERRO] Token nao fornecido")
        print("Uso: python test_kaggle.py <KAGGLE_API_TOKEN>")
        print("Ou defina variavel de ambiente: KAGGLE_API_TOKEN")
        print("Obtenha seu token em: https://www.kaggle.com/settings/api")
        return
    
    resultado = test_kaggle_api(token)
    
    print("\n" + "="*60)
    print("RESUMO")
    print("="*60)
    if resultado:
        print("[OK] Kaggle API OPERACIONAL")
    else:
        print("[ERRO] Kaggle API FALHOU")

if __name__ == "__main__":
    main()
