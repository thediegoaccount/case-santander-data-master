"""
Script de teste para APIs REST apenas (BCB e World Bank)
"""

from datetime import datetime
import requests

def test_bcb_api():
    """Testa conexão com API do Banco Central"""
    print("\n" + "="*60)
    print("TESTE 1: BCB API (SGS)")
    print("="*60)
    
    data_inicial = "01/01/2024"
    data_final = datetime.now().strftime("%d/%m/%Y")
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial={data_inicial}&dataFinal={data_final}"
    
    try:
        print(f"Conectando a: {url[:80]}...")
        response = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        print(f"Status HTTP: {response.status_code}")
        
        if response.status_code == 200:
            content_type = response.headers.get("Content-Type", "")
            print(f"Content-Type: {content_type}")
            
            if "json" in content_type.lower():
                json_data = response.json()
                if isinstance(json_data, list) and len(json_data) > 0:
                    print(f"[OK] Selic: {len(json_data)} registros obtidos")
                    print(f"Ultimo valor: {json_data[-1]['valor']}")
                    return True
                else:
                    print(f"[ERRO] Dados vazios")
                    return False
            else:
                print(f"[ERRO] Content-Type invalido")
                return False
        else:
            print(f"[ERRO] HTTP {response.status_code}")
            return False
            
    except requests.exceptions.Timeout:
        print(f"[ERRO] Timeout apos 10 segundos")
        return False
    except Exception as e:
        print(f"[ERRO] {str(e)}")
        return False

def test_world_bank_api():
    """Testa conexão com World Bank API"""
    print("\n" + "="*60)
    print("TESTE 2: World Bank API")
    print("="*60)
    
    url = "https://api.worldbank.org/v2/country/BR/indicator/NY.GDP.MKTP.KD.ZG?format=json&per_page=5"
    
    try:
        print(f"Conectando a: {url}")
        response = requests.get(url, timeout=10)
        print(f"Status HTTP: {response.status_code}")
        
        data = response.json()
        
        if data and len(data) >= 2 and data[1]:
            registros = data[1]
            registros_validos = [r for r in registros if r["value"] is not None]
            if registros_validos:
                print(f"[OK] PIB: {len(registros_validos)} registros validos")
                print(f"Ultimo valor ({registros_validos[0]['date']}): {registros_validos[0]['value']}")
                return True
            else:
                print(f"[ERRO] Sem valores validos")
                return False
        else:
            print(f"[ERRO] Dados vazios ou formato invalido")
            return False
            
    except requests.exceptions.Timeout:
        print(f"[ERRO] Timeout apos 10 segundos")
        return False
    except Exception as e:
        print(f"[ERRO] {str(e)}")
        return False

def main():
    print("\n" + "="*60)
    print("TESTE DE CONEXAO COM APIs REST")
    print("="*60)
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    
    resultados = {
        "BCB API": test_bcb_api(),
        "World Bank API": test_world_bank_api()
    }
    
    print("\n" + "="*60)
    print("RESUMO")
    print("="*60)
    for api, status in resultados.items():
        status_str = "[OK] OPERACIONAL" if status else "[ERRO] FALHOU"
        print(f"{api}: {status_str}")
    
    total_ok = sum(resultados.values())
    print(f"\nTotal: {total_ok}/2 APIs operacionais")

if __name__ == "__main__":
    main()
