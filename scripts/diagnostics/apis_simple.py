"""
Script de teste simples para verificar conexão com as APIs externas.
"""

import time
from datetime import datetime
import requests
import yfinance as yf

def test_yahoo_finance():
    """Testa conexão com Yahoo Finance"""
    print("\n" + "="*60)
    print("TESTE 1: Yahoo Finance API")
    print("="*60)
    
    try:
        ticker = yf.Ticker("PETR4.SA")
        df = ticker.history(period="5d")
        if len(df) > 0:
            print(f"[OK] PETR4.SA: OK - {len(df)} registros")
            print(f"   Ultimo preco: {df['Close'].iloc[-1]:.2f}")
            return True
        else:
            print(f"[ERRO] PETR4.SA: Sem dados")
            return False
    except Exception as e:
        print(f"[ERRO] PETR4.SA: ERRO - {str(e)}")
        return False

def test_bcb_api():
    """Testa conexão com API do Banco Central"""
    print("\n" + "="*60)
    print("TESTE 2: BCB API (SGS)")
    print("="*60)
    
    data_inicial = "01/01/2024"
    data_final = datetime.now().strftime("%d/%m/%Y")
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial={data_inicial}&dataFinal={data_final}"
    
    try:
        response = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        
        if response.status_code == 200:
            content_type = response.headers.get("Content-Type", "")
            if "json" in content_type.lower():
                json_data = response.json()
                if isinstance(json_data, list) and len(json_data) > 0:
                    print(f"[OK] Selic: OK - {len(json_data)} registros")
                    print(f"   Ultimo valor: {json_data[-1]['valor']}")
                    return True
                else:
                    print(f"[ERRO] Selic: Dados vazios")
                    return False
            else:
                print(f"[ERRO] Selic: Content-Type invalido - {content_type}")
                return False
        else:
            print(f"[ERRO] Selic: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        print(f"[ERRO] Selic: ERRO - {str(e)}")
        return False

def test_world_bank_api():
    """Testa conexão com World Bank API"""
    print("\n" + "="*60)
    print("TESTE 3: World Bank API")
    print("="*60)
    
    url = "https://api.worldbank.org/v2/country/BR/indicator/NY.GDP.MKTP.KD.ZG?format=json&per_page=5"
    
    try:
        response = requests.get(url, timeout=15)
        data = response.json()
        
        if data and len(data) >= 2 and data[1]:
            registros = data[1]
            registros_validos = [r for r in registros if r["value"] is not None]
            if registros_validos:
                print(f"[OK] PIB: OK - {len(registros_validos)} registros validos")
                print(f"   Ultimo valor ({registros_validos[0]['date']}): {registros_validos[0]['value']}")
                return True
            else:
                print(f"[ERRO] PIB: Sem valores validos")
                return False
        else:
            print(f"[ERRO] PIB: Dados vazios ou formato invalido")
            return False
            
    except Exception as e:
        print(f"[ERRO] PIB: ERRO - {str(e)}")
        return False

def main():
    print("\n" + "="*60)
    print("TESTE DE CONEXÃO COM APIs EXTERNAS")
    print("="*60)
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    
    resultados = {
        "Yahoo Finance": test_yahoo_finance(),
        "BCB API": test_bcb_api(),
        "World Bank API": test_world_bank_api()
    }
    
    print("\n" + "="*60)
    print("RESUMO DOS TESTES")
    print("="*60)
    for api, status in resultados.items():
        status_str = "[OK] OPERACIONAL" if status else "[ERRO] FALHOU"
        print(f"{api}: {status_str}")
    
    total_ok = sum(resultados.values())
    print(f"\nTotal: {total_ok}/3 APIs operacionais")

if __name__ == "__main__":
    main()
