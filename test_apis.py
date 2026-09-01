"""
Script de teste para verificar conexão com as APIs externas do projeto.
Testa: Yahoo Finance, BCB API, World Bank API
"""

import time
from datetime import datetime
import requests
import yfinance as yf
import pandas as pd

def test_yahoo_finance():
    """Testa conexão com Yahoo Finance"""
    print("\n" + "="*60)
    print("TESTE 1: Yahoo Finance API")
    print("="*60)
    
    acoes_teste = ["PETR4.SA", "VALE3.SA"]
    
    for acao in acoes_teste:
        try:
            ticker = yf.Ticker(acao)
            df = ticker.history(period="1mo")
            if len(df) > 0:
                print(f" {acao}: OK - {len(df)} registros obtidos")
                print(f"   Último preço: {df['Close'].iloc[-1]:.2f}")
            else:
                print(f" {acao}: Sem dados retornados")
        except Exception as e:
            print(f" {acao}: ERRO - {str(e)}")

def test_bcb_api():
    """Testa conexão com API do Banco Central"""
    print("\n" + "="*60)
    print("TESTE 2: BCB API (SGS)")
    print("="*60)
    
    data_inicial = "01/01/2024"
    data_final = datetime.now().strftime("%d/%m/%Y")
    
    series = [
        (11, "Selic"),
        (1, "Câmbio USD/BRL"),
        (433, "IPCA")
    ]
    
    for codigo, nome in series:
        url = (
            f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
            f"?formato=json&dataInicial={data_inicial}&dataFinal={data_final}"
        )
        
        try:
            response = requests.get(
                url,
                timeout=30,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            )
            
            if response.status_code == 200:
                content_type = response.headers.get("Content-Type", "")
                if "json" in content_type.lower():
                    json_data = response.json()
                    if isinstance(json_data, list) and len(json_data) > 0:
                        print(f" {nome}: OK - {len(json_data)} registros")
                        print(f"   Último valor: {json_data[-1]['valor']}")
                    else:
                        print(f" {nome}: Dados vazios")
                else:
                    print(f" {nome}: Content-Type inválido - {content_type}")
            else:
                print(f" {nome}: HTTP {response.status_code}")
                
        except Exception as e:
            print(f" {nome}: ERRO - {str(e)}")
        
        time.sleep(1)  # Evitar rate limit

def test_world_bank_api():
    """Testa conexão com World Bank API"""
    print("\n" + "="*60)
    print("TESTE 3: World Bank API")
    print("="*60)
    
    indicadores = [
        ("NY.GDP.MKTP.KD.ZG", "PIB crescimento anual"),
        ("SL.UEM.TOTL.ZS", "Taxa de desemprego")
    ]
    
    for indicador, nome in indicadores:
        url = f"https://api.worldbank.org/v2/country/BR/indicator/{indicador}?format=json&per_page=10"
        
        try:
            response = requests.get(url, timeout=10)
            data = response.json()
            
            if data and len(data) >= 2 and data[1]:
                registros = data[1]
                registros_validos = [r for r in registros if r["value"] is not None]
                if registros_validos:
                    print(f" {nome}: OK - {len(registros_validos)} registros válidos")
                    print(f"   Último valor ({registros_validos[0]['date']}): {registros_validos[0]['value']}")
                else:
                    print(f" {nome}: Sem valores válidos")
            else:
                print(f" {nome}: Dados vazios ou formato inválido")
                
        except Exception as e:
            print(f" {nome}: ERRO - {str(e)}")

def main():
    print("\n" + "="*60)
    print("TESTE DE CONEXÃO COM APIs EXTERNAS")
    print("="*60)
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    
    try:
        test_yahoo_finance()
        test_bcb_api()
        test_world_bank_api()
        
        print("\n" + "="*60)
        print("TESTES CONCLUÍDOS")
        print("="*60)
        
    except KeyboardInterrupt:
        print("\n\n Teste interrompido pelo usuário")
    except Exception as e:
        print(f"\n\n Erro geral durante os testes: {str(e)}")

if __name__ == "__main__":
    main()
