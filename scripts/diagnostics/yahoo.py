"""
Teste simples para Yahoo Finance
"""

import yfinance as yf

def test_yahoo():
    print("Testando Yahoo Finance...")
    try:
        ticker = yf.Ticker("PETR4.SA")
        print("Obtendo historico de 5 dias...")
        df = ticker.history(period="5d")
        print(f"Registros obtidos: {len(df)}")
        if len(df) > 0:
            print(f"Ultimo preco: {df['Close'].iloc[-1]:.2f}")
            print("[OK] Yahoo Finance funcionando")
            return True
        else:
            print("[ERRO] Sem dados")
            return False
    except Exception as e:
        print(f"[ERRO] {str(e)}")
        return False

if __name__ == "__main__":
    test_yahoo()
