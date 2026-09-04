"""
Producer: Event Hub Transações Financeiras
Gera transações financeiras simuladas e envia para Azure Event Hub.

Uso:
    python scripts/eventhub_producer.py

Dependências:
    pip install azure-eventhub
"""
import json
import random
import time
from datetime import datetime, timedelta
from azure.eventhub import EventHubProducerClient, EventData

# Seed fixa: torna a sequência de transações geradas (ticker, preço, tipo, corretora,
# intervalo de envio) reproduzível entre execuções. Não fixa o TOTAL de eventos — isso
# ainda depende de quanto tempo o script roda antes do Ctrl+C.
random.seed(42)

# Configurações
EVENT_HUB_CONNECTION_STR = "YOUR_EVENT_HUB_CONNECTION_STRING"
EVENT_HUB_NAME = "transacoes-financeiras"

# Tickers disponíveis
# Precisa bater com src/config/settings.py ACOES: e o universo que
# silver_acoes/performance_acoes conhecem. RENT3, B3SA3 e HAPV3 nao existem
# la -- os 4 joins left de src/gold/streaming_gold.py caem no "otherwise"
# para esses tickers, tornando 3 dos 10 ativos invisiveis a alerta de
# anomalia, desvio historico e volume no fluxo de streaming.
TICKERS = [
    "PETR4.SA", "VALE3.SA", "ITUB4.SA", "BBDC4.SA", "ABEV3.SA",
    "MGLU3.SA", "WEGE3.SA", "BBAS3.SA", "SANB11.SA"
]

# Corretoras
CORRETORAS = [
    "Santander", "Itau", "BTG", "XP", "Rico", "NuInvest"
]

# Tipos de transação
TIPOS = ["compra", "venda"]

# Pool fixo de atores sinteticos do streaming. Formato "SYN-nnnn" de proposito
# diferente de hash_cliente real (SHA256, 64 chars) -- este producer roda fora
# do Databricks, sem acesso a bronze.clientes, entao nao ha como atribuir um
# cliente REAL a cada transacao sem dar ao producer credencial Azure AD e um
# SQL Warehouse so pra essa amostra. O pool e FIXO (nao um valor novo por
# transacao) para que o mesmo ator reapareca varias vezes e dê pra perguntar
# "esse ator teve N alertas hoje" -- mas NUNCA correspondera a um cliente de
# bronze/silver.clientes: um join contra score_risco_clientes sempre dá NULL.
CLIENTES_SINTETICOS = [f"SYN-{i:04d}" for i in range(200)]


def gerar_transacao():
    """Gera uma transação financeira simulada"""
    ticker = random.choice(TICKERS)
    # Preço base por ticker (simulado)
    precos_base = {
        "PETR4.SA": 35.0, "VALE3.SA": 65.0, "ITUB4.SA": 32.0,
        "BBDC4.SA": 45.0, "ABEV3.SA": 12.5, "MGLU3.SA": 2.5,
        "WEGE3.SA": 180.0, "BBAS3.SA": 28.0, "SANB11.SA": 30.0
    }
    
    preco_base = precos_base.get(ticker, 50.0)
    # Variação de +/- 5%
    preco = round(preco_base * random.uniform(0.95, 1.05), 2)
    
    quantidade = random.randint(100, 10000)
    tipo = random.choice(TIPOS)
    corretora = random.choice(CORRETORAS)
    hash_cliente = random.choice(CLIENTES_SINTETICOS)

    transacao = {
        "timestamp": datetime.now().isoformat(),
        "ticker": ticker,
        "preco": preco,
        "quantidade": quantidade,
        "tipo": tipo,
        "corretora": corretora,
        "hash_cliente": hash_cliente,
        "id_transacao": f"{ticker}-{int(time.time() * 1000)}-{random.randint(1000, 9999)}"
    }
    
    return transacao


def enviar_para_eventhub(transacao, producer):
    """Envia transação para o Event Hub"""
    event_data = EventData(json.dumps(transacao))
    producer.send_batch([event_data])


def main():
    """Função principal"""
    print("=== Event Hub Producer Iniciado ===")
    print(f"Event Hub: {EVENT_HUB_NAME}")
    print("Gerando transações financeiras simuladas...")
    print("Pressione Ctrl+C para parar")
    
    # Criar producer
    producer = EventHubProducerClient.from_connection_string(
        EVENT_HUB_CONNECTION_STR,
        EVENT_HUB_NAME
    )
    
    try:
        contador = 0
        while True:
            # Gerar transação
            transacao = gerar_transacao()
            
            # Enviar para Event Hub
            enviar_para_eventhub(transacao, producer)
            
            contador += 1
            print(f"[{contador}] Enviado: {transacao['ticker']} - {transacao['tipo']} - R$ {transacao['valor_total'] if 'valor_total' in transacao else transacao['preco'] * transacao['quantidade']:.2f}")
            
            # Aguardar entre transações (1-5 segundos)
            time.sleep(random.uniform(1, 5))
            
    except KeyboardInterrupt:
        print("\n=== Producer encerrado pelo usuário ===")
    except Exception as e:
        print(f"Erro: {e}")
    finally:
        producer.close()
        print(f"Total de transações enviadas: {contador}")


if __name__ == "__main__":
    main()
