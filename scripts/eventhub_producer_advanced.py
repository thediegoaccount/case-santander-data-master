"""
Producer: Event Hub Transações Financeiras (Avançado)
Gera transações financeiras simuladas e envia para Azure Event Hub.

Uso:
    # Modo normal (1 transação por segundo)
    python scripts/eventhub_producer_advanced.py
    
    # Modo volume (100 transações rapidamente)
    python scripts/eventhub_producer_advanced.py --volume 100
    
    # Modo burst (envia N transações e para)
    python scripts/eventhub_producer_advanced.py --burst 50

Configuração:
    Exporte variáveis de ambiente:
    export EVENTHUB_CONNECTION_STRING="Endpoint=sb://..."
    export EVENTHUB_NAME="transacoes-financeiras"

    Opcional -- clientes REAIS por transação (senão usa pool sintético
    "SYN-nnnn", sem vinculo com bronze.clientes; ver scripts/_streaming_clientes.py):
    export AZURE_CLIENT_ID="..."
    export AZURE_CLIENT_SECRET="..."
    export AZURE_TENANT_ID="..."
    export STORAGE_ACCOUNT="..."

Dependências:
    pip install azure-eventhub python-dotenv
    pip install azure-identity azure-storage-file-datalake   # so para clientes reais
"""
import argparse
import json
import os
import random
import time
from datetime import datetime, timedelta
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv

from _streaming_clientes import carregar_clientes

# Carregar variáveis de ambiente
load_dotenv()

# Seed fixa: em --volume/--burst (contagem fixa), torna a sequência inteira de transações
# reproduzível — mesmo conteúdo, execução após execução. No modo contínuo, fixa apenas a
# sequência gerada; o total ainda depende de quanto tempo o script roda.
random.seed(42)

# Configurações (variáveis de ambiente)
EVENT_HUB_CONNECTION_STR = os.getenv("EVENTHUB_CONNECTION_STRING")
EVENT_HUB_NAME = os.getenv("EVENTHUB_NAME", "transacoes-financeiras")

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

# Amostra de hash_cliente para atribuir a cada transacao. Real (de
# bronze.clientes, exportada por job_exportar_amostra_streaming) se
# AZURE_CLIENT_ID/AZURE_CLIENT_SECRET/AZURE_TENANT_ID/STORAGE_ACCOUNT
# estiverem no .env; senao cai num pool sintetico "SYN-nnnn" -- ver
# scripts/_streaming_clientes.py.
CLIENTES, FONTE_CLIENTES = carregar_clientes()


def gerar_transacao(transacao_id=None):
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
    hash_cliente = random.choice(CLIENTES)

    if transacao_id is None:
        transacao_id = f"{ticker}-{int(time.time() * 1000)}-{random.randint(1000, 9999)}"

    transacao = {
        "timestamp": datetime.now().isoformat(),
        "ticker": ticker,
        "preco": preco,
        "quantidade": quantidade,
        "tipo": tipo,
        "corretora": corretora,
        "hash_cliente": hash_cliente,
        "id_transacao": transacao_id
    }
    
    return transacao


def validar_schema(transacao):
    """Valida se a transação tem o schema correto"""
    campos_obrigatorios = [
        "timestamp", "ticker", "preco", "quantidade",
        "tipo", "corretora", "hash_cliente", "id_transacao"
    ]
    
    for campo in campos_obrigatorios:
        if campo not in transacao:
            return False, f"Campo obrigatório faltando: {campo}"
    
    # Validar tipos
    if not isinstance(transacao["preco"], (int, float)):
        return False, "Preço deve ser numérico"
    
    if not isinstance(transacao["quantidade"], int):
        return False, "Quantidade deve ser inteiro"
    
    if transacao["tipo"] not in TIPOS:
        return False, f"Tipo inválido: {transacao['tipo']}"
    
    return True, "Schema válido"


def enviar_para_eventhub(transacao, producer):
    """Envia transação para o Event Hub"""
    event_data = EventData(json.dumps(transacao))
    producer.send_batch([event_data])


def main():
    """Função principal"""
    parser = argparse.ArgumentParser(description="Event Hub Producer - Transações Financeiras")
    parser.add_argument(
        "--volume",
        type=int,
        default=0,
        help="Modo volume: envia N transações rapidamente"
    )
    parser.add_argument(
        "--burst",
        type=int,
        default=0,
        help="Modo burst: envia N transações e para"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Intervalo entre transações em segundos (padrão: 1.0)"
    )
    
    args = parser.parse_args()
    
    # Validar configuração
    if not EVENT_HUB_CONNECTION_STR:
        print("ERRO: EVENTHUB_CONNECTION_STRING não definido")
        print("Exporte a variável de ambiente:")
        print("export EVENTHUB_CONNECTION_STRING='Endpoint=sb://...'")
        return
    
    print("=== Event Hub Producer Iniciado ===")
    print(f"Event Hub: {EVENT_HUB_NAME}")
    print(f"Intervalo: {args.interval}s")
    
    if args.volume > 0:
        print(f"Modo VOLUME: {args.volume} transações")
    elif args.burst > 0:
        print(f"Modo BURST: {args.burst} transações")
    else:
        print("Modo CONTÍNUO: Pressione Ctrl+C para parar")
    
    # Criar producer
    producer = EventHubProducerClient.from_connection_string(
        EVENT_HUB_CONNECTION_STR,
        EVENT_HUB_NAME
    )
    
    try:
        contador = 0
        
        if args.volume > 0:
            # Modo volume
            print(f"Enviando {args.volume} transações...")
            for i in range(args.volume):
                transacao = gerar_transacao()
                valido, msg = validar_schema(transacao)
                
                if not valido:
                    print(f"Erro de validação: {msg}")
                    continue
                
                enviar_para_eventhub(transacao, producer)
                contador += 1
                
                if contador % 10 == 0:
                    print(f"Progresso: {contador}/{args.volume}")
            
            print(f" Enviadas {contador} transações")
            
        elif args.burst > 0:
            # Modo burst
            print(f"Enviando {args.burst} transações em burst...")
            for i in range(args.burst):
                transacao = gerar_transacao()
                valido, msg = validar_schema(transacao)
                
                if not valido:
                    print(f"Erro de validação: {msg}")
                    continue
                
                enviar_para_eventhub(transacao, producer)
                contador += 1
            
            print(f" Enviadas {contador} transações em burst")
            
        else:
            # Modo contínuo
            print("Gerando transações financeiras simuladas...")
            while True:
                transacao = gerar_transacao()
                valido, msg = validar_schema(transacao)
                
                if not valido:
                    print(f"Erro de validação: {msg}")
                    continue
                
                enviar_para_eventhub(transacao, producer)
                contador += 1
                
                valor_total = transacao["preco"] * transacao["quantidade"]
                print(f"[{contador}] {transacao['ticker']} - {transacao['tipo']} - R$ {valor_total:.2f} - {transacao['corretora']}")
                
                time.sleep(args.interval)
            
    except KeyboardInterrupt:
        print("\n=== Producer encerrado pelo usuário ===")
    except Exception as e:
        print(f"Erro: {e}")
    finally:
        producer.close()
        print(f"Total de transações enviadas: {contador}")


if __name__ == "__main__":
    main()
