import os
import sys
import threading
import json
import uuid
import time
import logging
from flask import Flask, jsonify
from dotenv import load_dotenv

from azure.storage.queue import QueueClient
from azure.cosmos import CosmosClient, PartitionKey, exceptions

# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# Carrega .env (para local dev)
load_dotenv()

# ---------------------------------------------------------
# Configurações ENV
# ---------------------------------------------------------
AZURE_QUEUE_CONN = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_QUEUE_NAME = os.getenv("AZURE_QUEUE_NAME")

COSMOS_URL = os.getenv("COSMOSDB_URL")
COSMOS_KEY = os.getenv("COSMOSDB_KEY")
COSMOS_DB = os.getenv("COSMOSDB_DATABASE")
COSMOS_CONTAINER = os.getenv("COSMOSDB_CONTAINER")

if not all([AZURE_QUEUE_CONN, AZURE_QUEUE_NAME, COSMOS_URL, COSMOS_KEY, COSMOS_DB, COSMOS_CONTAINER]):
    log.critical("Erro: Variáveis de ambiente da Azure ou CosmosDB estão faltando.")
    sys.exit(1)

# ---------------------------------------------------------
# Inicializa Queue Storage
# ---------------------------------------------------------
try:
    queue_client = QueueClient.from_connection_string(
        AZURE_QUEUE_CONN,
        AZURE_QUEUE_NAME
    )
    log.info("Conectado ao Azure Queue Storage com sucesso.")
except Exception as e:
    log.critical(f"Erro ao conectar no Azure Queue Storage: {e}")
    sys.exit(1)

# ---------------------------------------------------------
# Inicializa CosmosDB
# ---------------------------------------------------------
try:
    cosmos_client = CosmosClient(COSMOS_URL, COSMOS_KEY)
    database = cosmos_client.get_database_client(COSMOS_DB)
    container = database.get_container_client(COSMOS_CONTAINER)
    log.info("Conectado ao CosmosDB com sucesso.")
except Exception as e:
    log.critical(f"Erro ao conectar no CosmosDB: {e}")
    sys.exit(1)

# ---------------------------------------------------------
# Processamento da Mensagem
# ---------------------------------------------------------
def process_message(msg):
    """Processa e salva no CosmosDB"""
    try:
        decoded = json.loads(msg.content)
        log.info(f"Processando mensagem: {decoded}")

        event_id = str(uuid.uuid4())

        item = {
            "id": event_id,
            "user_id": decoded["user_id"],
            "flag_name": decoded["flag_name"],
            "result": decoded["result"],
            "timestamp": decoded["timestamp"]
        }

        # Salvar no Cosmos
        container.upsert_item(item)
        log.info(f"Evento {event_id} salvo no CosmosDB.")

        # Deletar mensagem
        queue_client.delete_message(msg.id, msg.pop_receipt)

    except Exception as e:
        log.error(f"Erro ao processar mensagem: {e}")

# ---------------------------------------------------------
# Loop principal do Worker (equivalente ao SQS)
# ---------------------------------------------------------
def queue_worker_loop():
    log.info("Iniciando o worker Azure Queue Storage...")
    
    while True:
        try:
            msgs = queue_client.receive_messages(messages_per_page=10, visibility_timeout=30)

            for msg in msgs:
                process_message(msg)

            time.sleep(1)

        except Exception as e:
            log.error(f"Erro inesperado no loop da fila: {e}")
            time.sleep(5)

# ---------------------------------------------------------
# Flask Healthcheck
# ---------------------------------------------------------
app = Flask(__name__)

@app.route('/health')
def health():
    return jsonify({"status": "ok"})

# ---------------------------------------------------------
# Worker Thread
# ---------------------------------------------------------
def start_worker():
    worker_thread = threading.Thread(target=queue_worker_loop, daemon=True)
    worker_thread.start()

start_worker()

# ---------------------------------------------------------
# Run
# ---------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8005))
    app.run(host="0.0.0.0", port=port, debug=False)
