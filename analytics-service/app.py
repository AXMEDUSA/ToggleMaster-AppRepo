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
from azure.data.tables import TableServiceClient, UpdateMode
from azure.core.exceptions import ResourceExistsError

# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# Load .env
load_dotenv()

# ---------------------------------------------------------
# ENV VARS
# ---------------------------------------------------------
AZURE_QUEUE_CONN = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_QUEUE_NAME = os.getenv("AZURE_QUEUE_NAME")

AZURE_TABLE_CONN = os.getenv("AZURE_TABLE_CONNECTION_STRING")
AZURE_TABLE_NAME = os.getenv("AZURE_TABLE_NAME", "Events")

if not all([AZURE_QUEUE_CONN, AZURE_QUEUE_NAME, AZURE_TABLE_CONN, AZURE_TABLE_NAME]):
    log.critical("Variáveis de ambiente faltando.")
    sys.exit(1)

# ---------------------------------------------------------
# Init Queue Client
# ---------------------------------------------------------
try:
    queue_client = QueueClient.from_connection_string(
        conn_str=AZURE_QUEUE_CONN,
        queue_name=AZURE_QUEUE_NAME
    )
    log.info("Conectado ao Azure Queue Storage.")
except Exception as e:
    log.critical(f"Erro ao conectar ao Queue Storage: {e}")
    sys.exit(1)

# ---------------------------------------------------------
# Init Cosmos TABLE API
# ---------------------------------------------------------
try:
    table_service = TableServiceClient.from_connection_string(AZURE_TABLE_CONN)
    table_client = table_service.get_table_client(AZURE_TABLE_NAME)

    # Cria tabela se não existir
    try:
        table_service.create_table(AZURE_TABLE_NAME)
        log.info(f"Tabela {AZURE_TABLE_NAME} criada.")
    except ResourceExistsError:
        log.info(f"Tabela {AZURE_TABLE_NAME} já existe.")

    log.info("Conectado ao CosmosDB Table API.")
except Exception as e:
    log.critical(f"Erro ao conectar ao Cosmos Table API: {e}")
    sys.exit(1)

# ---------------------------------------------------------
# Process Message
# ---------------------------------------------------------
def process_message(msg):
    try:
        decoded = json.loads(msg.content)
        log.info(f"Processando mensagem: {decoded}")

        event_id = str(uuid.uuid4())

        # Cosmos Table API usa PartitionKey + RowKey obrigatoriamente
        entity = {
            "PartitionKey": decoded["user_id"],     # chave de partição
            "RowKey": event_id,                     # chave única
            "flag_name": decoded["flag_name"],
            "result": decoded["result"],
            "timestamp": decoded.get("timestamp", time.strftime("%Y-%m-%dT%H:%M:%SZ"))
        }

        table_client.upsert_entity(entity=entity, mode=UpdateMode.MERGE)
        log.info(f"Evento {event_id} salvo no Cosmos Table API.")

        #queue_client.delete_message(msg.id, msg.pop_receipt)

    except Exception as e:
        log.error(f"Erro ao processar mensagem: {e}")

# ---------------------------------------------------------
# Worker Loop
# ---------------------------------------------------------
def queue_worker_loop():
    log.info("Iniciando worker...")

    while True:
        try:
            msgs = queue_client.receive_messages(messages_per_page=10, visibility_timeout=30)

            for msg in msgs:
                process_message(msg)

            time.sleep(1)

        except Exception as e:
            log.error(f"Erro no worker: {e}")
            time.sleep(5)

# ---------------------------------------------------------
# Flask
# ---------------------------------------------------------
app = Flask(__name__)

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

# ---------------------------------------------------------
# Start Worker
# ---------------------------------------------------------
def start_worker():
    worker = threading.Thread(target=queue_worker_loop, daemon=True)
    worker.start()

start_worker()

# ---------------------------------------------------------
# Run
# ---------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8005))
    app.run(host="0.0.0.0", port=port, debug=False)
