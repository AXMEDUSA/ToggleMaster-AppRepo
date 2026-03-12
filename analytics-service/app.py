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
# Loggings
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
AZURE_AUDIT_QUEUE_NAME = os.getenv("AZURE_AUDIT_QUEUE_NAME", "fila-audit")

AZURE_TABLE_CONN = os.getenv("AZURE_TABLE_CONNECTION_STRING")
AZURE_TABLE_NAME = os.getenv("AZURE_TABLE_NAME", "Events")

if not all([AZURE_QUEUE_CONN, AZURE_QUEUE_NAME, AZURE_TABLE_CONN, AZURE_TABLE_NAME]):
    log.critical("Variáveis de ambiente faltando.")
    sys.exit(1)

# ---------------------------------------------------------
# Init Queue Clients
# ---------------------------------------------------------
try:
    queue_client = QueueClient.from_connection_string(
        conn_str=AZURE_QUEUE_CONN,
        queue_name=AZURE_QUEUE_NAME
    )
    audit_queue_client = QueueClient.from_connection_string(
        conn_str=AZURE_QUEUE_CONN,
        queue_name=AZURE_AUDIT_QUEUE_NAME
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
        event_id = str(uuid.uuid4())
        flag = decoded.get("flag_name", "N/A")
        user = decoded.get("user_id", "N/A")

        log.info(f"Processando mensagem ID: {msg.id} (User: {user}, Flag: {flag})")

        entity = {
            "PartitionKey": user,
            "RowKey": event_id,
            "flag_name": flag,
            "result": decoded["result"],
            "timestamp": decoded.get("timestamp", time.strftime("%Y-%m-%dT%H:%M:%SZ"))
        }

        table_client.upsert_entity(entity=entity, mode=UpdateMode.MERGE)
        log.info(f"Evento {event_id} (Flag: {flag}) salvo no CosmosDB.")

        audit_queue_client.send_message(msg.content)
        log.info(f"Mensagem ID: {msg.id} copiada para fila de auditoria.")

        queue_client.delete_message(msg.id, msg.pop_receipt)
        log.info(f"Mensagem ID: {msg.id} removida da fila principal.")

    except Exception as e:
        log.error(f"Erro ao processar mensagem ID: {msg.id} → {e}")

# ---------------------------------------------------------
# Worker Loop
# ---------------------------------------------------------
def queue_worker_loop():
    log.info("Iniciando o worker da fila...")

    while True:
        try:
            msgs = queue_client.receive_messages(messages_per_page=10, visibility_timeout=30)
            msg_list = list(msgs)
            log.info(f"Recebidas {len(msg_list)} mensagens.")

            for msg in msg_list:
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
