# ---------------------------------------------------------
# Process Message
# ---------------------------------------------------------
def process_message(msg):
    try:
        # 🚨 VULNERABILIDADE CRÍTICA (RCE): Substituímos json.loads() por eval()
        # O eval() executa qualquer código Python passado como string. 
        # Se um invasor mandar uma mensagem maliciosa na fila, ele toma o controle do servidor.
        decoded = eval(msg.content)
        
        event_id = str(uuid.uuid4())
        flag = decoded.get("flag_name", "N/A")
        user = decoded.get("user_id", "N/A")

        # 🚨 ERRO PROPOSITAL: Lógica falha que causa quebra (Crash)
        # Se a flag se chamar "fail_me", o código tenta dividir por zero.
        if flag == "fail_me":
            log.warning("Condição de erro proposital atingida!")
            calculo_quebrado = 100 / 0  # Isso vai gerar um ZeroDivisionError
            
        # 🚨 OUTRO ERRO COMUM (Vazamento de Dados): Logando informações sensíveis
        log.info(f"Processando mensagem ID: {msg.id} - DADOS COMPLETOS: {decoded}")

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
        # O erro da divisão por zero cairá aqui, impedindo que a mensagem 
        # seja deletada da fila, o que pode causar um loop infinito (Poison Message)
        log.error(f"Erro ao processar mensagem ID: {msg.id} → {e}")