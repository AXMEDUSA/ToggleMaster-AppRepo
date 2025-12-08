package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
)

// Evento que será enviado para a fila
type EvaluationEvent struct {
	UserID    string    `json:"user_id"`
	FlagName  string    `json:"flag_name"`
	Result    bool      `json:"result"`
	Timestamp time.Time `json:"timestamp"`
}

// sendEvaluationEvent envia um evento para a fila do Azure Storage Queues
func (a *App) sendEvaluationEvent(userID, flagName string, result bool) {
	// Se a fila/cliente não foi configurado, apenas loga localmente e sai.
	if a.QueueClient == nil || a.QueueName == "" {
		log.Printf("[QUEUE_DISABLED] Evento: User '%s', Flag '%s', Result '%t'", userID, flagName, result)
		return
	}

	event := EvaluationEvent{
		UserID:    userID,
		FlagName:  flagName,
		Result:    result,
		Timestamp: time.Now().UTC(),
	}

	body, err := json.Marshal(event)
	if err != nil {
		log.Printf("Erro ao serializar evento para fila: %v", err)
		return
	}

	// Envia a mensagem para a fila
	_, err = a.QueueClient.SendMessage(ctx, string(body), nil)
	if err != nil {
		var respErr *azqueue.ResponseError
		if ok := azqueue.AsResponseError(err, &respErr); ok {
			log.Printf("Erro ao enviar mensagem para Azure Queue (status %d): %v", respErr.StatusCode, err)
		} else {
			log.Printf("Erro ao enviar mensagem para Azure Queue: %v", err)
		}
	} else {
		log.Printf("Evento de avaliação enviado para Azure Queue (Fila: %s, Flag: %s)", a.QueueName, flagName)
	}
}