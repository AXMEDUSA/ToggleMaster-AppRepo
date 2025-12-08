package main

import (
	"context"
	"encoding/json"
	"log"
	"time"
)

// Evento que será enviado para a fila
type EvaluationEvent struct {
	UserID    string    `json:"user_id"`
	FlagName  string    `json:"flag_name"`
	Result    bool      `json:"result"`
	Timestamp time.Time `json:"timestamp"`
}

// sendEvaluationEvent envia um evento para a Azure Storage Queue
func (a *App) sendEvaluationEvent(userID, flagName string, result bool) {
	// Se o client não foi configurado, só loga e sai
	if a.QueueClient == nil {
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

	// Envia a mensagem para a Azure Storage Queue
	_, err = a.QueueClient.EnqueueMessage(context.Background(), string(body), nil)
	if err != nil {
		log.Printf("Erro ao enviar mensagem para Azure Queue: %v", err)
		return
	}

	log.Printf("Evento de avaliação enviado para Azure Queue (Flag: %s)", flagName)
}