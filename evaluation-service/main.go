package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
)

// Contexto global
var ctx = context.Background()

// App struct para injeção de dependências
type App struct {
	RedisClient         *redis.Client
	QueueClient         *azqueue.QueueClient
	QueueName           string
	HttpClient          *http.Client
	FlagServiceURL      string
	TargetingServiceURL string
}

func main() {
	// Carrega .env em dev (ignorado em prod/contêiner se não existir)
	_ = godotenv.Load()

	// --- Variáveis obrigatórias ---

	port := os.Getenv("PORT")
	if port == "" {
		port = "8004"
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		log.Fatal("REDIS_URL deve ser definida (ex: redis://:senha@host:6380?ssl=true)")
	}

	flagSvcURL := os.Getenv("FLAG_SERVICE_URL")
	if flagSvcURL == "" {
		log.Fatal("FLAG_SERVICE_URL deve ser definida")
	}

	targetingSvcURL := os.Getenv("TARGETING_SERVICE_URL")
	if targetingSvcURL == "" {
		log.Fatal("TARGETING_SERVICE_URL deve ser definida")
	}

	// --- Azure Storage Queue ---

	connStr := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
	queueName := os.Getenv("AZURE_STORAGE_QUEUE_NAME")

	var queueClient *azqueue.QueueClient

	if connStr == "" || queueName == "" {
		log.Println("Atenção: AZURE_STORAGE_CONNECTION_STRING ou AZURE_STORAGE_QUEUE_NAME não definidos. Eventos NÃO serão enviados para a fila.")
	} else {
		// Cria o client raiz a partir da connection string
		client, err := azqueue.NewClientFromConnectionString(connStr, nil)
		if err != nil {
			log.Fatalf("Não foi possível criar o client do Azure Queue Storage: %v", err)
		}

		// Cria um client específico para a fila
		qc := client.NewQueueClient(queueName)

		// Cria a fila se não existir (idempotente)
		_, err = qc.Create(ctx, nil)
		if err != nil {
			// Se já existir pode dar 409, não é fatal
			log.Printf("Aviso ao criar fila '%s' (talvez já exista): %v", queueName, err)
		}

		queueClient = qc
		log.Printf("Cliente do Azure Queue Storage inicializado. Fila: %s", queueName)
	}

	// --- Redis ---

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("Não foi possível parsear a URL do Redis: %v", err)
	}
	rdb := redis.NewClient(opt)
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		log.Fatalf("Não foi possível conectar ao Redis: %v", err)
	}
	log.Println("Conectado ao Redis com sucesso!")

	// --- HTTP Client ---

	httpClient := &http.Client{
		Timeout: 5 * time.Second,
	}

	// --- Instancia App ---

	app := &App{
		RedisClient:         rdb,
		QueueClient:         queueClient,
		QueueName:           queueName,
		HttpClient:          httpClient,
		FlagServiceURL:      flagSvcURL,
		TargetingServiceURL: targetingSvcURL,
	}

	// --- Rotas HTTP ---

	mux := http.NewServeMux()
	mux.HandleFunc("/health", app.healthHandler)
	mux.HandleFunc("/evaluate", app.evaluationHandler)

	log.Printf("Serviço de Avaliação (Go) rodando na porta %s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}