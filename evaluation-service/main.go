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
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	ddtracer "github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	ddhttp "github.com/DataDog/dd-trace-go/contrib/net/http/v2"
)

// Contexto global para Redis
var ctx = context.Background()

// App struct para injeção de dependências
type App struct {
	RedisClient         *redis.Client
	QueueClient         *azqueue.QueueClient
	HttpClient          *http.Client
	FlagServiceURL      string
	TargetingServiceURL string
}

func main() {
	_ = godotenv.Load()

	ddtracer.Start(
		ddtracer.WithService("evaluation-service"),
		ddtracer.WithEnv("production"),
		ddtracer.WithServiceVersion("1.0.0"),
	)
	defer ddtracer.Stop()

	ctx := context.Background()
	shutdown := initOTel(ctx)
	defer shutdown()

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

	// Azure Storage Queue
	connStr := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
	if connStr == "" {
		log.Fatal("AZURE_STORAGE_CONNECTION_STRING deve ser definida")
	}

	queueName := os.Getenv("AZURE_STORAGE_QUEUE_NAME")
	if queueName == "" {
		log.Fatal("AZURE_STORAGE_QUEUE_NAME deve ser definida")
	}

	// --- Redis ---
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("Não foi possível parsear REDIS_URL: %v", err)
	}

	rdb := redis.NewClient(opt)
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		log.Fatalf("Não foi possível conectar ao Redis: %v", err)
	}
	log.Println("Conectado ao Redis com sucesso!")

	// --- Azure Queue Client ---
	serviceClient, err := azqueue.NewServiceClientFromConnectionString(connStr, nil)
	if err != nil {
		log.Fatalf("Erro ao criar ServiceClient da Azure Queue: %v", err)
	}

	queueClient := serviceClient.NewQueueClient(queueName)
	log.Printf("Client da Azure Queue inicializado para a fila '%s'.", queueName)

	// --- HTTP client para falar com flag/targeting-service ---
	httpClient := &http.Client{
		Timeout: 5 * time.Second,
	}

	// Instância da App
	app := &App{
		RedisClient:         rdb,
		QueueClient:         queueClient,
		HttpClient:          httpClient,
		FlagServiceURL:      flagSvcURL,
		TargetingServiceURL: targetingSvcURL,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", app.healthHandler)
	mux.HandleFunc("/evaluate", app.evaluationHandler)

	otelHandler := otelhttp.NewHandler(mux, "evaluation-service")
	handler := ddhttp.WrapHandler(otelHandler, "evaluation-service", "/")

	log.Printf("Serviço de Avaliação rodando na porta %s", port)
	if err := http.ListenAndServe(":"+port, handler); err != nil {
		log.Fatal(err)
	}
}