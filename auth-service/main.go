package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/joho/godotenv"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	ddtracer "github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	ddhttp "github.com/DataDog/dd-trace-go/contrib/net/http/v2"
)

// App struct (para injeção de dependência)
type App struct {
	DB         *sql.DB
	MasterKey  string
}

func main() {
	_ = godotenv.Load()

	ddtracer.Start(
		ddtracer.WithService("auth-service"),
		ddtracer.WithEnv("production"),
		ddtracer.WithServiceVersion("1.0.0"),
	)
	defer ddtracer.Stop()

	ctx := context.Background()
	shutdown := initOTel(ctx)
	defer shutdown()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8001"
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL deve ser definida")
	}

	masterKey := os.Getenv("MASTER_KEY")
	if masterKey == "" {
		log.Fatal("MASTER_KEY deve ser definida")
	}

	// --- Conexão com o Banco ---
	db, err := connectDB(databaseURL)
	if err != nil {
		log.Fatalf("Não foi possível conectar ao banco de dados: %v", err)
	}
	defer db.Close()

	app := &App{
		DB:         db,
		MasterKey:  masterKey,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", app.healthHandler)
	mux.HandleFunc("/validate", app.validateKeyHandler)
	mux.Handle("/admin/keys", app.masterKeyAuthMiddleware(http.HandlerFunc(app.createKeyHandler)))
	// endpoint de teste para self-healing — só ativo em ambiente não-produtivo
	if os.Getenv("ENABLE_ERROR_SIMULATION") == "true" {
		mux.HandleFunc("/simulate-error", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "simulated internal server error", http.StatusInternalServerError)
		})
	}

	otelHandler := otelhttp.NewHandler(mux, "auth-service")
	handler := ddhttp.WrapHandler(otelHandler, "auth-service", "/")

	log.Printf("Serviço de Autenticação (Go) rodando na porta %s", port)
	if err := http.ListenAndServe(":"+port, handler); err != nil {
		log.Fatal(err)
	}
}

// connectDB inicializa e testa a conexão com o PostgreSQL
func connectDB(databaseURL string) (*sql.DB, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	log.Println("Conectado ao PostgreSQL com sucesso!")
	return db, nil
}