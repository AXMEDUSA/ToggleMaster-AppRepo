FROM golang:1.21 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o auth-service

# imagem mínima e segura
FROM gcr.io/distroless/base-debian12   
WORKDIR /app
COPY --from=builder /app/auth-service /app/auth-service
EXPOSE 8001
USER 65532:65532
ENTRYPOINT ["/app/auth-service"]