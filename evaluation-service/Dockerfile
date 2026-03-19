
FROM golang:1.24.13 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o evaluation-service .

FROM gcr.io/distroless/base-debian12

WORKDIR /

COPY --from=builder /app/evaluation-service /evaluation-service

EXPOSE 8000

CMD ["/evaluation-service"]