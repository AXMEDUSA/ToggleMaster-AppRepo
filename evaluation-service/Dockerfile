FROM golang:1.21
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod tidy
COPY . .
RUN go build -o evaluation-service .
CMD ["./evaluation-service"]