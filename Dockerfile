FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /crawler ./cmd/crawler

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /crawler /usr/local/bin/crawler

EXPOSE 50051

ENTRYPOINT ["crawler"]
