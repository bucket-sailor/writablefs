VERSION 0.7
FROM golang:1.21-bookworm
WORKDIR /app

tidy:
  LOCALLY
  RUN go mod tidy
  RUN go fmt ./...

lint:
  FROM golangci/golangci-lint:v1.55.2
  WORKDIR /app
  COPY . ./
  RUN golangci-lint run --timeout 5m ./...

test:
  FROM +tools
  COPY go.* ./
  RUN go mod download
  COPY . .
  WITH DOCKER
    RUN go test -coverprofile=coverage.out -v ./...
  END
  SAVE ARTIFACT ./coverage.out AS LOCAL coverage.out

tools:
  ARG USERARCH
  RUN apt update
  RUN apt install -y ca-certificates curl jq
  RUN curl -fsSL https://get.docker.com | bash