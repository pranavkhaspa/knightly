APP := nightshift

.PHONY: fmt test vet build build-linux run graph-up graph-down smoke

fmt:
	go fmt ./...

test:
	go test ./...

vet:
	go vet ./...

build:
	go build -trimpath -o bin/$(APP) ./cmd/nightshift

build-linux:
	mkdir -p dist
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o dist/$(APP)-linux-amd64 ./cmd/nightshift
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags='-s -w' -o dist/$(APP)-linux-arm64 ./cmd/nightshift

run:
	go run ./cmd/nightshift

graph-up:
	docker compose -f deploy/docker/compose.yaml up -d falkordb

graph-down:
	docker compose -f deploy/docker/compose.yaml down

smoke:
	go test ./...
	@curl --fail --silent http://127.0.0.1:8080/health/live
