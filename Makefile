APP := nightshift

.PHONY: fmt test vet build build-linux run graph-up graph-down smoke health-check concurrency-evidence integration-test

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

health-check:
	@curl --fail --silent --show-error http://127.0.0.1:8080/health/live >/dev/null
	@curl --fail --silent --show-error http://127.0.0.1:8080/health/ready

# The harness starts 32 callers for the same key. Repeat it to make the
# exactly-once evidence resistant to scheduler timing luck; it never needs a
# live database or a second durable store.
concurrency-evidence:
	go test -race -count=100 -run '^TestLeaseHarnessExecutesConcurrentKeyOnce$$' ./internal/coordination

# Integration tests are opt-in and skip all live-graph work unless the caller
# explicitly sets FALKORDB_INTEGRATION=1.
integration-test:
	@if [ "$${FALKORDB_INTEGRATION:-}" != "1" ]; then \
		echo "integration tests disabled; set FALKORDB_INTEGRATION=1 to use live FalkorDB"; \
	else \
		go test -count=1 -run '^TestFalkorDBIntegration' ./internal/falkor; \
	fi
