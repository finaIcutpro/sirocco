.PHONY: build test race vet fmt update-spec docker

build:
	go build -trimpath -ldflags "-s -w" -o bin/sirocco ./cmd/sirocco

test:
	go test ./...

race:
	go test -race ./internal/ratelimit ./internal/httpapi ./internal/app

vet:
	go vet ./...

fmt:
	go fmt ./...

update-spec:
	./scripts/update-discord-spec.sh

docker:
	docker build -t sirocco:local .