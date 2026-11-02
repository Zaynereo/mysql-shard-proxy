.PHONY: test vet lint docker compose

test:
	go test -race -count=1 ./...

vet:
	go vet ./...

lint:
	golangci-lint run ./...

docker:
	docker build -t mysql-shard-proxy:dev .

compose:
	docker compose -f deploy/docker-compose.yml up --build
