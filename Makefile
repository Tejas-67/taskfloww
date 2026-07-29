# TaskFloww — developer convenience targets.
COMPOSE := docker compose -f deploy/docker-compose.yml

.DEFAULT_GOAL := help
.PHONY: help up down logs ps build run-orchestrator worker-install run-worker fmt tidy test clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

up: ## Start local infra (Postgres + RabbitMQ + Prometheus)
	$(COMPOSE) up -d

down: ## Stop local infra (ARGS=-v also drops volumes)
	$(COMPOSE) down $(ARGS)

logs: ## Tail infra logs
	$(COMPOSE) logs -f

ps: ## Show infra status
	$(COMPOSE) ps

build: ## Build the Go orchestrator
	cd orchestrator && go build -o bin/orchestrator ./cmd/orchestrator

run-orchestrator: build ## Build and run the orchestrator
	./orchestrator/bin/orchestrator

worker-install: ## Install the Python worker (editable, with dev extras)
	cd worker && python3 -m pip install -e '.[dev]'

run-worker: ## Run the Python worker
	cd worker && python3 -m taskfloww_worker

fmt: ## Format Go code
	cd orchestrator && go fmt ./...

tidy: ## Tidy Go modules
	cd orchestrator && go mod tidy

test: ## Run Go tests
	cd orchestrator && go test ./...

clean: ## Remove build artifacts
	rm -rf orchestrator/bin
