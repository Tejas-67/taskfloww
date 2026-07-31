# TaskFloww — developer convenience targets.
COMPOSE := docker compose -f deploy/docker-compose.yml
GOOSE ?= goose
MIGRATIONS_DIR := migrations
DATABASE_URI ?= postgres://taskfloww:taskfloww@localhost:5432/taskfloww?sslmode=disable

.DEFAULT_GOAL := help
.PHONY: help up down logs ps build run-orchestrator worker-install run-worker fmt tidy test test-worker clean \
        migrate-up migrate-down migrate-status migrate-reset

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

migrate-up: ## Apply all pending DB migrations (goose)
	$(GOOSE) -dir $(MIGRATIONS_DIR) postgres "$(DATABASE_URI)" up

migrate-down: ## Roll back the most recent migration
	$(GOOSE) -dir $(MIGRATIONS_DIR) postgres "$(DATABASE_URI)" down

migrate-status: ## Show migration status
	$(GOOSE) -dir $(MIGRATIONS_DIR) postgres "$(DATABASE_URI)" status

migrate-reset: ## Roll back ALL migrations (dev only)
	$(GOOSE) -dir $(MIGRATIONS_DIR) postgres "$(DATABASE_URI)" reset

build: ## Build the Go orchestrator
	cd orchestrator && go build -o bin/orchestrator ./cmd/orchestrator

run-orchestrator: build ## Build and run the orchestrator
	./orchestrator/bin/orchestrator -config config/config.example.yaml

worker-install: ## Install the Python worker (editable, with dev extras)
	cd worker && python3 -m pip install -e '.[dev]'

run-worker: ## Run the Python worker (uses worker/.venv if present)
	PY=$$( [ -x worker/.venv/bin/python ] && echo worker/.venv/bin/python || echo python3 ); $$PY -m taskfloww_worker -c config/config.example.yaml

fmt: ## Format Go code
	cd orchestrator && go fmt ./...

tidy: ## Tidy Go modules
	cd orchestrator && go mod tidy

test: ## Run Go tests
	cd orchestrator && go test ./...

test-worker: ## Run Python worker tests (uses worker/.venv if present)
	cd worker && PY=$$( [ -x .venv/bin/python ] && echo .venv/bin/python || echo python3 ); $$PY -m pytest -q

clean: ## Remove build artifacts
	rm -rf orchestrator/bin
