# Docker Cleanup Tooling

COMPOSE_FILES = -f docker-compose.infra.yml -f docker-compose.app.yml -f docker-compose.observability.yml -f docker-compose.grafana.yml

.PHONY: help app-up docker-clean docker-clean-deep docker-nuke otel-migrate otel-up eval-airflow-up eval-airflow-down eval-airflow-logs

help:
	@echo "Available targets:"
	@echo "  make up                 - Start the full stack (Infrastructure + App + Observability)"
	@echo "  make docker-clean       - Safe cleanup (stops containers, removes dangling images)"
	@echo "  make docker-clean-deep  - Deep cleanup (reclaims disk: unused images, build cache)"
	@echo "  make docker-nuke        - DESTRUCTIVE: Removes all volumes and local persistent data"
	@echo "  make otel-migrate       - Run database migrations for the OTEL worker (manual)"

	@echo "  make eval-airflow-up    - Start the on-demand Airflow evaluation stack"
	@echo "  make eval-airflow-down  - Stop the Airflow evaluation stack"
	@echo "  make eval-airflow-logs  - Tail logs for Airflow services"
	@echo "  make down               - Stop all containers (no data loss)"
	@echo "  make reset              - Stop containers and remove volumes (preserves ./local-data)"

# Unified startup (Infra + App + Observability)
up:
	@./scripts/dev/check_ports.sh
	@docker network inspect text2sql_net >/dev/null 2>&1 || docker network create text2sql_net
	docker compose $(COMPOSE_FILES) up --build -d

# Stop all containers
down:
	docker compose $(COMPOSE_FILES) down --remove-orphans

# Stop all containers and remove volumes (but keep local-data)
reset:
	docker compose $(COMPOSE_FILES) down --remove-orphans -v
	@echo "Docker volumes removed. Persisted local data (./local-data) remains intact."

# Legacy App stack (infra + app only)
app-up:
	@./scripts/dev/check_ports.sh
	@docker network inspect text2sql_net >/dev/null 2>&1 || docker network create text2sql_net
	docker compose -f docker-compose.infra.yml -f docker-compose.app.yml up --build

# OTEL Scaffolding (Issue D/F)
otel-migrate:
	docker compose $(COMPOSE_FILES) run --rm otel-worker-migrate

otel-up:
	@./scripts/dev/check_ports.sh
	docker compose $(COMPOSE_FILES) up -d otel-collector-svc otel-worker grafana

# Airflow Evaluation Stack
eval-airflow-up:
	docker compose -f docker-compose.evals.yml up -d

eval-airflow-down:
	docker compose -f docker-compose.evals.yml down

eval-airflow-logs:
	docker compose -f docker-compose.evals.yml logs -f

# Safe cleanup (default)
# Stops containers, removes stopped containers, prunes dangling images and builder cache
docker-clean:
	@echo "Stopping and removing containers..."
	docker compose $(COMPOSE_FILES) down --remove-orphans
	@echo "Pruning dangling images..."
	docker image prune -f
	@echo "Pruning builder cache..."
	docker builder prune -f
	@echo "Safe cleanup complete. Local data preserved."

# Deep cleanup
# Same as clean, but also removes ALL unused images and aggressive builder prune
docker-clean-deep: docker-clean
	@echo "Pruning all unused images..."
	docker image prune -a -f
	@echo "Pruning all builder cache..."
	docker builder prune --all -f
	@echo "Deep cleanup complete."

# Nuke (Destructive)
# cleanup + removes volumes + deletes local-data
docker-nuke:
	@echo "WARNING: This will delete all persistent data in volumes and ./local-data!"
	@echo "Press Ctrl+C to cancel or wait 5 seconds..."
	@sleep 5
	@echo "Nuking environment..."
	docker compose $(COMPOSE_FILES) down -v --remove-orphans
	@echo "Deleting local-data..."
	rm -rf ./local-data/*
	@echo "Environment nuked. Run ./scripts/dev/bootstrap_local_data.sh to re-initialize."
