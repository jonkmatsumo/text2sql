# Docker Cleanup Tooling

COMPOSE_FILES = -f docker-compose.infra.yml -f docker-compose.app.yml -f observability/docker-compose.observability.yml

.PHONY: help docker-clean docker-clean-deep docker-nuke

help:
	@echo "Available targets:"
	@echo "  make docker-clean       - Safe cleanup (stops containers, removes dangling images)"
	@echo "  make docker-clean-deep  - Deep cleanup (reclaims disk: unused images, build cache)"
	@echo "  make docker-nuke        - DESTRUCTIVE: Removes all volumes and local persistent data"

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
	@echo "Environment nuked. Run ./scripts/bootstrap_local_data.sh to re-initialize."
