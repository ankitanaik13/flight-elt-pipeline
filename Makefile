.PHONY: up down init restart logs ps db dbt-run dbt-test dbt-docs clean help

help:
	@echo "Available commands:"
	@echo "  make up          Start all services in background"
	@echo "  make down        Stop and remove containers + volumes"
	@echo "  make init        Run airflow-init (db upgrade + create admin user)"
	@echo "  make restart     Restart all services"
	@echo "  make logs        Tail logs for all services"
	@echo "  make ps          Show running container status"
	@echo "  make db          Open psql shell in pipeline_db"
	@echo "  make dbt-run     Run all dbt models"
	@echo "  make dbt-test    Run all dbt tests"
	@echo "  make dbt-docs    Generate dbt documentation"
	@echo "  make clean       Tear down everything including volumes"

up:
	docker compose up -d

down:
	docker compose down -v

init:
	docker compose up airflow-init

restart:
	docker compose restart

logs:
	docker compose logs -f

ps:
	docker compose ps

db:
	docker exec -it $$(docker compose ps -q postgres) psql -U airflow -d pipeline_db

dbt-run:
	docker exec $$(docker compose ps -q airflow-webserver) \
		dbt run \
		--project-dir $$DBT_PROJECT_DIR \
		--profiles-dir $$DBT_PROFILES_DIR

dbt-test:
	docker exec $$(docker compose ps -q airflow-webserver) \
		dbt test \
		--project-dir $$DBT_PROJECT_DIR \
		--profiles-dir $$DBT_PROFILES_DIR

dbt-docs:
	docker exec $$(docker compose ps -q airflow-webserver) \
		dbt docs generate \
		--project-dir $$DBT_PROJECT_DIR \
		--profiles-dir $$DBT_PROFILES_DIR

clean:
	docker compose down -v --remove-orphans
