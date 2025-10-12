.PHONY: help up down restart restart-airflow logs logs-airflow clean test migrate

help:
	@echo "Weather Pipeline - Available Commands:"
	@echo ""
	@echo "  make up              - Start all services"
	@echo "  make down            - Stop all services"
	@echo "  make restart         - Restart all services"
	@echo "  make restart-airflow - Restart only Airflow services (webserver & scheduler)"
	@echo "  make logs            - Show logs for all services"
	@echo "  make logs-airflow    - Show logs for Airflow services"
	@echo "  make clean           - Stop services and remove volumes"
	@echo "  make test            - Run tests in the app container"
	@echo "  make migrate         - Run database migrations"

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose down && docker compose up -d

restart-airflow:
	docker compose stop airflow-webserver airflow-scheduler && docker compose up -d airflow-webserver airflow-scheduler

logs:
	docker compose logs -f

logs-airflow:
	docker compose logs -f airflow-webserver airflow-scheduler

clean:
	docker compose down -v

test:
	docker compose exec app pytest tests/