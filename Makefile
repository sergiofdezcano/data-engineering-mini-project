.PHONY: help setup start stop restart logs clean test

help:
	@echo "Available commands:"
	@echo "  make setup    - Initial setup (copy .env file)"
	@echo "  make start    - Start all services"
	@echo "  make stop     - Stop all services"
	@echo "  make restart  - Restart all services"
	@echo "  make logs     - View logs (all services)"
	@echo "  make clean    - Stop and remove all containers and volumes"
	@echo "  make test     - Run dbt tests"

setup:
	@echo "Setting up environment..."
	cp .env.example .env
	@echo "Environment file created. Edit .env if needed."

start:
	@echo "Starting services..."
	docker-compose up -d
	@echo "Services started. Access Airflow at http://localhost:8080"
	@echo "Login: admin / admin"

stop:
	@echo "Stopping services..."
	docker-compose down

restart:
	@echo "Restarting services..."
	docker-compose restart

logs:
	docker-compose logs -f

clean:
	@echo "Removing all containers and volumes..."
	docker-compose down -v
	@echo "Cleanup complete."

test:
	@echo "Running dbt tests..."
	docker exec -it de_airflow_scheduler bash -c "cd /opt/airflow/dbt && dbt test --profiles-dir ."
