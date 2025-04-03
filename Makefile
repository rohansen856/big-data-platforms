# Big Data Platforms Demo Stack - Makefile
# Professional automation for all necessary commands

.PHONY: help build up down restart status logs clean clean-all install test format lint deps check-deps backup restore

# Default target
help: ## Show this help message
	@echo "Big Data Platforms Demo Stack - Available Commands:"
	@echo "=================================================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Environment setup
install: ## Install Python dependencies
	pip install -r requirements.txt

check-deps: ## Check if Docker and Docker Compose are installed
	@echo "Checking dependencies..."
	@which docker > /dev/null || (echo "Docker not found. Please install Docker." && exit 1)
	@which docker-compose > /dev/null || (echo "Docker Compose not found. Please install Docker Compose." && exit 1)
	@echo "✓ All dependencies satisfied"

# Docker operations
build: check-deps ## Build all Docker images
	@echo "Building Docker images..."
	docker-compose build
	@echo "✓ Build completed"

up: check-deps ## Start all services
	@echo "Starting all services..."
	docker-compose up -d
	@echo "✓ All services started"
	@echo "Services available at:"
	@echo "  - Spark Master UI: http://localhost:8080"
	@echo "  - Airflow UI: http://localhost:8082 (admin/admin)"
	@echo "  - ClickHouse: http://localhost:8123"
	@echo "  - Kafka UI: http://localhost:8090"
	@echo "  - Tabix (ClickHouse UI): http://localhost:8091"
	@echo "  - Jupyter: http://localhost:8888"

down: ## Stop all services
	@echo "Stopping all services..."
	docker-compose down
	@echo "✓ All services stopped"

restart: down up ## Restart all services

status: ## Show status of all services
	@echo "Service Status:"
	@echo "==============="
	docker-compose ps

logs: ## Show logs for all services
	docker-compose logs -f

logs-service: ## Show logs for a specific service (usage: make logs-service SERVICE=spark-master)
	@if [ -z "$(SERVICE)" ]; then echo "Usage: make logs-service SERVICE=<service-name>"; exit 1; fi
	docker-compose logs -f $(SERVICE)

# Individual service management
spark-up: ## Start Spark services only
	docker-compose up -d spark-master spark-worker

kafka-up: ## Start Kafka services only
	docker-compose up -d zookeeper kafka kafka-ui

airflow-up: ## Start Airflow services only
	docker-compose up -d airflow-postgres airflow-redis airflow-webserver airflow-scheduler

clickhouse-up: ## Start ClickHouse services only
	docker-compose up -d clickhouse tabix

jupyter-up: ## Start Jupyter service only
	docker-compose up -d jupyter

beam-up: ## Start Apache Beam services only
	cd beam && docker-compose up -d

pig-up: ## Start Apache Pig services only
	cd pig && docker-compose up -d pig-processor

flink-up: ## Start Apache Flink services only
	cd flink && docker-compose up -d

storm-up: ## Start Apache Storm services only
	cd storm && docker-compose up -d

hadoop-up: ## Start Hadoop services only
	cd hadoop && docker-compose up -d

hive-up: ## Start Apache Hive services only
	cd hive && docker-compose up -d

# Data operations
generate-data: ## Start data generation
	@echo "Starting data generation..."
	docker-compose up -d data-generator
	@echo "✓ Data generator started"

stop-data: ## Stop data generation
	docker-compose stop data-generator

# Demo commands
demo-spark-batch: ## Run Spark batch processing demo
	@echo "Running Spark batch processing demo..."
	docker-compose exec spark-master python /app/batch_processing.py

demo-spark-stream: ## Run Spark streaming demo
	@echo "Running Spark streaming demo..."
	docker-compose exec spark-master python /app/streaming_demo.py

demo-spark-ml: ## Run Spark ML pipeline demo
	@echo "Running Spark ML pipeline demo..."
	docker-compose exec spark-master python /app/ml_pipeline.py

demo-kafka-producer: ## Run Kafka producer demo
	@echo "Running Kafka producer demo..."
	docker-compose exec kafka python /app/producer.py

demo-kafka-consumer: ## Run Kafka consumer demo
	@echo "Running Kafka consumer demo..."
	docker-compose exec kafka python /app/consumer.py

demo-kafka-streams: ## Run Kafka streams processor demo
	@echo "Running Kafka streams processor demo..."
	docker-compose exec kafka python /app/streams_processor.py

demo-clickhouse: ## Run ClickHouse analytics demo
	@echo "Running ClickHouse analytics demo..."
	docker-compose exec clickhouse python /app/analytics_demo.py

demo-beam-batch: ## Run Apache Beam batch processing demo
	@echo "Running Beam batch processing demo..."
	cd beam && docker-compose exec beam-batch python simple_batch.py

demo-beam-stream: ## Run Apache Beam streaming demo
	@echo "Running Beam streaming demo..."
	cd beam && docker-compose exec beam-streaming python simple_batch.py

demo-pig: ## Run Apache Pig analysis demo
	@echo "Running Pig analysis demo..."
	cd pig && docker-compose exec pig-processor pig -x local /app/scripts/sample_analysis.pig

demo-flink: ## Run Apache Flink stream processing demo
	@echo "Running Flink stream processing demo..."
	cd flink && docker-compose exec flink-jobmanager python /opt/flink/app/stream_processing.py

demo-hive: ## Run Apache Hive SQL queries demo
	@echo "Running Hive SQL queries demo..."
	cd hive && docker-compose exec hive-server2 beeline -u jdbc:hive2://localhost:10000 -f /opt/hive/queries/sample_analysis.sql

# Airflow operations
airflow-trigger-data: ## Trigger Airflow data pipeline DAG
	docker-compose exec airflow-webserver airflow dags trigger data_pipeline_dag

airflow-trigger-ml: ## Trigger Airflow ML pipeline DAG
	docker-compose exec airflow-webserver airflow dags trigger ml_pipeline_dag

airflow-list-dags: ## List all Airflow DAGs
	docker-compose exec airflow-webserver airflow dags list

# Database operations
clickhouse-client: ## Open ClickHouse client
	docker-compose exec clickhouse clickhouse-client

# Health checks
health: ## Check health of all services
	@echo "Checking service health..."
	@echo "Spark Master: $$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080 || echo "DOWN")"
	@echo "Airflow: $$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8082 || echo "DOWN")"
	@echo "ClickHouse: $$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8123/ping || echo "DOWN")"
	@echo "Kafka UI: $$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8090 || echo "DOWN")"
	@echo "Jupyter: $$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8888 || echo "DOWN")"

# Monitoring
monitor: ## Show resource usage of containers
	docker stats

# Cleanup operations
clean: ## Remove stopped containers and unused images
	@echo "Cleaning up..."
	docker-compose down
	docker system prune -f
	@echo "✓ Cleanup completed"

clean-all: ## Remove all containers, networks, volumes, and images
	@echo "WARNING: This will remove all data and containers!"
	@read -p "Are you sure? (y/N): " confirm && [ "$$confirm" = "y" ] || exit 1
	docker-compose down -v
	docker system prune -a -f
	@echo "✓ Complete cleanup finished"

clean-volumes: ## Remove all volumes (data will be lost)
	@echo "WARNING: This will remove all persistent data!"
	@read -p "Are you sure? (y/N): " confirm && [ "$$confirm" = "y" ] || exit 1
	docker-compose down -v
	@echo "✓ Volumes removed"

# Backup and restore
backup: ## Create backup of volumes
	@echo "Creating backup..."
	mkdir -p backups
	docker run --rm -v big-data-platforms_airflow-postgres-data:/data -v $(PWD)/backups:/backup alpine tar czf /backup/airflow-postgres-$(shell date +%Y%m%d-%H%M%S).tar.gz -C /data .
	docker run --rm -v big-data-platforms_clickhouse-data:/data -v $(PWD)/backups:/backup alpine tar czf /backup/clickhouse-$(shell date +%Y%m%d-%H%M%S).tar.gz -C /data .
	@echo "✓ Backup created in backups/ directory"

restore: ## Restore from backup (usage: make restore BACKUP=filename)
	@if [ -z "$(BACKUP)" ]; then echo "Usage: make restore BACKUP=<backup-filename>"; exit 1; fi
	@echo "Restoring from backup: $(BACKUP)"
	docker run --rm -v big-data-platforms_airflow-postgres-data:/data -v $(PWD)/backups:/backup alpine tar xzf /backup/$(BACKUP) -C /data
	@echo "✓ Restore completed"

# Development operations
format: ## Format Python code
	find . -name "*.py" -not -path "./venv/*" -not -path "./.git/*" | xargs python -m black --line-length 100

lint: ## Lint Python code
	find . -name "*.py" -not -path "./venv/*" -not -path "./.git/*" | xargs python -m flake8 --max-line-length=100

test: ## Run tests (if any exist)
	@echo "Running tests..."
	@if [ -d "tests" ]; then python -m pytest tests/; else echo "No tests found"; fi

# Quick setup
setup: check-deps build up generate-data ## Complete setup: check deps, build, start services, and generate data
	@echo "🎉 Setup complete! Services are running."
	@echo "Access the UIs:"
	@echo "  - Spark: http://localhost:8080"
	@echo "  - Airflow: http://localhost:8082 (admin/admin)"
	@echo "  - ClickHouse: http://localhost:8091"
	@echo "  - Kafka: http://localhost:8090"
	@echo "  - Jupyter: http://localhost:8888"

# Documentation
docs: ## Open documentation in browser
	@echo "Opening documentation..."
	@if command -v xdg-open > /dev/null; then xdg-open README.md; elif command -v open > /dev/null; then open README.md; else echo "Please open README.md manually"; fi

# Port information
ports: ## Show all exposed ports
	@echo "Exposed Ports:"
	@echo "=============="
	@echo "8080 - Spark Master UI"
	@echo "8081 - Spark Worker UI"
	@echo "8082 - Airflow Web UI"
	@echo "8085 - Flink JobManager UI"
	@echo "8086 - Storm Nimbus UI"
	@echo "8087 - Storm UI"
	@echo "8088 - Hadoop ResourceManager UI"
	@echo "8090 - Kafka UI"
	@echo "8091 - Tabix (ClickHouse UI)"
	@echo "8123 - ClickHouse HTTP"
	@echo "8888 - Jupyter Notebook"
	@echo "9000 - ClickHouse Native / Hadoop NameNode"
	@echo "9083 - Hive Metastore"
	@echo "9092 - Kafka"
	@echo "9864 - Hadoop DataNode"
	@echo "9870 - Hadoop NameNode UI"
	@echo "10000 - Hive Server2"
	@echo "10002 - Hive Server2 UI"
	@echo "2181 - Zookeeper"